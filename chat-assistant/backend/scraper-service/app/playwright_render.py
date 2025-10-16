import asyncio
import aiohttp
import gzip, hashlib, json, logging, time
from typing import Optional, Tuple, List
from urllib.parse import urlparse

import aioboto3
from botocore.config import Config as BotoConfig
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from urllib import robotparser

from .config import settings

logger = logging.getLogger("app.playwright")
r2_boto_config = BotoConfig(signature_version="s3v4", s3 = {"addressing_style": "path"})

_default_semaphore: Optional[asyncio.Semaphore] = None

def set_global_render_semaphore(limit: int):
    """Optional: set a module-level semaphore for limiting concurrent renders."""
    global _default_semaphore
    _default_semaphore = asyncio.Semaphore(limit)

async def _upload_bytes_to_r2(key: str, data: bytes, content_type: str, content_encoding: Optional[str] = None, metadata: Optional[dict] = None):
    """
    Upload bytes to R2 (S3-compatible) using aioboto3.
    """
    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=settings.r2_endpoint,
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        config=r2_boto_config,
    ) as client:
        put_kwargs = dict(Bucket=settings.r2_bucket, Key=key, Body=data, ContentType=content_type)
        if content_encoding:
            put_kwargs["ContentEncoding"] = content_encoding
        if metadata:
            # S3 metadata keys must be strings
            put_kwargs["Metadata"] = {str(k): str(v) for k, v in (metadata.items() if metadata else {})}
        await client.put_object(**put_kwargs)


async def _head_object_metadata(key: str) -> Optional[dict]:
    """
    Return metadata dict for an object key in R2/S3, or None if not found.
    """
    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=settings.r2_endpoint,
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        config=r2_boto_config,
    ) as client:
        try:
            resp = await client.head_object(Bucket=settings.r2_bucket, Key=key)
            return resp.get("Metadata", {}) or {}
        except Exception as e:
            logger.debug("head_object failed for %s: %s", key, e)
            return None
      
# ---- robots.txt check ----
async def can_fetch_via_robots(url: str, user_agent: str = "*", timeout: int = 5) -> bool:
    """
    Fetch robots.txt for the origin and check whether `user_agent` may fetch `url`.
    If robots is not present or unreachable, default to allowed.
    """
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(robots_url, timeout=timeout) as resp:
                if resp.status == 200:
                    txt = await resp.text()
                    rp = robotparser.RobotFileParser()
                    rp.parse(txt.splitlines())
                    return rp.can_fetch(user_agent, parsed.path or "/")
                else:
                    # 404 or other -> treat as allowed
                    return True
    except Exception as e:
        logger.debug("robots.txt check failed for %s: %s (treating as allowed)", robots_url, e)
        return True
    

# ---- helper: auto-scroll to trigger lazy loading ----
async def _auto_scroll(page, max_scrolls: int = 10, pause_ms: int = 300):
    last_height = await page.evaluate("() => document.body.scrollHeight")
    for i in range(max_scrolls):
        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(pause_ms / 1000.0)
        new_height = await page.evaluate("() => document.body.scrollHeight")
        if new_height == last_height:
            logger.debug("playwright: scroll stabilized after %d iterations", i + 1)
            break
        last_height = new_height

# ---- helper: wait for sustained zero inflight requests ----
async def _wait_for_zero_sustained(page, timeout_ms: int = 3000, poll_interval_ms: int = 100, zeros_required: int = 5) -> bool:
    deadline = time.time() + timeout_ms / 1000.0
    zeros_seen = 0
    while time.time() < deadline:
        try:
            v = await page.evaluate("() => window.__inflightRequests || 0")
        except Exception:
            v = None
        logger.debug("playwright: inflightRequests=%s", v)
        if v == 0:
            zeros_seen += 1
            if zeros_seen >= zeros_required:
                return True
        else:
            zeros_seen = 0
        await asyncio.sleep(poll_interval_ms / 1000.0)
    return False
  
# ---- Render + upload ----
async def render_page_and_upload(
    url: str,
    r2_key_html: str,
    snapshot_key: Optional[str] = None,
    wait_for_selector: Optional[str] = None,
    timeout_ms: int = 30_000,
    screenshot_full_page: bool = False,
    max_scrolls: int = 12,
    scroll_pause_ms: int = 400,
    wait_for_no_inflight_ms: int = 3_000,
    require_zero_duration_ms: int = 500,
    ignore_hosts: Optional[List[str]] = None,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> Tuple[str, str, Optional[str]]:
    """
    Render `url` using Playwright, upload gzipped HTML to R2 at r2_key_html and optionally a PNG snapshot.
    Returns (content_hash_hex, r2_html_key, snapshot_key_or_None).
    """
    sem = semaphore or _default_semaphore

    if ignore_hosts is None:
        ignore_hosts = [
            "google-analytics.com",
            "analytics.google.com",
            "googletagmanager.com",
            "doubleclick.net",
            "facebook.net",
            "fonts.googleapis.com",
        ]

    # robots check
    allowed = await can_fetch_via_robots(url, user_agent=settings.user_agent)
    if not allowed:
        logger.info("Blocked by robots.txt: %s", url)
        raise PermissionError(f"Blocked by robots.txt for {url}")

    # JavaScript instrumentation to count inflight requests.
    # This is intentionally conservative and avoids rewriting complex APIs.
    js_instrument = f"""
(function() {{
  try {{
    window.__inflightRequests = 0;
    window.__recentRequests = [];
    const ignoreHosts = {json.dumps(ignore_hosts)};

    function shouldCount(url) {{
      try {{
        if (!url) return true;
        for (const h of ignoreHosts) {{
          if (url.includes(h)) return false;
        }}
        return true;
      }} catch(e) {{ return true; }}
    }}

    // wrap fetch
    const origFetch = window.fetch;
    if (origFetch) {{
      window.fetch = async function(resource, init) {{
        try {{
          const url = typeof resource === 'string' ? resource : (resource && resource.url) || '';
          if (shouldCount(url)) {{
            window.__inflightRequests += 1;
            window.__recentRequests.push(url);
            if (window.__recentRequests.length > 100) window.__recentRequests.shift();
          }}
        }} catch(e){{}}
        try {{
          const result = await origFetch.apply(this, arguments);
          try {{ if (shouldCount(typeof resource === 'string' ? resource : (resource && resource.url) || '')) window.__inflightRequests -= 1; }} catch(e){{}}
          return result;
        }} catch(err) {{
          try {{ if (shouldCount(typeof resource === 'string' ? resource : (resource && resource.url) || '')) window.__inflightRequests -= 1; }} catch(e){{}}
          throw err;
        }}
      }};
    }}

    // wrap XHR
    (function() {{
      const origOpen = XMLHttpRequest.prototype.open;
      const origSend = XMLHttpRequest.prototype.send;
      XMLHttpRequest.prototype.open = function(method, url) {{
        this.__pw_url = url;
        return origOpen.apply(this, arguments);
      }};
      XMLHttpRequest.prototype.send = function() {{
        try {{
          const url = this.__pw_url || '';
          if (shouldCount(url)) {{
            window.__inflightRequests += 1;
            window.__recentRequests.push(url);
            if (window.__recentRequests.length > 100) window.__recentRequests.shift();
            this.addEventListener('loadend', function() {{
              try {{ window.__inflightRequests -= 1; }} catch(e){{}}
            }});
          }}
        }} catch(e){{}}
        return origSend.apply(this, arguments);
      }};
    }})();
  }} catch (e) {{ console.error('instrumentation error', e); }}
}})();
"""

    if sem:
        await sem.acquire()
        logger.debug("acquired render semaphore for %s", url)

    html = ""
    png = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"])
            page = await browser.new_page(user_agent=settings.user_agent)

            # add instrumentation
            try:
                await page.add_init_script(js_instrument)
            except Exception:
                logger.exception("failed to add init script")

            # navigate with fallbacks
            try:
                await page.goto(url, wait_until="networkidle", timeout=timeout_ms)
            except PlaywrightTimeoutError:
                logger.info("playwright: networkidle timeout for %s, falling back", url)
                try:
                    await page.goto(url, wait_until="load", timeout=timeout_ms)
                except PlaywrightTimeoutError:
                    logger.info("playwright: load timeout for %s, falling back to domcontentloaded", url)
                    await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, timeout=max(3000, timeout_ms // 3))
                except PlaywrightTimeoutError:
                    logger.info("playwright: wait_for_selector timed out for %s selector=%s", url, wait_for_selector)

            # auto-scroll
            try:
                await _auto_scroll(page, max_scrolls=max_scrolls, pause_ms=scroll_pause_ms)
            except Exception:
                logger.exception("playwright: auto_scroll failed for %s", url)

            # wait for inflight to settle
            try:
                poll_interval_ms = 100
                zeros_required = max(1, int(require_zero_duration_ms / poll_interval_ms))
                settled = await _wait_for_zero_sustained(page, timeout_ms=wait_for_no_inflight_ms, poll_interval_ms=poll_interval_ms, zeros_required=zeros_required)
                if not settled:
                    logger.debug("playwright: inflight did not settle for %s (recent=%s)", url, await page.evaluate("() => (window.__recentRequests||[]).slice(-10)"))
                else:
                    logger.debug("playwright: inflight settled for %s", url)
            except Exception:
                logger.exception("playwright: error while waiting for inflight requests for %s", url)

            await asyncio.sleep(0.15)  # small buffer for microtasks

            # get rendered html
            html = await page.content()

            if snapshot_key:
                try:
                    png = await page.screenshot(full_page=screenshot_full_page)
                except Exception:
                    logger.exception("playwright: screenshot failed for %s", url)

            await browser.close()
    except Exception:
        logger.exception("playwright render failed for %s", url)
        raise
    finally:
        if sem:
            try:
                sem.release()
            except Exception:
                logger.debug("failed to release semaphore")
            logger.debug("released render semaphore for %s", url)

    # compute sha
    content_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()

    # check existing object metadata to avoid re-uploading identical content
    try:
        meta = await _head_object_metadata(r2_key_html)
        old_sha = meta.get("sha256") if meta else None
    except Exception:
        old_sha = None

    if old_sha == content_hash:
        logger.info("content unchanged for %s (sha=%s) - skipping upload", url, content_hash)
    else:
        gz_html = gzip.compress(html.encode("utf-8"))
        try:
            await _upload_bytes_to_r2(r2_key_html, gz_html, content_type="text/html", content_encoding="gzip", metadata={"sha256": content_hash})
            logger.info("uploaded HTML for %s -> %s (sha=%s)", url, r2_key_html, content_hash)
        except Exception:
            logger.exception("playwright: failed to upload rendered html for %s to %s", url, r2_key_html)
            raise

    if snapshot_key and png:
        try:
            await _upload_bytes_to_r2(snapshot_key, png, content_type="image/png")
            logger.info("uploaded snapshot for %s -> %s", url, snapshot_key)
        except Exception:
            logger.exception("playwright: failed to upload snapshot for %s to %s", url, snapshot_key)

    return content_hash, r2_key_html, snapshot_key if snapshot_key else None
