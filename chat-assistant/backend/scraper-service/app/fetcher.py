import asyncio, gzip, hashlib, json, logging, os, tempfile
from asyncio import to_thread
from datetime import datetime, timezone, timedelta
from typing import Optional, Callable, Awaitable, Dict, Tuple
from urllib.parse import urlparse

from .config import settings
from .db import upsert_metadata

import aiohttp, aioboto3
from aiohttp import ClientResponseError, ClientConnectorError, ServerDisconnectedError, ClientSession
from botocore.config import Config as BotoConfig
from .playwright_render import render_page_and_upload

logger = logging.getLogger("app.fetcher")

# -----------------------
# Concurrency / politeness
# -----------------------

global_semaphore = asyncio.Semaphore(settings.concurrency)
domain_locks: Dict[str, asyncio.Semaphore] = {}
domain_last_req: Dict[str, float] = {}

async def polite_wait(domain: str):
    last = domain_last_req.get(domain, 0.0)
    now = asyncio.get_event_loop().time()
    wait = settings.per_domain_delay_s - (now - last)

    if wait > 0:
        await asyncio.sleep(wait)

async def acquire_domain_slot(domain: str):
    sem = domain_locks.get(domain)
    if not sem:
        sem = asyncio.semaphore(settings.per_domain_concurrency)
        domain_locks[domain] = sem
    
    await sem.acquire()
    await global_semaphore.acquire()
    await polite_wait(domain)

async def release_domain_slot(domain: str):
    try:
        global_semaphore.release()
    
    except ValueError:
        logger.warning("Global semaphore release attempted when not acquired")

    sem = domain_locks.get(domain)

    if sem:
        try:
            sem.release()
        except ValueError:
            logger.warning("domain semaphore release attempted when not acquired for domain %s", domain)

    domain_last_req[domain] = asyncio.get_event_loop().time()

# -----------------------
# Utilities
# -----------------------

def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def normalize_url(u: str) -> str:
    p = urlparse(u)
    scheme = p.scheme or "https"
    netloc = p.netloc.lower()
    path = p.path or "/"
    path = path.rstrip("/") if path != "/" else "/"
    query = ("?"+p.query) if p.query else ""
    return f"{scheme}://{netloc}{path}{query}"

def s3_key_for_raw(url_hash: str) -> str:
    dt = datetime.now(timezone.utc)
    return f"raw/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/sha256-{url_hash}.html.gz"

#simple exponential backoff helper
def backoff_delay(attempt: int, base: float = None, cap: float = None) -> float:
    base = base if base is not None else getattr(settings, "base_backoff_s", 1.0)
    cap = cap if cap is not None else getattr(settings, "cap_backoff_s", 30.0)
    delay = base*(2**attempt)
    return min(delay, cap)

# -----------------------
# R2 file uploader (file path -> put_object)
# -----------------------

r2_boto_config = BotoConfig(signature_version = "s3v4", s3={"addressing_style": "path"})

async def r2_upload_file(file_path: str, key: str, content_type: str = "text/html", content_encoding: Optional[str] = "gzip"):
    """
    Upload a local file to R2 bucket under `key`.
    Uses aioboto3; caller must ensure file_path exists.
    """
    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=settings.r2_endpoint,
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        config=r2_boto_config
    ) as client:
        #open the file synchronously and pass as Body (aioboto3 supports file-like)
        with open(file_path, "rb") as fh:
            put_kwargs = {"Bucket": settings.r2_bucket, "Key": key, "Body": fh, "ContentType": content_type}
            if content_encoding:
                put_kwargs["ContentEncoding"] = content_encoding

            await client.put_object(**put_kwargs)

# -----------------------
# Error classification
# -----------------------

def is_transient_fetch_exception(exc: Exception) -> bool:
    """
    Decide wether fetch error is transient (Retryable) or permanent.
    Treat 5xx and 429 and connector errors as transient.
    Treat 4xx (except 429) as permanent.
    """
    if isinstance(exc, ClientResponseError):
        st = getattr(exc, "status", None)
        if st is None:
            return True
        if st >= 500 or st == 429:
            return True
        return False
    #connection errors, server disconnected -> transient

    if isinstance(exc, (ClientConnectorError, ServerDisconnectedError)):
        return True
    #unknown treat as transient
    return True

# -----------------------
# Streaming fetch helper (reads chunks, returns tuple)
# -----------------------

async def fetch_html_stream(session: ClientSession, url: str, max_bytes: int, prefetch_bytes: int = 16*1024) -> Tuple[bytes, int, Dict, Optional[bytes], Optional[str]]:
    """
    Streams from URL:
      - reads up to prefetch_bytes first and returns them as prefetched_bytes (may be None if empty)
      - Also returns an active resp iterator so caller can continue streaming if desired.
    Return tuple:
    (prefetched_bytes, status, headers, remaining_body_bytes_if_response_ended, error_msg)
    """
    headers = {"User-Agent": settings.user_agent}
    timeout = aiohttp.ClientTimeout(total = settings.request_timeout_s)

    async with session.get(url, headers=headers, timeout=timeout) as resp:
        status = resp.status
        hdrs = dict(resp.headers)

        # If client error 4xx (except 429), we propagate as ClientResponseError to let caller decide (permanent)
        if 400 <= status < 500 and status != 429:
            raise ClientResponseError(request_info = resp.reqest_info, history = resp.history, status = status, message = f"client error {status}")
        
        # stream read: gather up to prefetch_bytes (small), then return control to caller to continue streaming
        pref_chunks = []
        pref_total = 0

        # read until either prefetch_bytes reached or stream exhausted
        async for chunk in resp.content.iter_chunked(64*1024):
            pref_chunks.append(chunk)
            pref_total += len(chunk)

            if pref_total >= prefetch_bytes:
                # we have enough to inspect; create a small iterator for the remainder
                # But we can't "rewind" the response — we will return the prefetched bytes and the rest of the stream
                # by continuing to iterate from resp.content in the caller.
                break
            
        pref_bytes = b"".join(pref_chunks)
        # Now, to allow caller to continue reading remaining chunks, we return resp and let caller iterate over resp.content
        # However, we cannot return resp object across context manager exit — thus we must let the caller perform streaming within this context.
        # So this helper is designed to be used with "async with session.get(...) as resp:" at caller, not here.
        # To keep the API simple, we will instead implement the caller logic inline (so we prefer the caller open the session.get).
        # To keep this function usable, we'll return pref_bytes, status, hdrs and let caller fetch again (but that causes a second request).
        # That's undesirable. Therefore, we will not use this helper; we'll implement prefetch+streaming inside the main function below.
        # For compatibility we return pref_bytes and headers and status and None for remaining.

        body_rest = None
        return pref_bytes, status, hdrs, body_rest, None
    # Note: Above helper includes a limitation; in the merged implementation below, we avoid calling fetch_html_stream()
    # and instead perform the streaming + prefetch logic inline so we can both prefetch and then continue streaming the same response.

# -----------------------
# DB upsert wrapper with retries (simple)
# -----------------------

async def db_upsert_with_retries(db_upsert_cb: Callable[..., Awaitable[Optional[str]]], *args, **kwargs) -> Optional[str]:
    tries = getattr(settings, "db_retries", 3)
    for attempt in range(tries):
        try:
            return await db_upsert_cb(*args, **kwargs)
        except Exception as e:
            logger.exception("db upsert attempt %s failed: %s", attempt + 1, e)
            if attempt + 1 >= tries:
                raise
        await asyncio.sleep(backoff_delay(attempt))
        
    
# -----------------------
# SPA detection (operates on bytes)
# -----------------------

import re

_SCRIPTS_RE = re.compile(rb"<script\b", re.I)
_APP_ROOT_RE = re.compile(rb'id=["\'](?:app|root|__next)["\']', re.I)
_REACT_DATA_RE = re.compile(rb"data-reactroot|window\.__INITIAL_STATE__|window\.__PRELOADED_STATE__", re.I)

def detect_spa_bytes(html_bytes: bytes, headers: dict) -> bool:
    """
    Heuristic SPA detection working on bytes (no decode required).
    """
    text = html_bytes #bytes
    if _APP_ROOT_RE.search(text):
        return True
    
    n_scripts = len(_SCRIPTS_RE.findall(text))
    if n_scripts >= 3 and len(text) < 8_000:
        return True
    
    if _REACT_DATA_RE.search(text):
        return True
    
    ct = (headers or {}).get("content-type","")
    if "text/html" in ct and n_scripts >= 1 and len(text) < 2_000:
        return True
    return False

# -----------------------
# Main merged fetch & store function
# -----------------------

async def fetch_and_store(
        url: str,
        correlationId: str,
        producer,
        db_upsert_cb: Callable[..., Awaitable[Optional[str]]],
        r2_upload_file: Callable[[str, str], Awaitable[None]],
        session: ClientSession,
):
    """
    Combined, robust fetcher:
     - fetch first small chunk(s)
     - run detect_spa on prefetched bytes
     - if SPA -> run Playwright render (which uploads rendered html / snapshot)
     - else -> continue streaming, gzip to temp file, compute hash, upload the temp file to R2
     - upsert DB with retries
     - DLQ on final failures
    """
    normalized = normalize_url(url)
    url_hash = sha256_hex(normalized.encode("utf-8"))
    domain = urlparse(normalized).netloc

    await acquire_domain_slot(domain)
    temp_name = None

    try:
        #fetch with streaming and prefetch
        max_bytes = settings.max_content_length_mb * 1024 * 1024
        fetch_retries = max(1, getattr(settings, "fetch_retries", 2))
        prefetch_size = getattr(settings, "prefetch_bytes", 16 * 1024) # bytes to prefetch for SPA detection

        resp = None
        last_exc = None
        body_status = None
        body_headers = None

        for attempt in range(fetch_retries):
            try:
                timeout = aiohttp.ClientTimeout(total = settings.request_timeout_s)
                resp = await session.get(normalized, headers = {"User-Agent": settings.user_agent}, timeout = timeout)
                # Note: do not use async with here because we must manage the response lifecycle manually for streaming continuation
                # However aiohttp response must be closed eventually; we'll ensure resp.release() / resp.close() later.

                # check status early for permanent client errors
                body_status = resp.status
                body_headers = dict(resp.headers)

                if 400 <= body_status < 500 and body_status != 429:
                    # permanent client error — raise to be handled below
                    raise ClientResponseError(request_info=resp.request_info, history=resp.history, status=body_status, message=f"Client error {body_status}")
                
                # prefetch chunks up to prefetch_size
                pref_chunks = []
                pref_total = 0

                async for chunk in resp.content.iter_chunked(64 * 1024):
                    pref_chunks.append(chunk)
                    pref_total += len(chunk)

                    if pref_total >= prefetch_size:
                        break
                    
                pref_bytes = b"".join(pref_chunks)

                # Decide SPA based on prefetched bytes
                if detect_spa_bytes(pref_bytes, body_headers):
                    # We will not continue streaming this response; close it and use Playwright
                    # Ensure connection resources are released

                    try:
                        await resp.release()
                    except Exception:
                        try:
                            resp.close()
                        except Exception:
                            pass
                    
                    # Call Playwright renderer which uploads the rendered HTML (and snapshot optionally)
                    rendered_key = f"rendered/{datetime.now(timezone.utc).year:04d}/{datetime.now(timezone.utc).month:02d}/{datetime.now(timezone.utc).day:02d}/sha256-{url_hash}.rendered.html.gz"
                    #snapshot_key = f"snapshot/{datetime.now(timezone.utc).year:04d}/{datetime.now(timezone.utc).month:02d}/{datetime.now(timezone.utc).day:02d}/sha256-{url_hash}.png"

                    try:
                        rendered_content_hash, r2_key_rendered, returned_snapshot_key = await render_page_and_upload(
                            normalized,
                            r2_key_html=rendered_key,
                            snapshot_key=None,
                            wait_for_selector=None,
                            timeout_ms=getattr(settings, "playwright_timeout_ms", 30_000)
                        )
                    except Exception as e:
                        logger.exception("Playwright render failed for %s", normalized)

                        # DLQ and exit
                        await producer.send_and_wait(settings.topic_dlq, json.dumps({
                            "reason": "playwright_render_failed",
                            "url": normalized,
                            "error": str(e),
                            "correlationId": correlationId
                        }).encode("utf-8"))
                        return
                    
                    # On success, upsert DB with rendered keys (adapt to your DB upsert signature)
                    r2_key = r2_key_rendered
                    r2_url = f"{settings.r2_endpoint.rstrip('/')}/{settings.r2_bucket}/{r2_key}"
                    ttl_days = settings.raw_ttl_days
                    ttl_expire = datetime.now(timezone.utc) + timedelta(days = ttl_days)

                    # call DB upsert wrapper
                    try:
                        doc_id = await db_upsert_with_retries(
                            db_upsert_cb,
                            normalized,
                            url_hash,
                            domain,
                            None,
                            r2_key,
                            returned_snapshot_key,
                            settings.r2_bucket,
                            r2_url,
                            rendered_content_hash,
                            body_status,
                            body_headers,
                            ttl_expire
                        )
                    except Exception as e:
                        logger.exception("DB upsert failed after Playwright upload; DLQ'ing orphan info for %s", r2_key)    
                        await producer.send_and_wait(settings.topic_dlq, json.dumps({
                            "reason": "db_upsert_after_upload_failed",
                            "url": normalized,
                            "r2_key": r2_key,
                            "error": str(e),
                            "correlationId": correlationId
                        }).encode("utf-8"))
                        return

                    #Emit event
                    out = {
                        "correlationId": correlationId,
                        "document_id": doc_id,
                        "url": normalized,
                        "r2_bucket": settings.r2_bucket,
                        "r2_key_rendered": r2_key,
                        "r2_snapshot_key": returned_snapshot_key,
                        "r2_url": r2_url,
                        "content_hash": rendered_content_hash,
                        "http_status": body_status,
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                        "url_hash": url_hash,
                        "domain": domain
                    }

                    await producer.send_and_wait(settings.topic_out, json.dumps(out).encode("utf-8"))
                    return
                
                # Not SPA: we will continue streaming the response, but we have already consumed prefetched chunks.
                # Prepare temp gz file, sha object and write prefetched chunks then continue reading remaining chunks.

                sha = hashlib.sha256()
                temp = tempfile.NamedTemporaryFile(prefix="s3stream-", suffix=".gz", delete = False)
                temp_name = temp.name
                temp.close()

                gz_file = open(temp_name, "wb")
                gz_writer = gzip.GzipFile(fileobj=gz_file, mode = "wb")

                total = pref_total 

                # update sha and write prefetched chunks
                for c in pref_chunks:
                    sha.update(c)
                    # write chunk in thread
                    await to_thread(gz_writer.write, c)

                # now stream remaining chunks and write them
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    total += len(chunk)
                    if total > max_bytes:
                        #cleanup and abort
                        try:
                            await to_thread(gz_writer.close)
                        except Exception:
                            pass
                        try:
                            await to_thread(gz_file.close)
                        except Exception:
                            pass
                        try:
                            os.remove(temp_name)
                        except Exception:
                            pass
                        raise ValueError("Body too large")
                    sha.update(chunk)
                    await to_thread(gz_writer.write, chunk)
                
                # finalise gzip
                await to_thread(gz_writer.close)
                await to_thread(gz_file.close)

                content_hash = sha.hexdigest()
                # success break
                break
            except Exception as e:
                last_exc = e
                logger.exception("fetch attempt %s for %s failed: %s", attempt + 1, normalized, e)
                #classify
                transient = is_transient_fetch_exception(e)

                # close and cleanup resp if open
                try:
                    if resp:
                        await resp.release()
                except Exception:
                    try:
                        resp.close()
                    except Exception:
                        pass
                if not transient:
                    # permanent -> DLQ
                    await producer.send_and_wait(settings.topic_dlq, json.dumps({
                        "reason": "fetch_permanent",
                        "url": normalized,
                        "status": getattr(e, "status", None),
                        "error": str(e),
                        "correlationId": correlationId
                    }).encode("utf-8"))
                    # cleanup temp if any
                    if temp_name and os.path.exists(temp_name):
                        try:
                            os.remove(temp_name)
                        except Exception:
                            pass
                    return
                
                if attempt + 1 >= fetch_retries:
                    #final transient failure -> DLQ
                    await producer.send_and_wait(settings.topic_dlq, json.dumps({
                        "reason": "fetch_error",
                        "url": normalized,
                        "error": str(e),
                        "correlationId": correlationId
                    }).encode("utf-8"))
                    if temp_name and os.path.exists(temp_name):
                        try:
                            os.remove(temp_name)
                        except Exception:
                            pass
                    return
                # retry after backoff
                await asyncio.sleep(backoff_delay(attempt))
            else:
                # unlucky: exhausted loop without break
                await producer.send_and_wait(settings.topic_dlq, json.dumps({
                    "reason": "fetch_error",
                  "url": normalized,
                  "error": str(last_exc),
                  "correlationId": correlationId
                }).encode("utf-8"))
                if temp_name and os.path.exists(temp_name):
                  try:
                      os.remove(temp_name)
                  except Exception:
                      pass
                return
            # At this point we have a gzipped temp file at temp_name and content_hash computed
            # upload temp file with retries

            r2_date_key = s3_key_for_raw(url_hash)
            if getattr(settings, "use_staging_for_uploads", False):
                r2_key = settings.staging_prefix.rstrip('/') + "/" + r2_date_key
            else:
                r2_key = r2_date_key

            upload_retries = max(1, getattr(settings, "upload_retries", 2))
            for attempt in range(upload_retries):
                try:
                    await r2_upload_file(temp_name, r2_key)
                    break
                except Exception as e:
                    logger.exception("r2 upload attempt %s failed for %s: %s", attempt + 1, normalized, e)
                    if attempt + 1 >= upload_retries:
                        await producer.send_and_wait(settings.topic_dlq, json.dumps({
                            "reason": "r2_upload_error",
                          "url": normalized,
                          "error": str(e),
                          "correlationId": correlationId
                        }).encode("utf-8"))

                        #cleanup
                        if temp_name and os.path.exists(temp_name):
                            try:
                                os.remove(temp_name)
                            except Exception:
                                pass
                        return
                    await asyncio.sleep(backoff_delay(attempt))
            
            # build r2_url and ttl
            r2_url = f"{settings.r2_endpoint.rstrip('/')}/{settings.r2_bucket}/{r2_key}"
            ttl_days = settings.raw_ttl_days
            ttl_expire = datetime.now(timezone.utc) + timedelta(days = ttl_days)

            # DB upsert with retries
            try:
                doc_id =  await db_upsert_with_retries(
                    db_upsert_cb,
                  normalized, url_hash, domain,
                  r2_key,                 # r2_key_raw
                  None,                   # r2_key_rendered
                  None,                   # r2_snapshot_key
                  settings.r2_bucket,
                  r2_url,
                  content_hash,
                  body_status,
                  body_headers,
                  ttl_expire
                )
            except Exception as e:
                logger.exception("DB upsert failed after upload; DLQ'ing orphan info for %s", r2_key)
                await producer.send_and_wait(settings.topic_dlq, json.dumps({
                    "reason": "db_upsert_after_upload_failed",
                  "url": normalized,
                  "r2_key": r2_key,
                  "error": str(e),
                  "correlationId": correlationId
                }).encode("utf-8"))

                # do not delete the uploaded object immediately; GC will handle it later
                if temp_name and os.path.exists(temp_name):
                  try:
                      os.remove(temp_name)
                  except Exception:
                      pass
                return
            # success: emit event
            out = {
                "correlationId": correlationId,
                "document_id": doc_id,
                "url": normalized,
                "r2_bucket": settings.r2_bucket,
                "r2_key_raw": r2_key,
                "r2_url": r2_url,
                "content_hash": content_hash,
                "http_status": body_status,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "url_hash": url_hash,
                "domain": domain
            }
            await producer.send_and_wait(settings.topic_out, json.dumps(out).encode("utf-8"))
        
    finally:
        # cleanup temp file if exists (we keep uploaded object in R2)
        if temp_name and os.path.exists(temp_name):
            try:
                os.remove(temp_name)
            except Exception:
                pass
        
        await release_domain_slot(domain)
           

                                         

    




