import asyncio
import logging
import asyncpg
from typing import Any, Dict, Optional
from datetime import datetime, timezone
from config import settings

logger = logging.getLogger("parser.db")
db_pool = Optional[asyncio.pool.Pool] = None

#prepared statements SQL
#1) lightweight lookup to check idempotency
SELECT_META_SQL = """
SELECT id, content_hash, parsed, parsed_at, document_id
FROM metadata
WHERE url = $1
"""

#2) mark processing (upsert minimal status)
MARK_PROCESSING_SQL = """
INSERT INTO metadata (url, document_id, parsed, parsed_status, updated_at)
VALUES ($1, $2, FALSE, 'processing', $3)
ON CONFLICT (url) DO UPDATE SET parsed_status = 'processing', updated_at = $3
RETURNING id;
"""

#3) Parser upsert -- updates parser specific columns only on metadata table
PARSER_UPSERT_SQL = """
INSERT INTO metadata (document_id, url, url_hash, domain, r2_bucket, 
r2_key_rendered, r2_parsed_prefix, content_hash, http_status, fetcheed_at, 
parsed, parsed_at, chunk_count, parsed_status, parse_warnings, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14. $15, $16)
ON CONFLICT (url) DO UPDATE SET
  r2_parsed_prefix = EXCLUDED.r2_parsed_prefix,
  content_hash = EXCLUDED.content_hash,
  parsed = EXCLUDED.parsed,
  parsed_at = EXCLUDED.parsed_at,
  chunk_count = EXCLUDED.chunk_count,
  parsed_status = EXCLUDED.parsed_status,
  parse_warnings = EXCLUDED.parse_warnings,
  updated_at = EXCLUDED.updated_at
  RETURNING id;
  """

async def _prepare_statements(conn: asyncpg.Connection) -> None:
    try:
        conn.select_meta_stmt = await conn.prepare(SELECT_META_SQL)
        conn.mark_processing_stmt = await conn.prepare(MARK_PROCESSING_SQL)
        conn.parser_upsert_stmt = await conn.prepare(PARSER_UPSERT_SQL)
        logger.debug("Prepared parser statements on connection")
    except Exception as e:
        logger.exception("failed preparing statements on new connection")

async def start_pg():
    global db_pool
    if db_pool:
        return db_pool
    
    db_pool = await asyncpg.create_pool(
        dsn = settings.POSTGRES_DSN,
        min_size = getattr(settings, "DB_POOL_MIN_SIZE", 1),
        max_size = getattr(settings, "DB_POOL_MAX_SIZE", 10),
        init = _prepare_statements
    )
    logger.info("Postgres connection pool for Parser created")
    return db_pool

async def close_pg():
    global db_pool
    if db_pool:
        await db_pool.close()
        bd_pool = None
        logger.info("Parser postgres connection pool closed")
      
async def fetch_metadata(url: str) -> Optional[Dict]:
    async with db_pool.acquire() as conn:
        rec = await conn.select_meta_stmt.fetchrow(url) if getattr(conn, "select_meta_stmt", None) else conn.fetchrow(SELECT_META_SQL, url)
        if rec:
            return dict(rec)
        return None

async def mark_processing(url: str, document_id: Optional[str] = None) -> Optional[str]:
    now = datetime.now(timezone.utc)
    async with db_pool.acquire() as conn:
        rec = await conn.mark_processing_stmt.fetchrow(url, document_id, now) if getattr(conn, "make_processing_stmt", None) else conn.fetchrow(MARK_PROCESSING_SQL, url, document_id, now)
        return str(rec[id]) if rec else None

async def parser_upsert(doc: dict) -> Optional[str]:
    """
    doc expected keys:
    document_id, url, url_hash, domain, r2_bucket, r2_key_rendered,
    r2_parsed_prefix, content_hash, http_status, fetched_at,
    parsed (bool), parsed_at (datetime), chunk_count (int), parsed_status (str),
    parse_warnings (dict), updated_at (datetime)
    """

    async with db_pool.acquire() as conn:
        try:
            params = (
                doc.get("document_id"),
                doc.get("url"),
                doc.get("url_hash"),
                doc.get("domain"),
                doc.get("r2_bucket"),
                doc.get("r2_key_rendered"),
                doc.get("r2_parsed_prefix"),
                doc.get("content_hash"),
                doc.get("http_status"),
                doc.get("fetched_at"),
                doc.get("parsed", True),
                doc.get("parsed_at"),
                doc.get("chunk_count"),
                doc.get("parsed_status", "done"),
                doc.get("parse_warnings"),
                doc.get("updated_at", datetime.now(timezone.utc))
            )
            upsert_stmt = getattr(conn, "parser_upsert_stmt", None)
            if upsert_stmt:
                rec = await upsert_stmt.fetchrow(*params)
            else:
                rec = await conn.fetchrow(PARSER_UPSERT_SQL, *params)
            return str(rec["id"]) if rec else None
        except Exception:
            logger.exception(f"Failed to upsert parser metadata for url: %s", doc.get("url"))
            return None
    