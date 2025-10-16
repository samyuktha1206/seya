import asyncpg
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from .config import settings

logger = logging.getLogger("app.db")
db_pool: Optional[asyncpg.pool.Pool] = None

#Upsert SQL

UPSERT_METADATA_SQL = """
INSERT INTO metadata (
url, url_hash, domain, r2_key_raw, r2_key_rendered, r2_snapshot_key, r2_bucket,
r2_url, content_hash, http_status, response_headers, fetched_at, ttl_expire_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), $12)
ON CONFLICT (url) DO UPDATE SET
  r2_key_raw = EXCLUDED.r2_key_raw,
  r2_key_rendered = EXCLUDED.r2_key_rendered,
  r2_snapshot_key = EXCLUDED.r2_snapshot_key,
  r2_bucket = EXCLUDED.r2_bucket,
  r2_url = EXCLUDED.r2_url,
  content_hash = EXCLUDED.content_hash,
  http_status = EXCLUDED.http_status,
  response_headers = EXCLUDED.response_headers,
  ttl_expire_at = EXCLUDED.ttl_expire_at,
  fetched_at = now()
RETURNING id;
"""
CREATE_METADATA_TABLE_SQL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE IF NOT EXISTS metadata (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  url TEXT UNIQUE,
  url_hash TEXT,
  domain TEXT,
  r2_key_raw TEXT,
  r2_key_rendered TEXT,
  r2_snapshot_key TEXT,
  r2_bucket TEXT,
  r2_url TEXT,
  content_hash TEXT,
  http_status INT,
  response_headers JSONB,
  fetched_at TIMESTAMPTZ default now(),
  rendered_by TEXT,
  rendered_at TIMESTAMPTZ,
  parse_warnings JSONB,
  parsed BOOLEAN DEFAULT FALSE,
  ttl_expire_at TIMESTAMPTZ
);
"""

async def _prepare_statements(conn: asyncpg.Connection):
  """
  Called for each new connection in the pool. 
  Prepare commonly used statements and attach them to the 
  commonly used objects for reuse.

  """
  try:
    conn._upsert_stmt = await conn.prepare(UPSERT_METADATA_SQL)
    logger.debug("Prepared upsert statements for a new connection")
  except Exception:
    logger.exception("Failed to prepare statements on connection")

async def init_db():
  """
  Initialize the connection pool and create metadata table (in dev). 
  In production, prefer running migrations (Alembic) and remove the CREATE TABLE block.
  """

  global db_pool
  
  if not settings.database_url:
    logger.info("No DATABASE_URL set; DB disabled")
    return
  
  min_size = getattr(settings, "DB_POOL_MIN_SIZE", 1)
  max_size = getattr(settings, "DB_POOL_MAX_SIZE", 4)

  #create_pool supports an init function that is called for each new connection
  db_pool = await asyncpg.create_pool(
    dsn=settings.database_url,
    min_size=min_size,
    max_size=max_size,
    init=_prepare_statements
  )

  #dev convenience: ensure extension & table exists (use migrations in prod)
  async with db_pool.acquire() as conn:
    try:
      await conn.execute(CREATE_METADATA_TABLE_SQL)
    except Exception:
      logger.exception("Error creating extension/table (use migrations in prod)")
    logger.info("DB initialized (pool min=%s max=%s)", min_size, max_size)

async def close_db():
  """
  Close the connection pool.
  """
  global db_pool

  if db_pool:
    await db_pool.close()
    db_pool = None
    logger.info("DB pool closed")

async def upsert_metadata(
  url: str,
  url_hash: str,
  domain: str,
  r2_key_raw:Optional[str],
  r2_key_rendered:Optional[str],
  r2_snapshot_key:Optional[str],
  r2_bucket:Optional[str],
  r2_url:Optional[str],
  content_hash: Optional[str],
  http_status: int,
  headers: Optional[Dict[str, Any]],
  ttl_expire_at: Optional[datetime],
) -> Optional[str]:
  """
  Insert or update metadata record and return the id (UUID string).
  Uses a prepared statement attached to the connection object.
  """
  global db_pool

  if not db_pool:
    logger.warning("DB pool not initialized: upsert skipped for url=%s", url)
    return None
  
  #normalize headers to a dict (asyncpg maps dict -> JSONB automatically)
  response_headers = headers or {}
  
  async with db_pool.acquire() as conn:
    try:
      #use prepared statements if available
      upsert_stmt = getattr(conn, "_upsert_stmt", None)
      if upsert_stmt:
        rec = await upsert_stmt.fetchrow(
          url, url_hash, domain, r2_key_raw, None, None,
          r2_bucket, r2_url, content_hash, http_status,
          response_headers, ttl_expire_at
        )
      else:
        #fallback to ad-hoc query if prepared not present
        rec = await conn.fetchrow(
          UPSERT_METADATA_SQL, url, url_hash, domain, r2_key_raw,  None, None,
          r2_bucket, r2_url, content_hash, http_status,
          response_headers, ttl_expire_at
        )
      return str(rec["id"]) if rec else None
    except Exception:
      logger.exception("upsert document failed for url=%s", url)
      return None
