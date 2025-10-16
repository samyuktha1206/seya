## Purpose

Short, actionable guidance for AI coding agents working on the scraper-service microservice.

## Big picture (what this service does)
- This is a small async Python scraper service that: consumes search result events from Kafka, fetches HTML (optionally renders with Playwright), stores raw HTML in Cloudflare R2 (S3 API), persists metadata to Postgres, and produces a "scraper.fetched.v1" event back to Kafka.
- Key flows: Kafka -> fetcher (aiohttp / Playwright) -> R2 upload (aioboto3) -> Postgres upsert (asyncpg) -> Kafka out.

## Key files to read first
- `app/config.py` — pydantic settings and env var prefixes (SEYA_). Important defaults: concurrency, per-domain delay, Kafka topics, R2 and DB connection details.
- `app/kafka_client.py` — module-level singletons: `consumer` and `producer`; `init_kafka()` and `close_kafka()` lifecycle.
- `app/fetcher.py` — main fetching logic, per-domain politeness, s3 key generation, and message production. Contains several project-specific patterns and some existing bugs/typos (see "Watchouts").
- `app/r2_storage.py` — compress and upload raw HTML to R2 using aioboto3; helper `r2_object_url` for public URL formation.
- `app/db.py` — asyncpg pool initialization and prepared-statement pattern. Note: in production the CREATE TABLE block should be replaced by migrations.
- `app/models.py` — pydantic schemas for inbound/outbound Kafka messages (useful when validating and constructing messages).
- `app/playwright_render.py` — Playwright-based rendering utilities (currently incomplete in repository snapshot).

## Project-specific conventions & patterns
- Settings: uses `pydantic-settings` with `env_prefix='SEYA_'` and a `.env` file. Look up `settings` from `app.config` everywhere.
- Singletons: modules expose module-level singletons (`producer`, `consumer`, `dp_pool`) that are initialized by calling `init_*` functions. Agents should prefer calling those in tests or entrypoints rather than re-creating objects.
- DB prepared statements: `db._prepare_statements` attaches prepared statements to `asyncpg.Connection` objects (e.g., `conn._upsert_stmt`) for reuse.
- Politeness/concurrency: `fetcher.py` implements a global semaphore and per-domain semaphores plus a per-domain delay (`settings.per_domain_delay_s`). This is the canonical pattern for throttling — preserve it when changing fetch logic.
- Storage: raw HTML is gzipped before upload and stored under date-based keys (`raw/YYYY/MM/DD/sha256-<hash>.html.gz`). `r2_storage.upload_to_r2` performs gzip + put_object with `ContentEncoding='gzip'`.

## Integration points & external dependencies
- Kafka: topics configured in `app/config.py` (e.g., `topic_in`, `topic_out`, `topic_dlq`) and connected via `aiokafka` in `app/kafka_client.py`.
- Cloud R2 (S3-compatible): `aioboto3` + `botocore` config in `app/r2_storage.py` and `app/playwright_render.py`.
- Postgres: `asyncpg` used directly; migrations are not present — the code will create the `metadata` table in dev if the DB is reachable.
- Optional Playwright rendering: `app/playwright_render.py` contains Playwright usage; `config.use_playwright` toggles behavior.

## Concrete developer workflows (discoverable from repo)
- Environment: settings loaded from `.env` (read by `pydantic-settings`) with `SEYA_` prefix.
- Install Python deps (example list derived from imports): aiohttp, aioboto3, aiokafka, asyncpg, playwright, pydantic-settings, botocore. Install and then install Playwright browsers:

```powershell
python -m pip install -r requirements.txt  # or install packages listed above
python -m playwright install
```

- Database: provide `SEYA_DATABASE_URL`. In development the service will attempt to create the `metadata` table automatically; in prod use proper migrations (Alembic).

## Common bugs & watchouts (explicit, actionable)
These are observable problems in the current snapshot — fix these before running or creating callers/tests that rely on the modules:
- `fetcher.py` typos and name bugs:
  - `asyncio.semaphore` should be `asyncio.Semaphore` (case-sensitive constructor).
  - `global_semaphore = asyncio.semaphore(settings.concurrency)` will raise — change to `asyncio.Semaphore(settings.concurrency)`.
  - `domain_locks` type annotations use `asyncio.semaphore`; prefer `asyncio.Semaphore`.
  - `normalize_url` uses `p.scheme.lower()` instead of `p.netloc`; this produces invalid URLs. Use `p.netloc` and ensure scheme defaults to `https`.
  - `fetch_html_stream` type hints declare `-> bytes` but actually returns `(body, status, headers)` tuple; keep types accurate.
  - `fetch_html_store` references `producer` without importing it; either import `producer` from `app.kafka_client` or accept it as an explicit parameter. Also several typos: `ClientSsession` (extra s), `settings.r2_buckets` vs `settings.r2_bucket`, `uft-8` typo in encoding, inconsistent variable names (db_pool vs dp_pool).
  - These bugs are high priority — they cause runtime NameError/AttributeError/TypeError.
- `db.py` issues:
  - module defines `dp_pool` but uses `db_pool` — normalize to one name and ensure `init_db` assigns the module-level pool variable.
- `playwright_render.py` is incomplete: signature `def set_global_render_semaphore()` missing a colon and body. Expect more work to wire Playwright rendering into the fetch flow.

## How to modify safely
- When editing cross-module singletons (producer/consumer/db pool), update both initialization and imports where used. Prefer a small change that explicitly imports the singleton: `from .kafka_client import producer` rather than assuming globals exist in other modules.
- Preserve the polite throttling pattern in `fetcher.py` when adding parallelism or render steps.
- Keep R2 uploads gzipped (consumers downstream expect gzip ContentEncoding) — check `r2_storage.upload_to_r2`.

## What to test / quick smoke checks
- Unit test ideas to add: normalize_url, s3_key_for_raw, polite_wait timing, semaphore acquire/release edge cases, upload_to_r2 (mock aioboto3), and the pydantic models in `models.py`.
- Quick manual smoke (once import errors fixed):

```powershell
$env:SEYA_KAFKA_BOOTSTRAP="localhost:9092" ; $env:SEYA_R2_ENDPOINT="https://..." ; $env:SEYA_DATABASE_URL="postgresql://..."
python -c "from app import fetcher, kafka_client; print('imports ok')"
```

## If you're the agent making edits
- First fix the typos listed in "Common bugs & watchouts".
- Run a static check (pylance/mypy/flake8) and run a minimal smoke import as above.
- When adding features, update `app/models.py` with pydantic models and use them for validation before sending Kafka messages.

---
If anything above is unclear or you'd like the instructions tailored (for example include a ready-to-run Dockerfile, tests, or an entrypoint script), tell me which area to expand and I'll iterate.
