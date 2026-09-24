# census-api (Flask) — Census Reporter backend

JSON API at api.censusreporter.org, backed by PostgreSQL/PostGIS. It's old code in production, so **keep changes small and incremental.**
For the whole-system map (5 sibling repos), see `../censusreporter/CLAUDE.md`. For API semantics, see `API.md`.
**`DATA_UPDATES.md` is the canonical runbook for loading a new ACS/TIGER release** across all repos.

## Layout
- `census_extractomatic/api.py` (~2200 lines): nearly everything. It has app setup, release config, and all routes. Key globals at the top:
  - `allowed_acs`: releases the API serves, in preference order. `/latest/` and 1-year→5-year fallback walk this list.
  - `ACS_NAMES`: display names/years. `allowed_tiger`: TIGER schemas, newest first.
  - `default_table_search_release`, `release_to_expand_with`, `PARENT_CHILD_CONTAINMENT`, `SUMLEV_NAMES`.
- Main routes: `/1.0/geo/search`, `/1.0/geo/<tiger>/<geoid>[/parents]`, `/1.0/geo/show/<tiger>`, vector/geojson tiles,
  `/1.0/table/*`, `/2.0/table/<release>/<table>`, `/1.0/tabulation/*`, `/1.0/data/show/<acs>` (the workhorse for profiles and comparisons),
  `/1.0/data/download/<acs>` (uses GDAL/OGR exporters), `/1.0/data/compare/...`, `/2.1/full-text/search`, `/1.0/user_geo/*`, `/1.0/aggregate/*`.
- `validation.py`: `qwarg_validate` decorator plus validators for query args.
- `exporters.py`: Excel/CSV/OGR download formats.
- `full_text_search.py` + `full-text-search/`: Postgres full-text search. The index is built by `metadata_script.sql`.
- `user_geo.py`: user-uploaded geographies, aggregated from 2010/2020 decennial blocks (`dec20X0_pl94`) via a Celery task (Redis broker).
  `aggregation/AGGREGATION.md` describes the manual ops process.
- `tools/`: `topic_scraper`, `update_table_priorities` (run as `python -m census_extractomatic.tools.X`).
- `sitemap/build_all.py`: writes sitemap XML into `../censusreporter/.../static/sitemap/`.
- Top-level `extract_*.py`, `fabfile.py`, `server/`: legacy and mostly unused (Elasticsearch/fabric era).

## Database shape (what the SQL expects)
- One schema per release, e.g. `acs2024_5yr`. Each table has a `bNNNNN_moe` table (estimates + `_moe` columns) and a `bNNNNN` view (estimates only),
  keyed by `geoid` (Census Reporter format `16000US1714000`). There's also `geoheader` and `census_table_metadata`/`census_column_metadata`.
- `tigerYYYY.census_name_lookup` (names, geometries, sumlevel, full_geoid) and `tigerYYYY.census_geo_containment` (parent/child relations).
- `public.census_tabulation_metadata`: the unified cross-release table metadata. Full-text search tables live alongside it.
- SQL is raw `text()` via Flask-SQLAlchemy (`db.session.execute`). No ORM models.

## Running locally
- `pipenv install`, then `pipenv run flask run` (port 5000). Env comes from `.envrc` (direnv): `DATABASE_URL`, `EXTRACTOMATIC_CONFIG_MODULE`,
  `FLASK_APP=census_extractomatic/api.py`, `CACHE_TYPE`, `REDIS_URL`, AWS creds.
- The DB is normally reached through an **SSH tunnel to the production/staging database on localhost:5433**, so queries hit real data. Treat it as read-only.
- GDAL on Apple Silicon needs a special install (see `DEVELOPMENT.md`). The Python bindings must match Homebrew's GDAL version, which is `brew pin`ned.
  `No module named '_gdal'` / `Library not loaded: libgdal.NN.dylib` means they're out of sync; rebuild them with the DEVELOPMENT.md command.
- Port 5000 is often taken by macOS AirPlay. Use `flask run --port 5055` if needed. New Relic logs a harmless `memory_utilization` TypeError locally.
- Celery (only needed for user_geo work): `celery -A census_extractomatic.user_geo:celery_app worker`.
- There is no automated test suite.

## Deployment — do not do this without explicit permission
- Remotes: `origin` (GitHub) and `dokku` (dokku.censusreporter.org). **`git push dokku` deploys to production.**
- `Procfile`: `web` (gunicorn) and `worker` (celery). New Relic is initialized at import time in api.py.

## Known rough edges
- Most SQL in api.py hardcodes `tigerYYYY.` rather than using `allowed_tiger[0]`. `exporters.py` does it the better way: its download builders take a `tiger_release`
  argument, passed as `allowed_tiger[0]`. (It can't import from api.py, which would be a circular import.)
- The download builders in `exporters.py` are reached only through the `supported_formats` dict, so grepping for their function names finds nothing but the definitions.
- `.envrc` contains live credentials. Never print, copy, or commit them.
