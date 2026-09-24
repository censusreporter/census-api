# Historical data via Parquet/GeoParquet cold storage

## Problem

census-api's Postgres database only has room for the current ACS releases
(`allowed_acs` in `census_extractomatic/api.py` today lists just
`acs2024_1yr` and `acs2024_5yr`, each its own Postgres schema on the
production host's attached disk). Users regularly ask for older releases
and year-over-year comparisons, but there's no room to keep every past
release "hot" on that disk indefinitely.

## Goals

- Serve historical ACS estimates (`data/show`, `data/compare`, table
  metadata) and historical TIGER geography (`geo/*` lookups, boundaries)
  for releases back to roughly 2010, from cheap object storage rather
  than attached database disk.
- Do not modify the behavior, code paths, or dependencies of the existing
  Postgres-backed "hot" serving path for current releases. This is
  strictly additive.
- Keep serving through the same census-api process and the same public
  endpoint URLs — no new service to deploy, no client-visible API change.
- Be testable entirely on a local machine, without needing real S3 access
  or a mock-S3 service, before deploying.

## Non-goals (for this iteration)

- Migrating current/hot releases off Postgres.
- Full-text search across historical table metadata (out of scope until
  it's clear this is needed).
- A general-purpose ETL orchestrator for the backfill; a one-off driver
  script over the existing per-release build tools is sufficient.

## Architecture

census-api stays a single Flask process. Each release-serving route
(`data/show`, `data/compare`, `table` metadata, `geo/*`) gets a **storage
routing guard** added at the top of its existing function:

1. If the requested release/year is in the existing `allowed_acs` /
   `allowed_tiger` hot lists, execution falls through to the current code,
   completely unchanged.
2. Else if it's in a new `cold_releases` registry, delegate to a new
   DuckDB-backed query module and format the response identically to the
   hot path's response shape.
3. Else, 404 — same as today.

This keeps the hot path byte-for-byte unchanged while adding a second
storage engine behind the same API surface.

## Storage layout

Parquet/GeoParquet files, addressed by a local path in dev/test or an
`s3://` URL in production (DuckDB's `read_parquet()`/`ST_Read()` accept
either transparently):

- ACS estimates + MOE: `{root}/acs/{release}/{table_id}.parquet` — one
  file per table per release, columns = geoid + interleaved
  estimate/MOE columns, **rows sorted by geoid** so DuckDB's row-group
  min/max statistics prune effectively on `geoid IN (...)` filters.
- ACS metadata: `{root}/acs/{release}/_metadata/census_table_metadata.parquet`,
  `census_column_metadata.parquet`, `geoheader.parquet` — small, read in
  full per query, mirroring the metadata tables read from Postgres today.
- TIGER geometries: `{root}/tiger/{year}/{sumlevel}.parquet` as
  GeoParquet (WKB geometry column + bbox-covering column for spatial
  pruning), one file per geography level per vintage — same partitioning
  census-sqlite's `build_tiger.py` already produces per SQLite DB.
- TIGER containment relationships: `{root}/tiger/{year}/containment.parquet`.

## Release / geo-vintage correlation

A `cold_releases` registry (same shape as today's `ACS_NAMES`) maps each
historical ACS release to:
- its Parquet root path/prefix, and
- the **TIGER year it must be paired with** (e.g. `acs2013_5yr` →
  `tiger2013`).

Cold-path `data/show` and `geo/*` lookups always resolve geography using
the release's paired TIGER year, never "latest." This is required because
GEOIDs and boundaries change across vintages — a 2013 tract must resolve
against 2013 TIGER, not today's TIGER, even if that tract has since split
or been renamed. `latest` continues to resolve only against the hot list,
so current client behavior is unaffected.

## Query engine

New module, e.g. `census_extractomatic/cold_storage.py`:
- Opens a DuckDB connection per request (or a small pooled/reused
  connection — an implementation detail for the plan) with the `httpfs`
  and `spatial` extensions loaded.
- S3 access configured via new environment variables following this
  repo's existing `os.environ.get(...)` convention in `config.py` (e.g.
  `COLD_STORAGE_S3_BUCKET`, `COLD_STORAGE_S3_REGION`, plus standard AWS
  credential env vars/IAM role — exact names finalized in the
  implementation plan).
- Exposes functions mirroring the shapes of the existing hot-path
  Postgres queries in `api.py`: table metadata lookup, `{table_id}_moe`
  -equivalent join across requested table_ids filtered by geoid, geo
  name/population lookup, and containment lookup for `geo/.../parents`.
- Each call is a straightforward `SELECT ... FROM
  read_parquet('{root}/{table_id}.parquet') WHERE geoid IN (...)` (or
  `ST_Read`/spatial equivalent for geometry) — no persistent
  cross-request state.

## Build / ETL pipeline

Duplicate census-sqlite's `build.py` and `build_tiger.py` outright into
new scripts (rather than sharing a writer abstraction with the SQLite
versions — explicit choice to avoid coupling the two output formats):
- The duplicated `build.py` writes each table's rows to
  `acs/{release}/{table_id}.parquet` (sorted by geoid) instead of
  `CREATE TABLE ... executemany` into SQLite, using PyArrow/DuckDB as the
  writer. Metadata tables get the same treatment into `_metadata/`.
- The duplicated `build_tiger.py` writes GeoParquet (via DuckDB's spatial
  extension, which can emit a bbox-covering column) instead of loading
  into SpatiaLite, after the same fiona/shapely geometry-building logic
  it already has.
- `build_containment_relationships`'s spatial intersection logic is
  unchanged; output goes to `containment.parquet` instead of a SQLite
  table.
- Backfilling ~2010+ requires looping the per-release/per-year build over
  the full historical range; a thin driver script invoking the duplicated
  build scripts per year/release and uploading each output to S3 as it
  completes is sufficient — no fancier orchestration needed for a
  largely one-off backfill.

## Error handling

Cold-path failures map to the same HTTP semantics the hot path already
uses:
- Unrecognized/unsupported release → 404 via `get_acs_name`/`abort(404,
  ...)`, exactly as today.
- Geoid not present in the requested release's parquet files → 404, same
  as a missing hot-path geoid.
- DuckDB/S3 read failure (network blip, missing object, credential
  issue) → 502/503 rather than a raw 500, logged with release + table_id
  + geoid context rather than surfacing DuckDB's raw error to the client.

## Testing

DuckDB's `read_parquet()`/`ST_Read()` treat a local filesystem path and
an `s3://` URL identically, so:
- `cold_releases` config points at a local fixtures directory in
  dev/test and at real `s3://` URLs in production, via the same
  environment-driven config pattern the rest of `config.py` uses.
- Automated tests build small Parquet/GeoParquet fixture files once
  (via the duplicated build scripts run against a tiny slice of real
  data, or hand-built with pyarrow/duckdb) and check them into the test
  fixtures directory. The suite runs against these local files — no S3,
  no network, no MinIO/mock-S3 required.
- A separate manual local smoke test (pointing `cold_releases` at a real
  `s3://` bucket with read-only credentials) validates actual S3/httpfs
  network and credential behavior before deploying — this is the one
  thing the local-filesystem fixture tests can't cover.

## Open questions for the implementation plan

- Exact DuckDB connection lifecycle (per-request vs. pooled/reused) and
  memory-footprint tuning under concurrent cold-path requests.
- Exact S3/AWS credential env var names and how they're wired into
  Dokku's config for this app.
- Whether `data/compare` and `table/search` need cold-path support in
  the first version, or can be added after `data/show`/`geo/*` prove out.
- Source and exact scope of the ~2010+ backfill (which ACS 1yr/5yr years
  and TIGER vintages are actually available/worth building first).
