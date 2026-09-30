# Plan: pygeoapi performance and correctness (post-standup)

Status: T1–T4 done on DIE-363 and deployed (2026-09-24); T5 done on DIE-364
(`pygeoapi_rebuild` sensor); T6 partly (format version); T7 partly (per-feature bbox). See "Progress" below.
Picks up from the pygeoapi standup (PR #137, branch
`DIE-359-deploy-pygeo-api-on-cloud-run`) and the per-product GeoParquet work (branch
`DIE-363-update-the-layer-generation-code-to-land-in-pygeo-api-instead-of-geo-server`,
4 commits on top of #137, not yet pushed). Read `SPEC.md` §6 first; it describes the
current design.

## Progress (2026-09-24)

Measured locally: patched image, files on local disk, 4 workers, 2 GiB.

| Task | State | Result |
|---|---|---|
| T1 gzip | done | `id=9033` page: 3.85 MB → 83 KB on the wire; unfiltered `limit=5000`: 4.25 MB → 303 KB |
| T2 pin + patch | done | pages are exactly 5,000 rows, no overlap, 48,809 distinct ids; date-only `datetime` returns 200 |
| T3 sort | done, changed | sorted by **`id`, `source`**, then `datetime` (not `source` first: with source first the `id` ranges of row groups overlap and well 9033 hit 4 of 9 groups; id first, 1–2). Whole series 1.3–1.4 s vs 1.6–2.5 s time-sorted |
| T4 bake | done | `--bake-to`; 31/31 collections pass items/bbox/by-id from baked files; 88 MB baked; `limit=10` in 13–35 ms |
| T5 rebuild | done (DIE-364) | `pygeoapi_rebuild` sensor starts the Cloud Build trigger once product runs finish (SPEC §6.6) |
| T6 | partial | `PARQUET_FORMAT_VERSION` done (now `"2"`, so every existing `latest.parquet` is rewritten on the next run). Others open |
| T7 | partial | per-feature `bbox` removed (with the empty-result fix below). The `bbox` covering column is still a property |

**Live results** (baked build with the locally built Parquet files uploaded by hand, 2
workers): the whole well-9033 series in 5,000-row pages takes 9.6–9.7 s (GeoServer ~10 s;
before, ~26 s), with 48,809 distinct rows and no overlap. With 10,000-row pages it takes
7.9–8.0 s: each request has ~0.4 s of fixed cost, so fewer, larger pages are faster.
Most `limit=10` requests take 0.17–0.22 s total, of which ~0.13 s is network. Cloud Run
memory stayed at 26–29% of 2 GiB, and the latency breakdown is almost all user execution.

**Client feedback, fixed after the first live test** (in `patch_pygeoapi.py`):
- Empty results returned `"bbox": [NaN, NaN, NaN, NaN]`, which isn't valid JSON. The
  Parquet provider now leaves out both the collection bbox and the per-feature bboxes.
- `numberMatched` was an estimate except on the last page. Items pages now carry the true
  total, so clients can fetch pages in parallel.
- Not fixed here (separate ticket): WQP `source` labels differ between products. See T6.

The rebuild (`orchestration/pygeoapi/cloudbuild.yaml`) is started by the
`pygeoapi_rebuild` sensor after product runs finish (T5). The build's account needs
`storage.objectViewer` on the products bucket for the bake, as well as what builds need
today.

New finding — **memory at 2 GiB with 4 workers.** Under sustained 10,000-row page load the
local container's cgroup peak reached its 2 GiB cap (anon 1.88 GiB; workers 470–650 MB
each). This is true of the live config too. Cloud Run has no swap, so this would be an OOM
restart. Same load: `ARROW_DEFAULT_MEMORY_POOL=system` peaked at 1,982 MiB (no real help);
`WSGI_WORKERS=2` peaked at 1,781 MiB with the same timings. **Decided: `WSGI_WORKERS=2`**,
set in `cloudbuild.yaml` `--set-env-vars`. Watch Cloud Run memory after deploy; raise to
`--memory=4Gi` if it still approaches the cap.

## Goal

The water-level hydrograph client must be at least as fast against pygeoapi as it was
against GeoServer, and paging must be correct. Every layer should get faster along the way.

## Where things stand

- `die-pygeoapi` is live on Cloud Run: 31 collections, 2 GiB, dedicated service account,
  reading products from `gs://dataservices-die-products/products/{id}/`.
- The live `nm_waterlevels_timeseries` is served from a **manually uploaded**
  `latest.parquet`. It has no `feature_id` column, is sorted by `datetime`, and carries
  a stray `__index_level_0__` column. The other 30 layers are GeoJSON through OGR.
- The DIE-363 branch makes the pipeline write `latest.parquet` for every product and makes
  the image build choose Parquet for each product when the file is usable (point geometry,
  bbox covering column, id column, and a tz-aware `datetime` for timeseries). Locally,
  28 of 31 layers move to Parquet. The 3 polygon layers stay on GeoJSON because the Parquet
  provider's `bbox` is containment-only.

## Measurements to beat

The hydrograph client requests one well (filter by well id) in 5,000-row pages. Timed on
the longest well (id 9033, 48,809 readings):

| | GeoServer | pygeoapi (live, today) |
|---|---|---|
| Time per 5,000-row page | ~1.0 s | ~2.5–3 s |
| Rows returned per page | 5,000 | ~9,600 (pages overlap) |
| Uncompressed size per page | 2.8 MB | 6.9 MB |
| Gzip | yes | no |
| `numberMatched` | true total (48,809) | lookahead (offset + returned + 1) |
| Whole series | ~10 s (10 pages) | ~26 s (10 pages) |

GeoServer is **not** backed by PostGIS. The `<product_id>/geoserver` asset uploads a
GeoPackage that GeoServer stores on its own disk. Its advantages are local disk, a
long-lived datastore, and SQL paging.

Local results (DIE-363 branch, files on local disk, 2 GiB): every layer answers items,
`bbox`, and item-by-id in 12–240 ms. Peak memory is 1,733 MiB. On the live service the same
Parquet reads cost ~1.2 s per request, because pygeoapi builds a new provider on every
request (`load_plugin` in `pygeoapi/api/itemtypes.py`), and opening a `gs://` dataset
takes several GCS round trips.

## Root causes (verified in the pygeoapi 0.25.dev0 source in the image)

1. **Per-request GCS overhead (~1.2 s floor).** The provider is constructed per request.
   Opening `pyarrow.dataset.dataset("gs://...")`, reading the footer, and `get_fields()`
   all go over the network, even for `limit=10` with no filter.
2. **Paging bug in the Parquet provider.** In
   `pygeoapi/provider/parquet.py::_response_feature_collection`:
   ```python
   for batch in batches:
       read += batch.num_rows
       if read > limit:
           batches_list.append(batch.slice(0, limit + 1))   # ignores rows already collected
           break
   ```
   When a filter spreads matches across several batches, a page holds `limit + 1` rows
   *plus* every earlier batch. That's the ~9,600 rows for `limit=5000`, a constant
   +~4,600 at `limit=10000`, and the overlap between pages. Fix: slice
   `limit + 1 - (read - batch.num_rows)`.
3. **`numberMatched` is lookahead by design.** It's `offset + len(rows)` with one extra row
   fetched to decide on a `next` link. Fixing (2) does **not** make it a true total.
   (Since patched to count the filtered rows; see Progress.)
4. **No gzip.** Cloud Run doesn't compress, and `server.gzip` isn't set.
5. **Well lookups scan the whole file.** The timeseries Parquet is sorted by `datetime`,
   so an `id=` filter can't skip row groups.
6. **Bigger rows.** Every Parquet column becomes a property, including the `bbox` covering
   column. geopandas `__geo_interface__` also adds a bbox to every feature. The manual
   file also has `__index_level_0__`, which the pipeline converter doesn't write
   (`index=False`).

## Decisions

- **Bake the product files into the image at build time, and rebuild after each pipeline
  run.** The pipeline is moving to a weekly cadence, so data only changes at rebuilds.
  This removes cause (1) entirely.
  - Rejected: a Cloud Storage FUSE volume mount (the speed gain depends on caching and
    is unmeasured), copying at container startup (unnecessary with weekly data), and
    PostGIS (new always-on infrastructure; reverses SPEC's "no database").
- **Keep the build-time Parquet check** (per product, falling back to GeoJSON). Now it
  also decides which file gets baked.
- **Patch the Parquet provider in the image** rather than fork pygeoapi, and report the
  bug upstream.
- **Out of scope here:** EDR (`/locations/{id}` CoverageJSON; see "Later"), PostGIS.

## Tasks

Do them in order. Each is independently shippable. Re-run the measurement protocol
(below) after T3 and after T5.

### T1 — Gzip responses

- `orchestration/pygeoapi/config.yml.j2`: add `gzip: true` under `server:`.
- Accept: `curl -sI -H 'Accept-Encoding: gzip' .../items?limit=5000` shows
  `Content-Encoding: gzip`, and the compressed size is a fraction of the raw size.
  Clients that don't send `Accept-Encoding` still get plain JSON.

### T2 — Pin the base image, patch the Parquet provider

- `orchestration/pygeoapi/Dockerfile`: replace `geopython/pygeoapi:latest` with a pinned
  digest (`geopython/pygeoapi@sha256:...`). Record the pygeoapi version it contains in a
  comment. Current `:latest` is 0.25.dev0.
- Add a build step that patches the paging line (root cause 2). The patch **must fail the
  build** if the expected source line isn't found, so a base-image bump can't drop it
  silently. A small Python script is safer than `sed`: assert the old text occurs
  exactly once, then replace it.
- Optional, same patch step: coerce naive `datetime_` bounds to UTC before comparing, so
  date-only queries (`2020-01-01/2020-12-31`) stop returning 500 against the tz-aware
  column.
- Open an upstream pygeoapi issue or PR for the paging bug, and link it in the Dockerfile
  comment.
- Accept:
  - `items?id=9033&limit=5000&offset=N` returns exactly 5,000 rows for every page but the
    last, and the pages don't overlap.
  - The union of all pages has 48,809 distinct `feature_id`s.
  - The date-only `datetime` query returns 200 (if the optional fix is done).

### T3 — Sort timeseries Parquet by well, then time

- `backend/persisters/geodataframe.py::collection_to_geoparquet` (currently
  `sort_values(PARQUET_TIME_FIELD)`): for timeseries collections, sort by
  `("id", "source", "datetime")` (see Progress for why `id` first). Other products need
  no particular order.
  - Trade-off: `datetime` queries across all wells lose row-group skipping. They stay
    correct, just slower. The hydrograph is the priority.
  - Consider a smaller `row_group_size` (e.g. 50,000) so one well spans few row groups.
    Measure first.
- Update `tests/test_persisters/test_geodataframe.py::test_datetime_is_utc_and_sorted`.
- Takes effect only when the pipeline rewrites the timeseries Parquet. Existing files
  won't be rewritten on their own: `_sync_parquet` skips when the GeoJSON content hash
  matches, and a converter change doesn't change that hash. Do T6's
  `PARQUET_FORMAT_VERSION` first, or delete the object once.

### T4 — Bake product files into the image

- Build flow (replaces the GCS paths in the config):
  1. With `--network=cloudbuild` (already set), a new build step downloads, for each
     product in `products.yaml`, `latest.parquet` when it passes
     `generate_config.parquet_id_field`, otherwise `latest.geojson`, into
     `/data/products/{id}/`. `generate_config.py` can do the download, e.g. with
     `--bake-to /data/products`.
     - Permission or network errors fail the build (as the check does today).
     - A product with neither usable file fails the build. Better to keep the old revision
       serving than to ship an image that won't start.
  2. Render the config against the baked copies (`--source-root /data/products`).
- Local builds without the build arg keep today's GCS-path config, so a local
  `docker build` still works.
- Image size: ~81 MB of Parquet (all 28 point layers) plus ~15 MB of GeoJSON for the 3
  polygon layers.
- Runtime no longer reads GCS for products. `CPL_MACHINE_IS_GCE` and the service
  account's bucket read become unnecessary but harmless. Remove them in a later cleanup.
- Update `SPEC.md` §6 and the invariants: sources are baked copies taken from GCS at build
  time. Drop "OGR MUST use /vsigs/" and "Parquet MUST use gs://".
- Accept:
  - The live floor for `limit=10` drops from ~1.2 s to local-disk levels (target < 100 ms
    server time).
  - The build log lists each product's baked file and the reason.
  - All 31 collections return 200.

### T5 — Rebuild after a pipeline run (done, DIE-364)

The `pygeoapi_rebuild` sensor (`orchestration/pygeoapi_rebuild.py`, SPEC §6.6) polls
every 10 minutes. After a product job succeeds, and once no product job is queued or
running, it calls `projects.locations.triggers.run` on the `die-pygeoapi-rebuild` Cloud
Build trigger. Several runs finishing on the same day give one build.

- It launches no Dagster run. Sensor ticks cost no Dagster+ credits.
- It rebuilds after every successful product run, even if the data didn't change.
  Skipping unchanged runs (`parquet_status`) was left out for simplicity; builds fall
  within the Cloud Build free tier.
- Schedules are **monthly and staggered** (`products.yaml`: days 1, 15, 22, 23, 25 at
  06:00 America/Denver), so expect up to 5 builds a month.
- It is stopped by default (so local and branch deployments never rebuild prod). Turn it
  on in the prod deployment.

### T6 — Pipeline correctness fixes

- **Unique GeoJSON feature ids.** `backend/persisters/ogc_features.py:565` builds
  timeseries ids as `f"{source}:{site_id}:{date}"`, which repeats for multiple readings on
  one day (499,711 distinct ids in 882,280 features). Use the full `datetime` (or append a
  sequence number). The Parquet `feature_id` already de-duplicates with `:2`, `:3`
  suffixes. Once the GeoJSON ids are unique, those suffixes stop appearing. GeoServer and
  GeoJSON consumers see the id change.
- **Well id isn't unique across sources** (e.g. `10493` is ST2/BernCo). The hydrograph
  client filters by `id` alone. Either filter by `source` too, or add a `site_key`
  (`source:id`) column and document it.
- **Force a Parquet rewrite when the converter changes.** `_sync_parquet`
  (`orchestration/resources/gcs.py`) skips when the content hash matches, so converter
  changes (T3) don't reach existing files. Include a `PARQUET_FORMAT_VERSION` in the hash
  stored on `latest.parquet`.
- **Data quality:** one timeseries observation is dated 2316-03-02. Find the source
  record, and decide whether to reject future dates at transform time.
- **Dagster+ memory:** converting the 733 MB timeseries peaked at ~3.6 GB RSS locally
  (JSON load + GeoDataFrame). Watch the first run. If it's too high, build the Parquet
  from the in-memory records instead of re-parsing the GeoJSON.
- **WQP source labels differ between products** (separate ticket). In
  `backend/connectors/wqp/transformer.py`, WQP site records are labeled `WQP/{provider}`
  (`WQP/NWIS`, `WQP/STORET`), but observation records (water levels, analytes) are
  labeled plain `WQP`. Products built from observations say `WQP`: the summaries, the
  timeseries, major chemistry, and MCL exceedance. Products built from sites say
  `WQP/NWIS`. The client's fallback copes, but for those wells the summary shows the
  USGS-NWIS feed's row. The recommended fix is to label observations `WQP/{provider}` too.
  That changes public `source` values in every WQP-backed product, on GeoServer as well.
- **CI:** `uv sync --extra dev` has no pyarrow, jinja2, or dagster, so the new Parquet,
  config, and GCS-sync tests skip in CI. Add `--extra parquet` and the needed packages to
  CI, or run those tests in `orchestration-ci.yml`.

### T7 — Trim response rows (optional, after T1–T5)

- Extend the T2 patch to drop the `bbox` covering column from properties. (Per-feature
  `bbox` is already gone.) Gzip already recovers most of these bytes,
  so do this only if measurements still show a gap.
- Don't drop the Z coordinate for size reasons; that's a data decision.

## Measurement protocol

Run it against the live service after T3 (with a build of that code) and after T5.

1. **Timing:** time each page of `items?id=9033&limit=5000&offset=0,5000,...` until a short
   page, with `Accept-Encoding: gzip`. Record the time per page, the rows per page, the
   bytes on the wire (`%{size_download}`), and the total.
2. **Correctness:** the union of the pages is exactly 48,809 distinct `feature_id`s.
3. **Baseline:** `limit=10` with no filter on 3 layers (the floor).
4. **All layers:** items, `bbox`, and item-by-id for all 31 collections return 200. The
   polygon layers return 5 for the standard bbox (`-107,32,-105,34`).
5. **Targets:** whole series ≤ 10 s (GeoServer parity); floor < 100 ms server time; no
   OOM at 2 GiB.

## Later

- **EDR for the timeseries.** A `BaseEDRProvider` subclass serving
  `/collections/nm_waterlevels_timeseries/locations/{source:id}` as a CoverageJSON
  `PointSeries`. It sits beside the Features provider on the same collection, and returns
  one request per well with no paging. Model it on OcotilloAPI's `core/edr_provider.py`
  (PostgreSQL-backed). The DIE version would read the baked Parquet with a pyarrow filter
  (fast after T3). Notes from that code: implement `instances()`/`instance()` exactly as
  pygeoapi calls them, and return fresh dicts from `get_fields()`. Per-observation fields
  such as `approval_status` and `qualifier` need to be separate parameters, or stay on
  the Features endpoint. Needs NewWeaver to read CoverageJSON.
- **PostGIS**, if a shared Postgres becomes available. It makes the Ocotillo EDR provider
  reusable nearly as-is, and gives SQL paging with true counts.
- **Upstream pygeoapi:** the OpenAPI contact address bug (`gen_contact` overwrites
  `administrativeArea` with postal code and country).
