# Agent guide — DIE orchestration

Dagster code location for the Data Integration Engine. Defines, schedules, and
publishes the DIE data products. This dir is a `dg` (Dagster CLI) project — see
`[tool.dg]` in `pyproject.toml`.

## Use `dg` for Dagster operations

Prefer the `dg` CLI over raw `dagster ...` or ad-hoc scripts for anything
Dagster-related (listing, running, validating, dev server). Run it from the
`orchestration/` directory so `uv` uses the venv that has dagster + dg:

```bash
uv run dg <command>          # from the orchestration/ directory
```

Note: the `dg` project root is the **repo root** (`[tool.dg]` in the top-level
`pyproject.toml`), because the `orchestration` package lives at repo-root level
(`./orchestration/`) — same place the Dagster+ serverless build imports it from.
`dg` discovers that project by walking up from `orchestration/`; just always run
via `uv run` from `orchestration/` and it resolves correctly.

Common operations:

| Task | Command |
|------|---------|
| Start the local dev UI | `uv run dg dev` |
| List assets / jobs / schedules / resources | `uv run dg list defs` |
| Materialize asset(s) | `uv run dg launch --assets <selection>` |
| Run a product job | `uv run dg launch --assets '*<product_id>/geoserver'` |
| Scaffold a new component/asset | `uv run dg scaffold ...` |

Asset-selection syntax is the standard Dagster one (`key`, `key*`, `*key`,
`group:<name>`). A product's full graph is `sources → <product_id> → geoserver`,
so `<product_id>/geoserver` plus upstream covers the whole product.

## Validating changes

Validate that all definitions load and component YAML is valid:

```bash
uv run dg check defs         # from orchestration/
```

`uv run dg list defs` also exercises loading and is a good quick check. The
plain import smoke test still works as a fallback:

```bash
uv run python -c "import orchestration.definitions; print('ok')"
```

## Architecture (so changes land in the right place)

- Products are declared in `config/products.yaml`. Each entry expands — via
  `assets/products.py:build_product_assets` — into a per-source asset graph:
  `sources/<key>` → combine (`<product_id>`) → `<product_id>/geoserver`.
- `definitions.py` wires assets, one asset job per product (`<product_id>_job`),
  schedules, and resources (`die_config`, `gcs`, `geoserver`, `io_manager`).
- Resources live in `resources/`. The backend DIE engine is the `nmuwd` package
  (repo root `backend/`); orchestration only drives it.
- Per-source and geoserver assets soft-fail: errors surface as red asset checks
  (WARN), not hard failures, so one dead source doesn't block a product.

## Deploy

Dagster+ serverless builds from the repo root per `dagster_cloud.yaml`
(`module_name: orchestration.definitions`). Runtime deps come from the root
`requirements.txt`. Secrets are set as Dagster+ env vars, not in code:

- `GCP_SERVICE_ACCOUNT_KEY` — GCS upload/IO-manager auth (JSON key).
- `GEOSERVER_URL` / `GEOSERVER_USER` / `GEOSERVER_PASSWORD` / `GEOSERVER_WORKSPACE`.
- `USGS_API_KEY` — USGS/NWIS API key. Without it the USGS water data API is
  heavily rate-limited. Resolved via `dg.EnvVar` into `DIEConfigResource`, which
  exports it to the environment for the NWIS connector.
- `DIE_FORWARD_LOGS_TO_DAGSTER` (optional) — forward DIE logs to the compute log.
