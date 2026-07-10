# Removal Plan: retire the non-Dagster surface (CLI + FastAPI + Cloud-Tasks worker)

Premise (confirmed by owner): the CLI, the FastAPI trigger (`frontend/api`), and
the Flask Cloud-Tasks worker (`backend/worker.py`) are **dead — not called
anywhere**. The only live entry point is the Dagster asset graph
(`orchestration/`). This plan removes the dead surface and the backend output
machinery only it used.

Impact: **~1,250 lines deleted (~12% of the Python)** plus a real architectural
simplification — `Config`, `unifier`, and `Persister` each shed their entire
output/format/naming half, and three output paths collapse to one (Dagster).

## Inventory

### Delete outright
| Target | Lines | Why |
|--------|------:|-----|
| `frontend/cli.py` | 496 | the CLI |
| `frontend/api/app.py` (+ `__init__`) | 209 | FastAPI + Cloud-Tasks trigger |
| `backend/worker.py` | 116 | Flask Cloud-Tasks worker |
| `backend/persisters/strategies.py` | 57 | Local/GCS write strategies — only the dump path uses them |
| `frontend/cronjob_worker.sh`, `auto_worker_requirements.txt` | — | worker scaffolding |

### Gut (delete the CLI-only parts, keep the Dagster core)
| File | Delete | Keep |
|------|--------|------|
| `backend/unifier.py` | `unify_analytes`, `unify_waterlevels`, `unify_sites`, `_unify_parameter`, `health_check`, `get_sources`, `get_source_bounds`, `get_county_bounds`, `get_county_bounds` (~150 lines) | `_site_wrapper`, `unify_source`, `unify_source_both`, `unify_source_multi`, `collect_sites` |
| `backend/persister.py` | `dump_summary`/`dump_sites`/`dump_timeseries_unified`/`dump_timeseries_separated`, `_records_to_bytes`, `_timeseries_to_bytes`, `add_extension`, `_dump_*`, `_make_output_directory` (~120 lines) | the 3-list accumulator (`records`/`sites`/`timeseries`, `load`, `finalize` no-op) that the source assets read |
| `backend/persisters/factory.py` | the cloud/GCS branch | reduce `make_persister` to `BasePersister(config)` (~5 lines) — or inline it |
| `backend/config.py` | `finalize`, `update_output_name`, `make_output_directory`, `make_output_path`, `output_path`, `report`, `_warn_output_mode_exclusivity`, and the CLI-only knobs: `output_timeseries_unified`, `output_timeseries_separated`, `output_name`, `output_dir`, `use_cloud_storage`, `output_format` (audit — see below), `latest_water_level_only` (audit) (~90 lines) | inputs (`bbox`/`county`/`wkt`/dates/`parameter`/`sources`/`sites_only`), unit outputs, `validate`, `bounding_wkt`, `bbox_bounding_points`, source enumeration, and **`output_summary`** (see gotcha) |
| `backend/__init__.py` `OutputFormat` | `CSV`, `GEOSERVER` (already gone), `GEOJSON` if unused after (audit) | `OGC_SUMMARY`/`OGC_TIMESERIES` if still referenced |

### Deps / packaging
- Drop `click` (and `flask`/`fastapi`/`uvicorn` wherever the api pins them), the `die = "frontend.cli:cli"` script entry, and `packages = ["frontend", "backend"]` → `["backend"]`.

## Gotchas / audits before deleting
1. **`output_summary` STAYS.** `unify_source_both` toggles it and the transformer reads it live to pick `SummaryRecord` vs `ParameterRecord`. It is *not* a CLI-only flag — do not remove it with the other output knobs.
2. **`output_format` audit.** `die_config.get_config` sets it (`"ogc_summary"`/`output_type`), but the Dagster combine builds GeoJSON via `ogc_features` directly — confirm nothing in `orchestration/` reads `config.output_format` before deleting it. If unused, drop it and the `OutputFormat` CSV/GEOJSON members.
3. **`latest_water_level_only` audit.** Grep for readers; likely CLI-only, but confirm.
4. **`unify_source` (singular)** — orchestration uses `unify_source_both`, not `unify_source`. Confirm `unify_source` has no remaining caller (tests?) before deleting; likely removable.
5. **`config.report()` / logging setup** — the CLI wires `setup_logging`; Dagster uses `forward_die_logs`. Confirm no orchestration dependency on `report()`.
6. **Tests** — `tests/test_cli/` and any worker/api tests go; `tests/test_config_validation.py` and unit tests that touch removed Config methods need trimming.

## Ordered phases (each ends suite-green)
1. **Delete the entry points** — `frontend/`, `backend/worker.py`, their tests, the `die` script + web deps. Nothing else imports them, so the backend still imports and the suite still passes. (Biggest LOC drop, lowest risk.)
2. **Gut `unifier`** — remove the CLI unify wrappers + `_unify_parameter` + the `health_check`/`get_*` helpers. Grep-confirm zero references first.
3. **Collapse `Persister`** — delete the dump/byte methods, delete `strategies.py`, reduce `factory.make_persister` to `BasePersister(config)`. Source assets only read the 3 lists, so a smoke-materialize (or the existing unify tests) confirms parity.
4. **Slim `Config`** — remove the output machinery + CLI knobs (keeping `output_summary`). Update `die_config.get_config` if it sets any now-removed attr.
5. **Prune `OutputFormat` + deps** — after the audits.

Run the full suite after each phase; run one live `unify_source_both`/`unify_source_multi` (backend, verifiable) after phases 3–4 to confirm the Dagster-facing path is intact.

## Net
- ~1,250 lines removed; `Config` becomes an input-only query spec; `unifier` is fetch→transform only; `Persister` is a plain accumulator; one output path (Dagster) instead of three.
- Zero risk to the Dagster path if the audits (gotchas 1–5) are honored — the source assets touch only `unify_source_both`/`collect_sites`/`unify_source_multi` + the persister's 3 lists, none of which this plan removes.
