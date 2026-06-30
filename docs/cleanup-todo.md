# DIE Cleanup TODO

> **Status:** Tier 1 (all), plus 2.1 and 2.2, are **DONE** (this PR). Remaining:
> Tier 2 items 2.3–2.7, all of Tier 3, all of Tier 4.

Prioritized cleanup backlog from a code-analysis sweep (backend + frontend +
orchestration). Each item: location, effort (S/M/L), risk, and whether it
changes behavior. Items marked ✓ were confirmed against current `main`
(post #95 dual-fetch, #99 descriptions, #100 new products, #101/#102 config
registry). Tiers are ordered by safety — Tier 1 is batchable into one no-risk PR.

---

## Tier 1 — Dead / commented-out code (no behavior change, safe) — ✅ DONE

| # | Location | Action | Effort | ✓ |
|---|----------|--------|--------|---|
| 1.1 | `backend/unifier.py:84-113` | Delete commented `_perister_factory()` block (~30 lines) | S | ✓ |
| 1.2 | `backend/unifier.py:116-120` | Delete commented `_unify_wrapper()` | S | ✓ |
| 1.3 | `backend/config.py` `all_site_sources()` | Delete commented `pods` wiring (3 lines) | S | ✓ |
| 1.4 | `frontend/api/app.py:60-82` | Delete commented `create_queue()` (~23 lines) | S | ✓ |
| 1.5 | `frontend/api/app.py:101-103, 139-147` | Delete commented cache-check + alt Cloud Tasks payload | S | ✓ |
| 1.6 | `frontend/api/app.py:87` | Remove `print("unify waterlevels", item)` debug | S | ✓ |
| 1.7 | `frontend/api/app.py:114-119` | Remove dead `task_id is not None` path (never set) | S | ✓ |
| 1.8 | `frontend/api/app.py` `ConfigModel.sources` | Remove field — accepted on POST but never read | S | ✓ |
| 1.9 | `orchestration/definitions.py:211` | Drop `_all_specs` from `_build_graph` return + unpack — unused | S | ✓ |
| 1.10 | `orchestration/resources/die_config.py:44-56` | Drop `is_summary`/`output_format` mapping — **now dead**: source asset always passes synth `output_type="ogc_timeseries"` and `unify_source_both` ignores both. (Also resolves the missing `ogc_hardness`/`ogc_water_type` in the `is_summary` tuple — harmless today, but the whole block goes.) | S | ✓ |
| 1.11 | `backend/worker.py:91-97` | Delete commented `sources_in_polygon_handler()` | S | |
| 1.12 | `backend/record.py:52-55`, `frontend/cli.py:28`, `backend/logger.py:75` | Delete misc commented lines | S | |

---

## Tier 2 — Consistency / small tidy (low risk)

| # | Location | Action | Effort | Behavior | ✓ |
|---|----------|--------|--------|----------|---|
| **2.1 (requested) ✅** | `backend/config.py:79-97` + reads at `config.py`, `orchestration/assets/products.py:169`, 2 tests | **Flatten `PARAMETER_SOURCE_MAP`**: `param: {"agencies":[...]}` → `param: [...]`. `"agencies"` is the only key ever present; every read is `["agencies"]`. | S | none | ✓ |
| 2.2 ✅ | `backend/unifier.py` | `type(site_records) == list` → `isinstance(...)` | S | none | ✓ |
| 2.3 | `backend/config.py` (`parameter == "ph"`) | Use `PH` constant from `constants.py` | S | none | |
| 2.4 | `backend/transformer.py:272`, `backend/record.py:70` | `record_type == "analytes"/"waterlevels"` string compares → constants | S | none | |
| 2.5 | `frontend/api/app.py` | Hoist magic strings to constants: bucket `"die_cache"` (×3), queue `"die-queue"`, header casing | S | none | |
| 2.6 | `frontend/api/app.py` `router_parameters()` | Derive parameter list from `PARAMETER_SOURCE_MAP` instead of hardcoded 2-item list | M | none | |
| 2.7 | `orchestration/definitions.py:155,204` | Hoist default cron `"0 6 * * *"` + timezone `"America/Denver"` to constants | S | none | |

---

## Tier 3 — De-duplicate knowledge across layers (medium)

| # | Location | Action | Effort | ✓ |
|---|----------|--------|--------|---|
| 3.1 | `frontend/cli.py:36-121` (`--no-X` flags) + `cli.py:385-398` (hardcoded agency list in `sites()`) | Derive from `backend.config.SOURCE_KEYS` — the remaining hardcoded source list after #101/#102. Click flags are harder to generate dynamically; at minimum dedup the `sites()` list. | M | ✓ |
| 3.2 | `orchestration/definitions.py:60-68` (`_SUPPORTED_OUTPUT_TYPES`) vs `products.py` combine `if/elif` chain | Single registry mapping `output_type → (dumper, is_summary)`; both the supported-set and the dispatch derive from it. Adding an output type → one entry. | M | |
| 3.3 | `backend/config.py:79` `PARAMETER_SOURCE_MAP[WATERLEVELS]` | Derive the waterlevels agency list from the `SOURCES` registry (== sources with a waterlevel class; the desync test already asserts equality). Keep analyte entries authored. | S | ✓ |

---

## Tier 4 — Larger refactors (separate efforts, not quick wins)

- **`OutputMode` enum** — replace the 3 output bools (`output_summary` / `output_timeseries_unified` / `output_timeseries_separated`) + stringly-typed dispatch in `cli.py` and `die_config.py`, and make `output_format` an `OutputFormat` enum rather than a bare string. (Touches CLI/API/orchestration; the validation in #101 is a stopgap.)
- **Merge `read_summary` / `read_timeseries`** (`backend/source.py:375-433`) — ~85% identical; extract the shared fetch/clean/iterate skeleton.
- **`config.validate()` `sys.exit(2)` → raise** — library code shouldn't exit the process; raise a `ConfigError` and let CLI translate to an exit code. (Affects callers; needs care.)
- **Hoist connector `_extract_*` duplication** — `_extract_source_parameter_results/dates/units` repeat dict/list access across connectors; lift common shapes to a base.
- **Spatial-filter precedence** — `bbox_bounding_points` (bbox first) vs `bounding_wkt` (wkt first) resolve multiple filters differently; #101 warns, but unify the precedence. Consider a small `Scope` value object and drop the `wkt=None`-means-statewide magic in `die_config`.
- **Remove deprecated shims** — `transformer_klass`, `_SubclassValidatorShim`, `_validate_record` (`backend/source.py`). Verify no connector still relies on the override path first.

---

### Suggested execution order
1. Tier 1 as one PR (pure deletions; tests + `dg check` are the gate).
2. Tier 2 incl. the requested flatten (2.1) as one small PR.
3. Tier 3 piecemeal.
4. Tier 4 each as its own scoped PR.
