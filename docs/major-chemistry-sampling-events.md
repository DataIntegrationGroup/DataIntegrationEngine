---
generated-by: Claude Code
generated-on: 2026-11-18
prompted-by: Jake Ross (DIE-404)
---

# Investigation: major-ion products mix sampling dates within a well (DIE-404)

Status: investigation and recommendation only. No product code changed yet.
Decisions needed before implementation are listed under "Open decisions".

## Finding

The ticket's claim is correct. Every product built on `_pivot_by_well` gets
each analyte's own latest value. The analytes on one well's feature can
therefore come from different sampling dates, and nothing checks or reports it.

### Why it happens

1. Each shared source asset produces one `SummaryRecord` per (well, analyte).
   `backend/source.py` (`summarize`, via `_summarize_records`) picks that analyte's own latest
   observation (`latest_value`, `latest_units`, `latest_date`), independent of
   the other analytes.
2. `_pivot_by_well` (`backend/persisters/ogc_features.py`) groups those records
   by `(source, id)` and stores `{value, units, date}` per analyte. It never
   compares the dates.
3. The products read from that map:

| Product (`output_type`) | Combines ions into one result? | Dates on the feature |
|---|---|---|
| `ogc_major_chemistry` | no, but shows them side by side | `<analyte>_date` per analyte |
| `ogc_mcl_exceedance` | no, one analyte at a time | `<analyte>_date` per analyte |
| `ogc_hardness` | yes (Ca + Mg) | `calcium_date`, `magnesium_date` |
| `ogc_water_type` (Piper) | yes (all 8 ions) | **none** |
| `ogc_sar` | yes (Na, Ca, Mg) | `sodium_date`, `calcium_date`, `magnesium_date` |
| `ogc_ion_balance` | yes (all 8 ions) | **none** |
| `ogc_wqi` | yes (score over the MCL set) | **none** |

Water type, ion balance and WQI give no dates at all, so a user cannot tell a
mixed-date result from a real one.

### Reproduction

One NMBGMR well with a complete 1985 analysis, and a 2019 resample of only
sodium and chloride. Built with the current dumpers:

```python
from backend.record import SummaryRecord
from backend.persisters.ogc_features import dump_ion_balance_collection

def rec(a, v, d):
    return SummaryRecord({"source": "NMBGMR", "id": "W-1", "name": "W-1",
        "latitude": 34.0, "longitude": -106.9, "parameter_name": a,
        "latest_value": v, "latest_units": "mg/L", "latest_date": d})

recs = [rec("calcium", 40, "1985-06-01"), rec("magnesium", 10, "1985-06-01"),
        rec("potassium", 2, "1985-06-01"), rec("bicarbonate", 150, "1985-06-01"),
        rec("sulfate", 48, "1985-06-01"),
        rec("sodium", 400, "2019-03-12"), rec("chloride", 60, "2019-03-12")]
dump_ion_balance_collection("/tmp/ib.json", recs, {"id": "x"})
```

Result: `charge_balance_pct = 59.5`, `balance_class = "suspect"`, with no date on
the feature. The well is flagged as a bad analysis only because sodium comes
from 2019 and the anions from 1985. The major chemistry feature for the same
well shows `calcium_date = 1985-06-01` next to `sodium_date = 2019-03-12`.

## What data is available for a fix

The fix does not need new fetching. Each shared source asset already ships
both summary records **and** the full per-observation timeseries
(`unify_source_both`, `orchestration/assets/products.py` `_source_asset`), and
the combine asset already loads both into `all_records` and `all_timeseries`.
Every observation is a `ParameterRecord` with `source`, `id`,
`parameter_name`, `parameter_value`, `parameter_units`, `date_measured` and
`time_measured`.

There is no sample or event ID shared across sources, so a **sampling event is
(source, well id, `date_measured`)**: the calendar date, ignoring time. Times
are not reliable across analytes (some sources stamp each analyte separately
or leave time blank).

The ticket asks for the fix in `_pivot_by_well` or the step before it. It
cannot go inside `_pivot_by_well` as written: that function only receives
the latest value per analyte, and the older values needed to rebuild an event
are already gone by then. The fix belongs one step earlier, building events from
`all_timeseries`.

## Recommendation

1. **Add an event-selection step** in `backend/persisters/ogc_features.py`,
   for example `_pivot_by_event(sites, timeseries, analytes, required)`. It
   returns the same well dict shape as `_pivot_by_well` (so the dumpers barely
   change), plus `sample_date` and `n_analytes_in_event`.
   - Group observations by (source, id, `date_measured`).
   - If one analyte has more than one numeric reading on the same day, use
     their mean (same rule as the analyte trend product's monthly mean).
   - Values and units come from the timeseries records, which went through the
     same transformer and unit conversion as the summary records.
2. **Event choice rule (proposed):** each product names a `required` analyte
   set. Pick the **most recent event that has every required analyte**. If no
   event has them all, pick the event with the most required analytes, newest
   first on ties, and set `event_complete = false`. This avoids a 1960 full
   analysis beating a 2020 analysis that lacks only carbonate. Carbonate is
   often unreported, or below detection at normal pH.
   Proposed required sets:
   - major chemistry, water type, ion balance: Ca, Mg, Na, Cl, SO4, HCO3
     (K and CO3 used when present, not required)
   - hardness: Ca, Mg
   - SAR: Na, Ca, Mg
   - WQI: at least one MCL analyte (see open decision 2)
3. **Feature schema:** replace the per-analyte `<analyte>_date` properties
   with one `sample_date`, plus `n_analytes_in_event` and `event_complete`.
   Add `sample_date` to water type, ion balance and WQI, which have no date
   today. A well whose events never share a date simply shows fewer analytes.
   It does not get values pulled from other dates.
4. **MCL exceedance: keep the latest value per analyte** (open decision 1).
   Each comparison is independent, and the useful question for a drinking-water
   screen is "is the most recent arsenic reading above the MCL". Forcing a
   shared event would hide a recent arsenic exceedance whenever the newest full
   suite is older. Keep the per-analyte `<analyte>_date` there and say in the
   product abstract that analytes may come from different dates.
5. **Wire it in the combine asset** for the six event-based output types: pass
   `all_sites` / `all_timeseries` to the dumpers instead of `SummaryRecord`s.
   Memory use stays the same because `all_timeseries` is already loaded there.
6. **Tests** in `tests/test_persisters/test_ogc_features.py`: the reproduction
   above (expects `balanced` from one event, not `suspect`), the
   incomplete-event fallback, same-day duplicates, and MCL still reporting
   per-analyte latest values.
7. **Docs and abstracts:** update the `products.yaml` abstracts ("latest value
   for each analyte" becomes "values from the most recent complete sampling
   event"), and note in `CHANGELOG.md` that the property names changed.

Suggested split into tickets, all in this repo:

1. Event selection helper + major chemistry, with tests.
2. Move hardness, water type, SAR, ion balance, WQI onto it.
3. MCL wording/abstract only (if decision 1 holds).

## Open decisions

1. **MCL exceedance:** keep per-analyte latest (recommended), or one event?
2. **Partial events:** accept the required sets above? For WQI, which
   analytes must be in the event, given that it scores against the MCL set
   (arsenic, nitrate, fluoride, uranium, sulfate, chloride, TDS), which are
   often sampled on different dates?
3. **Schema break:** removing `<analyte>_date` changes the published
   properties of `nm_major_chemistry`, `nm_hardness` and `nm_sar`. Is that
   acceptable for v1.1, or keep the per-analyte dates (all equal to
   `sample_date`) for one release?
4. **Event key:** is calendar date good enough, or does any source (NMBGMR
   or Ocotillo) expose a sample ID we should prefer when it is present?
