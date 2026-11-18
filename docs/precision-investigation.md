# Investigation: inconsistent significant figures in output products (DIE-403)

Status: findings only. No code changes in this branch; fixes are proposed as
follow-up tickets at the end.

## Symptom

Values in the published OGC products sometimes carry long trailing decimal
runs (`12.340000000000002`, `70.44800000000001`) or more digits than the
source reported (`5381.561852` ft for an elevation the source gave as
`1640.3` m). Precision is inconsistent across sources: some sources' values
come through clean, others don't. The reporter notes this started after the
CLI was retired.

## Root cause

There are two separate effects, and both come from the same gap: **nothing in
the Dagster output path rounds numeric values anymore.**

1. **Binary floating-point noise.** Multiplying or averaging decimal values
   in `float` produces results that are not exactly representable, and
   Python's shortest `repr` then shows the error:
   `176.12 * 0.4 = 70.44800000000001`.
2. **False precision.** Even when the product is exact, a 5-6
   significant-digit conversion factor applied to a 2-digit measurement
   yields more digits than were measured (`0.67 mg/L as N * 4.427 =
   2.96609`, `1640.3 m * 3.28084 = 5381.561852`). Means have the same
   problem in a different form: `(12.1 + 12.2 + 12.4) / 3 =
   12.233333333333334` is a repeating decimal cut off at float precision,
   not a conversion artifact.

### Why it showed up with the CLI retirement

The CLI wrote CSVs through `BaseRecord.to_row()`, which calls
`BaseRecord._get_sigfig_formatted_value()` (`backend/record.py:55`). That
method rounds a fixed set of fields:

| Field | Decimals |
|---|---|
| `elevation`, `well_depth` | 2 |
| `latitude`, `longitude` | 6 |
| `min`, `max`, `mean` (summary records) | 2 |
| `parameter_value` (water-level records only) | 2 |

The CLI's CSV path was deleted in `e836103` ("Remove dead CLI surface (phase
3)"). The Dagster path builds features in `backend/persisters/ogc_features.py`
with `getattr(record, k)` (`_make_feature`, `dump_timeseries_collection`,
`_pivot_by_well`), which reads the raw payload and skips
`_get_sigfig_formatted_value`. `to_row()` / `to_dict()` now have no callers.
The values then go through GeoPandas/GeoParquet (float64, lossless) and
`json.dump`, which prints the shortest round-tripping `repr`, so the noise
reaches pygeoapi unchanged.

Reproduced locally with the real `SummaryRecord` and `dump_summary_collection`:

```
CLI to_dict (old CSV path): {'elevation': 5381.56, 'latest_value': 2.96609, 'min': 2.97, 'mean': 12.23}
OGC geometry:               [-106.9, 34.1, 5381.561852]
OGC props:                  {'latest_value': 2.96609, 'min': 2.96609, 'mean': 12.233333333333334}
```

Note that the CLI never rounded analyte `latest_value` / `earliest_value` or
analyte `parameter_value`, so analyte false precision predates the retirement.
Only the fields in the table above regressed.

## Where the extra digits first appear

Values parsed from source payloads are clean as far as DIE is concerned:
`float("12.34")` round-trips exactly, and JSON numbers parse to the shortest
representation. (A provider could still send noisy numbers itself; that was
not checked against live payloads.) Digits are
added at three points:

| Stage | Location | What happens |
|---|---|---|
| Unit conversion (analytes, water levels) | `StandardUnitConverter.convert`, `backend/converter.py:125` | `input_value * conversion_factor`. Factor `1.0` is exact; every other factor introduces noise and/or false precision. |
| Length conversion (elevation, well depth) | `transform_length_units`, `backend/transformer.py:107-110` | `* 0.3048` or `* 3.28084` when source units differ from the output units (ft). |
| Source-specific arithmetic | `CABQSiteTransformer._transform_elevation`, `backend/connectors/st2/transformer.py:66` | stickup height converted ft to m, subtracted from elevation (m), then converted back to ft by the transformer. Two conversions plus a subtraction. |
| Aggregation | `backend/source.py:134-136` | summary `mean = sum(values) / n`, an unbounded decimal for most inputs. `min`/`max` are computed from already-converted values (`source.py:105`), so they carry the conversion artifacts but add none. |

Serialization (`_dump_collection`, `json.dump`) does not add digits; it just
stops hiding them.

Derived products are mostly fine: trend, change, status, seasonal amplitude,
depletion, hardness, water type, SAR, ion balance, data density and well
density already round explicitly in `ogc_features.py`.

### How often each conversion produces float noise

Measured on 10,000 random 2-decimal source values (noise = `repr(x) !=
repr(round(x, 10))`, which isolates binary noise from legitimately long
decimals):

| Conversion | Factor | Values with float noise |
|---|---|---|
| m to ft | 3.28084 | 25% |
| ft to m | 0.3048 | 36% |
| mg/L as N to mg/L (nitrate) | 4.427 | 57% |
| ug/L to mg/L | 0.001 | 27% |
| mg/L as CaCO3 to mg/L (bicarbonate / calcium / carbonate) | 1.22 / 0.4 / 0.6 | 28% / 39% / 30% |
| ton/ac-ft to mg/L | 735.47 | 24% |
| pCi/L to mg/L (uranium) | 0.00149 | 25% |
| any value with factor 1.0 | 1.0 | 0% |

Every non-identity conversion also produces false precision, whether or not it
produces noise, and most means have unbounded decimals.

## Affected sources and parameters

Determined from code (units each connector reports). Live unit mixes for the
SensorThings and WQP sources depend on the provider's datastream metadata, so
those rows say "when".

| Source | Field / parameter | Why |
|---|---|---|
| NMBGMR (AMP) | site `elevation` (all sites) | reported in m, converted to ft (`connectors/nmbgmr/transformer.py:33`) |
| ST2 sources (EBID, CABQ, PVACD, BernCo, NMOSE Roswell) | site `elevation` (all sites) | SensorThings elevation is m (`connectors/st_connector.py:188`), converted to ft |
| ST2 / CABQ | site `elevation` | stickup double conversion (above) |
| ST2 sources | water-level `parameter_value` | when the datastream unit is not ft |
| WQP | `elevation`, `well_depth` | when `MeasureUnitCode` is m |
| WQP, NMED DWB (nmenv), NMBGMR, BoR | analyte values | when reported in ug/L, mg/L as N, mg/L as CaCO3, ton/ac-ft or pCi/L |
| All sources | nitrate | reported "as N" (any source parameter named "nitrate as n") is multiplied by 4.427 |
| All sources | summary `mean` | aggregation |
| All sources | summary `min` / `max` / `latest_value` / `earliest_value` | inherit any conversion noise from the underlying values |
| USGS (NWIS), NMOSE, ISC Seven Rivers, BoR, CKAN | site `elevation` / `well_depth`; water levels in ft | **not affected**: already in ft, no arithmetic |

Affected products are therefore the 14 `ogc_summary` products (water levels
and each analyte), the 14 `ogc_timeseries` products, `nm_major_chemistry`, the MCL exceedance product (raw latest values
are copied through), and the `z` coordinate of every product's geometry for
NMBGMR and ST2 sites.

## Precision metadata available today

- No connector captures a source-reported precision, detection limit
  precision, or significant-figure count. The only precision signal is the
  textual value itself, and most connectors call `float()` on it immediately
  (`usgs/source.py:271,280`, `ckan/source.py:150`, `converter._coerce_value`).
- WQP returns `ResultMeasureValue` as text (TSV service), so the
  reported digits are available at ingest, but only the value is kept.
- The only written conversion reference is `UNIT_CONVERSIONS.md`, which lists
  factors but no precision policy.

## Recommended approaches

Whatever the approach, round **when values are produced** (in the converter,
the length transform and the summary aggregation), not as string formatting
at serialization. That keeps GeoParquet, GeoJSON and pygeoapi consistent,
keeps values numeric, and means derived products compute on the same values
users see.

### A. Significant figures from the raw source value (recommended)

`StandardUnitConverter.convert` and `transform_length_units` both receive the
source value before DIE has done any arithmetic on it: the raw string for
summary earliest/latest values (`transformer.py:512`), and
`float(source_result)` for timeseries and summary min/max
(`transformer.py:281`, `source.py:105`), whose shortest `repr` carries the
same digits minus trailing zeros. So the fix can live in those two
functions with no connector changes:

1. Count significant figures of the input (`"0.67"` gives 2, `1640.3` gives 5,
   `"12.30"` gives 4 when the string is available, 3 after `float()`).
2. Treat conversion factors as exact and round the converted value to the
   input's significant figures: `float(f"{v:.{n}g}")`. `1640.3 m` gives
   `5381.6 ft`, `0.67 mg/L as N` gives `3.0 mg/L`, `0.5 ug/L` gives
   `0.0005 mg/L`.
3. Factor `1.0`: return the input unchanged (already clean).
4. Mean: round to the decimal places of the most precise input (or that plus
   one). `12.233333333333334` becomes `12.23`.
5. CABQ stickup: subtract in feet after the elevation is in feet.

- Pros: one central change; output precision matches what the source
  reported; handles low concentrations correctly (fixed decimals would turn
  `0.0005 mg/L` into `0.0`); removes all binary noise as a side effect.
- Cons: trailing zeros lost to `float()` or JSON numbers undercount precision
  by one or more digits (`12.30` is treated as 3 sig figs); this rounds
  slightly harder than the source intended, never softer. Treating factors
  like 0.4 or 1.22 as exact is a policy choice that needs to be written down
  (follow-up 2). Edge cases to cover in tests: scientific notation
  (`"1.2E-3"`), integers with trailing zeros (`"1200"`), non-detects (`0.0`).

### B. Fixed per-field decimals (the CLI's old behavior)

Reinstate the CLI's table (`elevation`/`well_depth`/`min`/`max`/`mean`/water
level at 2 decimals) for the Dagster path.

- Pros: smallest change; exactly restores pre-retirement output for the
  fields the CLI covered.
- Cons: wrong for analytes: decimals depend on the concentration scale, and
  converting ug/L to mg/L moves the decimal point by 3. Would need a
  per-parameter decimals table that still fails for wide-range analytes.
  Acceptable for length fields in feet (ft to 0.01), where it agrees with A
  for typical inputs.

### C. Noise-only cleanup (not recommended on its own)

Round every converted value to ~10 significant figures. Removes
`70.44800000000001` but leaves `5381.561852` and `2.96609`.

### D. `Decimal` arithmetic in the converter (not recommended)

Removes binary noise but not false precision, and is slower across millions
of observations.

**Recommendation:** approach A, with B's fixed 2 decimals kept as an optional
cap for the length fields (elevation, well depth, depth to water in feet) if
the team wants output that exactly matches the old CLI. Do not round
latitude/longitude: DIE does no arithmetic on them, so they are already
exactly what the source sent.

## Proposed follow-up tickets

1. **Round converted and aggregated values to source significant figures.**
   Approach A in `StandardUnitConverter.convert`, `transform_length_units`
   and the summary mean; regression tests on `dump_summary_collection` and
   `dump_timeseries_collection` asserting no float noise or false precision
   for converted values. Remove the unused `BaseRecord.to_row` / `to_dict` /
   `_get_sigfig_formatted_value` so there is one rounding path.
2. **Data services unit conversion significant-figure doc.** Extend
   `UNIT_CONVERSIONS.md` (or a sibling doc) with the precision policy:
   factors treated as exact, sig figs taken from the source, mean rule.
   (Split requested in the ticket comments.) Should land before or with 1.
3. **Keep raw source value strings where available** (WQP TSV, NMBGMR,
   NMED) so trailing zeros survive to the converter. Optional refinement of
   1; only worth it if under-counting precision turns out to matter.
4. **CABQ stickup elevation double conversion.** Subtract the stickup in feet
   after the elevation is in feet. Small, independent cleanup; can fold into 1.

## Open questions

- The ticket comments ask for the user feedback showing where this was seen.
  It was not in the ticket; the findings above are from code and a local
  reproduction. Adding the reported product/feature would confirm which row
  of the "affected" table the user hit.
- Does the team accept "conversion factors are exact, precision comes from
  the source value" as the policy (follow-up 2)? Follow-up 1 depends on it.
- Should output exactly match the old CLI for length fields (2 decimals), or
  is source significant figures enough everywhere?
