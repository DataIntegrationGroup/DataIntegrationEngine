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
   `176.12 * 0.4 = 70.44800000000001`, `(12.1 + 12.2 + 12.4) / 3 =
   12.233333333333334`.
2. **False precision from unit conversion.** Even when the product is exact,
   a 5-6 significant-digit conversion factor applied to a 2-digit
   measurement yields more digits than were measured:
   `0.67 mg/L as N * 4.427 = 2.96609`, `1640.3 m * 3.28084 = 5381.561852`.

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

Values parsed from source payloads are clean: `float("12.34")` round-trips
exactly, and JSON numbers parse to the shortest representation. Digits are
added at three points:

| Stage | Location | What happens |
|---|---|---|
| Unit conversion (analytes, water levels) | `StandardUnitConverter.convert`, `backend/converter.py:125` | `input_value * conversion_factor`. Factor `1.0` is exact; every other factor introduces noise and/or false precision. |
| Length conversion (elevation, well depth) | `transform_length_units`, `backend/transformer.py:107-110` | `* 0.3048` or `* 3.28084` when source units differ from the output units (ft). |
| Source-specific arithmetic | `CABQSiteTransformer._transform_elevation`, `backend/connectors/st2/transformer.py:66` | stickup height converted ft to m, subtracted from elevation (m), then converted back to ft by the transformer. Two conversions plus a subtraction. |
| Aggregation | `backend/source.py:134-136` | summary `mean = sum(values) / n`. Noise on most non-trivial means. |

Serialization (`_dump_collection`, `json.dump`) does not add digits; it just
stops hiding them.

Derived products are mostly fine: trend, change, status, seasonal amplitude,
depletion, hardness, water type, SAR, ion balance, data density and well
density already round explicitly in `ogc_features.py`.

### How often each conversion produces float noise

Measured on 10,000 random 2-decimal source values (noise = `repr(x) !=
repr(round(x, 10))`):

| Conversion | Factor | Values with float noise |
|---|---|---|
| m to ft | 3.28084 | 25% |
| ft to m | 0.3048 | 36% |
| mg/L as N to mg/L (nitrate) | 4.427 | 57% |
| ug/L to mg/L | 0.001 | 27% |
| mg/L as CaCO3 to mg/L (bicarbonate / calcium / carbonate) | 1.22 / 0.4 / 0.6 | 28% / 39% / 30% |
| ton/ac-ft to mg/L | 735.47 | 24% |
| pCi/L to mg/L (uranium) | 0.00149 | 25% |
| mean of 3 one-decimal values | | 79% |
| any value with factor 1.0 | 1.0 | 0% |

Every non-identity conversion also produces false precision, whether or not it
produces noise.

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

### A. Restore a per-field rounding policy at the data level (recommended first step)

Apply rounding when values are produced (after conversion in the transformer
and after aggregation in `source.py`), not as string formatting at
serialization. That keeps Parquet, GeoJSON and pygeoapi consistent and keeps
values numeric. Start from the CLI's table, extend it to analyte
`parameter_value` / `latest_value` / `earliest_value` and the geometry `z`,
and keep it in one place (one mapping in `backend/constants.py` or
`record.py`) so products and docs share it.

- Pros: small change, restores pre-retirement behavior, removes all float
  noise, easy to test.
- Cons: fixed decimals, not significant figures. A source that reports
  `0.0032 mg/L` arsenic would be wrong at 2 decimals, so analytes need
  per-parameter decimals (or a significant-figure rule) rather than one
  number.

### B. Source-precision-aware significant figures (fuller fix)

Count the significant figures of the raw source value at ingest (from the
string before `float()`), carry it on the record, and round the converted
value to that many significant figures (`float(f"{v:.{n}g}")`). Means use
the minimum precision of their inputs plus one digit, or a fixed policy.

- Pros: output matches what the source measured; handles mixed-precision
  sources and tiny concentrations correctly.
- Cons: touches every connector's value extraction; JSON sources that send
  numbers (not strings) lose trailing zeros (`12.30` arrives as `12.3`), so
  the counted precision is a lower bound; needs a documented policy for
  conversion factors with fewer significant figures than the data (0.4, 0.6,
  1.22).

### C. Noise-only cleanup (not recommended on its own)

Round every converted value to ~10 significant figures. Removes
`12.340000000000002` but leaves `5381.561852`. Useful only as a guard
inside A or B.

### D. `Decimal` arithmetic in the converter (not recommended)

Removes binary noise but not false precision, and is slower across millions
of observations. A or B gives the same benefit with less churn.

**Recommendation:** do A now with per-parameter decimals for analytes, then
B where precision matters (analyte concentrations near MCLs), alongside a
written sig-fig policy.

## Proposed follow-up tickets

1. **Round Dagster-path outputs with a shared per-field precision policy.**
   Apply after conversion/aggregation (approach A); cover elevation,
   well_depth, geometry z, water-level values, summary min/max/mean,
   analyte latest/earliest/parameter values. Remove or reuse the unused
   `BaseRecord.to_row` / `to_dict` / `_get_sigfig_formatted_value`.
   Regression tests on `dump_summary_collection` and
   `dump_timeseries_collection` asserting no float noise for converted values.
2. **Data services unit conversion significant-figure doc.** Extend
   `UNIT_CONVERSIONS.md` (or a sibling doc) with the precision policy per
   unit and parameter, including how conversion-factor precision is handled.
   (Split requested in the ticket comments.)
3. **Capture source precision at ingest (approach B).** Keep the raw value
   string or its significant-figure count on observation records; use it in
   conversion rounding.
4. **CABQ stickup elevation double conversion.** Subtract the stickup in feet
   after the elevation is in feet instead of converting ft to m and back.
   Small, independent correctness cleanup.

## Open questions

- The ticket comments ask for the user feedback showing where this was seen.
  It was not in the ticket; the findings above are from code and a local
  reproduction. Adding the reported product/feature would confirm which row
  of the "affected" table the user hit.
- What precision should analyte concentrations keep? This determines whether
  ticket 1 alone is enough or ticket 3 is needed before the fix ships.
