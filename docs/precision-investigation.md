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

**Where users saw it:** the attribute table of `nm_waterlevels_summary`, in
the calculated `min`, `mean` and `max` fields (confirmed in PR review).

### Why those three fields

- `mean` is `sum(values) / n` (`backend/source.py:136`). It has float noise
  or a cut-off repeating decimal for almost every well with more than one
  distinct value, regardless of source.
- `min` and `max` are picked from the converted values (`source.py:105`).
  Water levels are output in ft, so they are clean for sources that report
  ft (factor 1.0 returns the parsed value unchanged) and noisy for sources
  that report m (factor 3.28084), for example `12.345 m` becomes
  `40.501969800000005`.
- The CLI hid all three: it rounded `min`/`mean`/`max` to 2 decimals.

Which water-level sources report meters is decided by each provider's
metadata (`DepthToWaterBGSUnits` for NMBGMR, `unit_of_measure` for USGS, the
datastream `unitOfMeasurement` for the ST2 sources). The live payloads and the
published product could not be reached from the environment used for this
investigation, so the implementation ticket should confirm the list by
checking the published `min`/`max` per source.

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

### A. Source decimal places, shifted by the conversion factor, rounded at output (recommended, agreed in review)

Three parts:

**1. Count the source's decimal places at ingest.** `StandardUnitConverter.convert`
and `transform_length_units` are the last places that see the source value
before DIE does arithmetic on it. Count decimals there with
`-Decimal(str(value)).as_tuple().exponent` (floored at 0), which handles
`"12.30"` (2), `"1.2E-3"` (4) and `"1200"` (0). Two call sites call `float()`
before the converter (`transformer.py:281`, `source.py:105`); pass the raw
value through instead, since `convert` already coerces it. Where a source
sends JSON numbers rather than strings, trailing zeros are already gone
(`12.30` arrives as `12.3`), so the count can be one or more short. That
rounds slightly harder than the source intended, never softer.

**2. Shift the decimal count by the conversion factor's order of magnitude.**

```
decimals_out = max(0, decimals_in - floor(log10(factor)))
```

- Factor `1.0`: keeps the source's decimals (the value is unchanged anyway).
- Factors between 1 and 10 (m to ft 3.28084, as N 4.427, as CaCO3 1.22):
  keeps the source's decimals.
- Factors below 1 add decimals (ft to m 0.3048 adds 1, ug/L to mg/L 0.001
  adds 3), so small converted values are not rounded to zero.
- Factors of 10 or more remove decimals (ton/ac-ft 735.47 removes 2).

Plain decimal places without the shift fail on unit changes: `0.5 ug/L` is
`0.0005 mg/L`, which rounds to `0.0` at 1 decimal. Significant figures drop
a measured digit whenever the converted value crosses a power of ten
(`9.87 m` gives `32.4 ft`) and turn short whole numbers coarse (`37 m` gives
`120.0 ft`). The shifted rule avoids both.

**3. Carry the decimal count on the record and round once at output.** Store
it next to each value (for example `value_decimals`, `elevation_decimals`,
`well_depth_decimals`) and apply it in one helper when features are built in
`backend/persisters/ogc_features.py` (`_make_feature`,
`dump_timeseries_collection`, `_pivot_by_well`, the site-dict features). That
point sits before both the GeoJSON and GeoParquet writers. All arithmetic
(conversion, `min`/`max`/`mean`, trends, medians) runs on unrounded values,
so derived results are not computed from already-rounded inputs.

- Each observation, and `earliest_value`/`latest_value`, rounds to its own
  decimals.
- `min`, `max` and `mean` round to the decimals of the most precise input
  for that well. Inputs `12.1, 12.2, 12.4` give `12.233333333333334`,
  rounded to `12.2`.
- Derived products that already round their own computed fields (trends,
  change, hardness, and so on) are unchanged.
- Latitude/longitude are not rounded: DIE does no arithmetic on them, so they
  are already exactly what the source sent.
- CABQ stickup: subtract in feet after the elevation is in feet, so the
  elevation goes through one conversion.

Conversion factors are treated as exact. That is a policy choice, accepted in
review on the condition that it is documented (follow-up 2).

### B. Fixed per-field decimals (the CLI's old behavior, rejected)

Reinstate the CLI's table (`elevation`/`well_depth`/`min`/`max`/`mean`/water
level at 2 decimals). Smallest change, but it drops measured digits
(`45.123 ft` to `45.12`) and zeroes small converted analyte values
(`0.5 ug/L` to `0.0 mg/L`).

### C. Source significant figures (rejected in review)

Round each converted value to the source value's significant figures. Handles
unit changes, but drops a measured digit when the converted value crosses a
power of ten and coarsens short whole numbers (see the table below).

### D. Noise-only cleanup or `Decimal` arithmetic (not recommended)

Rounding to ~10 significant figures, or computing in `Decimal`, removes
`70.44800000000001` but leaves the false precision (`5381.561852`,
`2.96609`).

### Comparison

Computed with the current conversion code:

| Source value | Raw DIE output today | CLI (2 decimals) | Source sig figs (C) | Shifted decimals (A) |
|---|---|---|---|---|
| `45.123 ft` (no conversion) | `45.123` | `45.12` | `45.123` | `45.123` |
| `9.87 m` to ft | `32.3818908` | `32.38` | `32.4` | `32.38` |
| `3.05 m` to ft | `10.006561999999999` | `10.01` | `10.0` | `10.01` |
| `12.345 m` to ft | `40.501969800000005` | `40.5` | `40.502` | `40.502` |
| `37 m` to ft | `121.39108` | `121.39` | `120.0` | `121.0` |
| `1640.3 m` elevation to ft | `5381.561852` | `5381.56` | `5381.6` | `5381.6` |
| `45.12 ft` to m | `13.752576` | `13.75` | `13.75` | `13.753` |
| `0.5 ug/L` to mg/L | `0.0005` | `0.0` | `0.0005` | `0.0005` |
| `1.5 pCi/L` uranium to mg/L | `0.002235` | `0.0` | `0.0022` | `0.0022` |
| `0.67 mg/L as N` to mg/L | `2.96609` | `2.97` | `3.0` | `2.97` |
| mean of `12.1, 12.2, 12.4 ft` | `12.233333333333334` | `12.23` | | `12.2` |

The one place A reports more digits than the source measured is a factor
below 1: `45.12 ft` to `13.753 m` (0.001 m is finer than 0.01 ft). That was
judged acceptable because the rule never hides a measured digit.

## Proposed follow-up tickets

1. **Round outputs to shifted source decimal places.** Approach A: count
   decimals in `StandardUnitConverter.convert` / `transform_length_units`,
   carry them on records, round once in `ogc_features.py`. Regression tests on
   `dump_summary_collection` and `dump_timeseries_collection` using the
   comparison table above, plus scientific notation, `"1200"` and non-detects
   (`0.0`). Remove the unused `BaseRecord.to_row` / `to_dict` /
   `_get_sigfig_formatted_value` so there is one rounding path.
2. **Data services unit conversion precision doc.** Extend
   `UNIT_CONVERSIONS.md` (or a sibling doc) with the policy: conversion
   factors treated as exact, decimals taken from the source and shifted by
   the factor's order of magnitude, the `min`/`max`/`mean` rule, rounding
   at output. (Split requested in the ticket comments; a review condition for
   the policy.) Should land before or with 1.
3. **Keep raw source value strings where available** (WQP TSV, NMBGMR,
   NMED) so trailing zeros survive to the converter. Optional refinement of
   1; only worth it if under-counting decimals turns out to matter.
4. **CABQ stickup elevation double conversion.** Subtract the stickup in feet
   after the elevation is in feet. Small, independent cleanup; can fold into 1.

## Resolved in review

- **Where users saw it:** the `nm_waterlevels_summary` attribute table,
  `min`/`mean`/`max` (see Symptom).
- **Policy:** conversion factors are treated as exact. Accepted on the
  condition that it is documented (follow-up 2).
- **Rule:** source decimal places shifted by the conversion factor's order
  of magnitude (approach A), replacing an earlier source-significant-figures
  proposal and the CLI's fixed 2 decimals.
- **Where to round:** once, when output features are built, after all
  calculations.
- **Short whole numbers** (`37 m`): resolved by the shifted rule (`121.0 ft`).
