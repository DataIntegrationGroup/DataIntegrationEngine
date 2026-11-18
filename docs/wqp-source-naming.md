# WQP `source` naming (DIE-377)

Investigation of why Water Quality Portal features carry different `source` values in
different product layers, and a recommendation for one naming scheme.

## Finding: confirmed

The ticket's description is correct. The WQP connector writes two different values:

| Transformer (`backend/connectors/wqp/transformer.py`) | `source` written |
|---|---|
| `WQPSiteTransformer` (site records) | `WQP/<ProviderName>`, e.g. `WQP/NWIS`, `WQP/STORET` |
| `WQPAnalyteTransformer` (chemistry observations) | `WQP` (`source_tag`) |
| `WQPWaterLevelTransformer` (water-level observations) | `WQP` (`source_tag`) |

`ParameterTransformer._transform` (`backend/transformer.py`) overwrites every observation
and summary record's `source` with the class's `source_tag`. It never reads
`site_record.source`. Running the three transformers on a stub NWIS-provided station gives
`WQP/NWIS` for the site and `WQP` for both observation records.

Every other connector uses the same string for both (`NMBGMR`, `USGS-NWIS`, `BOR-RISE`,
`ISCSevenRivers`, `CKAN/OSERoswell`, ...). WQP is the only one where they differ.

### Which layers get which value

Where a layer's `source` comes from depends on its writer in
`backend/persisters/ogc_features.py`:

| Value in layer | Writer reads `source` from | Writers (examples of layers) |
|---|---|---|
| `WQP` | the observation / summary record | `dump_summary_collection` (`nm_arsenic_summary`, `nm_waterlevels_summary`, ...), `dump_timeseries_collection`, `dump_major_chemistry_collection` (`nm_major_chemistry`), `dump_hardness_collection`, `dump_water_type_collection`, `dump_sar_collection`, `dump_ion_balance_collection`, `dump_mcl_exceedance_collection`, `dump_wqi_collection` |
| `WQP/<provider>` | the site record | `dump_trend_collection` (`nm_arsenic_trend`, `nm_waterlevel_trends`, windowed trends), `dump_monitoring_recency_collection`, `dump_data_density_collection`, `dump_waterlevel_change_collection`, `dump_waterlevel_status_collection`, `dump_seasonal_amplitude_collection`, `dump_depletion_projection_collection`, `dump_well_correlation_collection` |

### Effects

1. **Filtering.** `source=WQP` matches WQP wells in the summary and chemistry layers and
   none in the trend and site-derived layers. `source=WQP/NWIS` does the reverse.
2. **Feature ids.** `_feature_id` builds ids as `<source>:<id>`, so the same well is
   `WQP:USGS-324512106...` in `nm_arsenic_summary` and `WQP/NWIS:USGS-324512106...` in
   `nm_arsenic_trend`. Joining layers on feature id silently misses every WQP well.
   Timeseries ids (`<source>:<id>:<date>`) use `WQP`.
3. **Docs.** `docs/products/README.md` ("Sources") and `orchestration/config/sources.yaml`
   say WQP values look like `WQP/<provider>`. That's true for half the layers.
4. **No data loss.** Trend and other site-derived writers pair sites with observations
   by position (`zip(site_records, timeseries_records)`), not by `source`. No WQP wells
   are dropped. The problem is labels and ids only.
5. **Schema pages / abstracts.** `generate_config.source_abbreviation` maps both `WQP` and
   `WQP/<provider>` to the abbreviation `WQP`, so collection abstracts look the same. The
   schema `enum` for `source` lists the raw values, which is where DIE-317 made the split
   visible.

## Options

**A. `WQP` everywhere.** Change `WQPSiteTransformer` to write `WQP`.

- Matches the registry tag and the abbreviation, so an exact `source=WQP` filter works
  in every layer.
- Changes feature ids in the site-derived layers only (trend, recency, density,
  water-level change/status, seasonal amplitude, depletion, correlation: 17 layers in
  `products.yaml`). The summary, chemistry and timeseries layers (35 layers) keep their
  ids. No product excludes WQP, so every one of these layers can hold WQP wells.
- Drops the provider name from `source`. Little is lost: for wells the provider is
  normally `NWIS` or `STORET` (the EPA warehouse, not the reporting agency; WQP also has
  smaller providers such as `STEWARDS` and `BIODATA`), and the
  `MonitoringLocationIdentifier` already used as `id` carries the reporting organization
  as its prefix (`USGS-...`, `21NMEX-...`, ...). That prefix is more informative than
  `STORET`.

**B. `WQP/<provider>` everywhere.** Make the WQP parameter transformers take `source`
from `site_record.source`.

- Keeps the provider.
- Changes ids in the summary, chemistry and timeseries layers instead (35 layers, versus
  17 under A).
- `source=WQP` still matches nothing. The `?source=` query parameter is an exact match, so
  clients would have to list every `WQP/<provider>` value.

**C. `WQP` in `source`, provider in a new property** (e.g. `wqp_provider`).

- Clean filtering plus full provenance, but adds a column to every layer's schema and data
  dictionary for one source. More work than the ticket's problem needs.

## Recommendation

Use **A**: `source = "WQP"` in every layer.

Implementation, as a follow-up ticket:

1. `WQPSiteTransformer`: write `"source": "WQP"`. Leave `ProviderName` out of the record.
   Nothing reads it today.
2. Add a connector-wide guard test: for each source in `SOURCE_DICT`, the site
   transformer's `source` equals the parameter transformers' `source_tag`. This catches
   the next connector that drifts, not just WQP. A stronger variant is to make
   `ParameterTransformer` default to `site_record.source` and drop `source_tag`, but that
   touches every connector and should be its own change.
3. Update the WQP note under "Sources" in `docs/products/README.md` and the header
   comment in `orchestration/config/sources.yaml`. The `<tag>/<anything>` prefix match in
   `generate_config.source_abbreviation` is generic and can stay.
4. Release note: feature ids for WQP wells in the site-derived layers change from
   `WQP/<provider>:<id>` to `WQP:<id>`.

Side effects to check when implementing:

- **Well correlation.** Sites from the same agency are never linked spatially
  (`backend/well_correlation.py`). Today `WQP/NWIS` and `WQP/STORET` count as different
  agencies, so a NWIS-provided and a STORET-provided WQP well within 150 m can be linked.
  Under A they're one agency and won't be. Since the two providers publish different
  organizations' stations, the change is small. Compare correlation output before and
  after.
- **Point dedupe.** `_dedupe_well_points` keys on `(source, id)`. If the same
  `MonitoringLocationIdentifier` ever came back from both providers, A would collapse it
  to one point. That is the correct result.

## Out of scope, noted for follow-up

- WQP's NWIS-provided stations are USGS wells that the `USGS-NWIS` connector also
  returns, with the same `USGS-<site_no>` id. They show up twice in combined layers under
  two `source` values. Cross-source deduplication is a separate question from naming.
