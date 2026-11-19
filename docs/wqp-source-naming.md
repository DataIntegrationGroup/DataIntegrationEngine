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

## The id keeps the reporting agency

`WQPSiteTransformer` uses WQP's `MonitoringLocationIdentifier` as `id`, unchanged, and
`_feature_id` only puts `<source>:` in front of it. WQP builds that identifier from the
reporting organization's code plus the organization's own site code, and WQP's station
pages are filed under `/provider/<ProviderName>/<OrganizationIdentifier>/<id>`. New Mexico
examples:

| Provider | Organization | `MonitoringLocationIdentifier` | Feature id with `source = WQP` |
|---|---|---|---|
| NWIS | `USGS-NM` | [`USGS-323018103310702`](https://www.waterqualitydata.us/provider/NWIS/USGS-NM/USGS-323018103310702/) | `WQP:USGS-323018103310702` |
| STORET | `21NMEX` | [`21NMEX-42RGRAND004.1`](https://www.waterqualitydata.us/provider/STORET/21NMEX/21NMEX-42RGRAND004.1) | `WQP:21NMEX-42RGRAND004.1` |
| STORET | `21NMEX_WQX` | [`21NMEX_WQX-50SprValencRd`](https://www.waterqualitydata.us/provider/STORET/21NMEX_WQX/21NMEX_WQX-50SprValencRd/) | `WQP:21NMEX_WQX-50SprValencRd` |

So the reporting agency stays readable in the feature id if `source` drops the provider
(option A below). Two details:

- USGS stations are prefixed `USGS-`, not with their organization code `USGS-NM`. The
  prefix names the agency, not always the exact `OrganizationIdentifier`.
- The provider (`NWIS`/`STORET`) isn't in the id. In these examples it follows from the
  prefix (`USGS-` from NWIS, state and other agencies from STORET), but that is an
  observation, not a WQP rule.

The examples come from WQP's public station pages; the investigation couldn't query
`waterqualitydata.us` directly. Checking the WQP rows DIE actually ingests is part of
ticket 1 below.

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
  smaller providers such as `STEWARDS` and `BIODATA`), and the id keeps the reporting
  agency (see [The id keeps the reporting agency](#the-id-keeps-the-reporting-agency)).

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

## Proposed implementation tickets

General scope for the backlog; each needs refinement. Tickets 1 to 3 implement the
recommendation and should ship in one release, so the id change happens once.

1. **fix(wqp): write `source = "WQP"` for WQP sites.** `WQPSiteTransformer` writes
   `"source": "WQP"` and drops `ProviderName`; nothing reads it. Add a test with a NWIS and a
   STORET station. As part of this ticket, pull a sample of real WQP station rows and
   confirm every `MonitoringLocationIdentifier` starts with an agency prefix as described
   above.
2. **test(connectors): site and observation `source` must match.** For each source in
   `SOURCE_DICT`, assert the site transformer's `source` equals its parameter transformers'
   `source_tag`. This catches the next connector that drifts, not just WQP. (A stronger
   option, making `ParameterTransformer` take `source` from `site_record.source` and
   dropping `source_tag`, touches every connector and can be a separate refactor.)
3. **docs(products): update WQP source docs and release note.** Update the WQP note under
   "Sources" in `docs/products/README.md` and the header comment in
   `orchestration/config/sources.yaml`. The `<tag>/<anything>` prefix match in
   `generate_config.source_abbreviation` is generic and can stay. Release note: feature
   ids for WQP wells in the 17 site-derived layers change from `WQP/<provider>:<id>` to
   `WQP:<id>`.
4. **chore(correlation): compare well correlation before and after ticket 1.** Sites from
   the same agency are never linked spatially (`backend/well_correlation.py`). Today
   `WQP/NWIS` and `WQP/STORET` count as different agencies, so a NWIS-provided and a
   STORET-provided WQP well within 150 m can be linked; after ticket 1 they can't. The two
   providers publish different organizations' stations, so the change should be small.
   Diff `nm_well_correlation` output to confirm. (`_dedupe_well_points`, which keys on
   `(source, id)`, could also collapse a station returned by both providers; that is the
   correct result.)
5. **investigate: duplicate USGS wells from WQP and USGS-NWIS.** WQP's NWIS-provided
   stations are USGS wells that the `USGS-NWIS` connector also returns, with the same
   `USGS-<site_no>` id. They show up twice in combined layers under two `source` values.
   Decide whether to drop NWIS-provided rows from the WQP connector, dedupe in the
   combine step, or keep both and let well correlation link them.
6. **investigate: WQP legacy endpoints and recent USGS data.** The WQP connector calls the
   legacy `/data/Station/search` and `/data/Result/search` endpoints (WQX 2.2 profiles).
   WQP notes that those profiles don't contain USGS data added after March 11, 2024; newer
   USGS data is only in the WQX 3.0 profiles. Confirm whether this applies to DIE's
   queries and, if it does, how it interacts with ticket 5 (the `USGS-NWIS` connector may
   already cover the gap).
