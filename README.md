# New Mexico Unified Water Data: Data Integration Engine
[![Format code](https://github.com/DataIntegrationGroup/DataIntegrationEngine/actions/workflows/format_code.yml/badge.svg?branch=main)](https://github.com/DataIntegrationGroup/DataIntegrationEngine/actions/workflows/format_code.yml)
[![CI/CD](https://github.com/DataIntegrationGroup/DataIntegrationEngine/actions/workflows/cicd.yml/badge.svg)](https://github.com/DataIntegrationGroup/DataIntegrationEngine/actions/workflows/cicd.yml)
[![Dependabot Updates](https://github.com/DataIntegrationGroup/DataIntegrationEngine/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/DataIntegrationGroup/DataIntegrationEngine/actions/workflows/dependabot/dependabot-updates)

![NMWDI](https://newmexicowaterdata.org/wp-content/uploads/2023/11/newmexicowaterdatalogoNov2023.png)
![NMBGMR](https://waterdata.nmt.edu/static/nmbgmr_logo_resized.png)

The Data Integration Engine (DIE) integrates New Mexico groundwater data — water
levels and water quality — from a dozen heterogeneous agency APIs into
standardized, geospatial data products.

It runs as a scheduled **Dagster** asset graph: each product is fetched from its
sources, harmonized (datum, units, datetimes), combined into an OGC GeoJSON
FeatureCollection, uploaded to GCS, and published as a GeoServer layer.

## Architecture

```
 fetch (dlt)              transform                 products                publish
 per-agency APIs   ──▶   datum / units /     ──▶   OGC FeatureCollections ──▶  GCS +
 retry + paginate        geo-filter / summarize    (GeoPandas)                 GeoServer
```

- **`backend/`** — the integration engine (`nmuwd` package)
  - `connectors/` — one connector per agency; HTTP via [dlt](https://dlthub.com)'s
    REST client (`connectors/_dlt.py`), SensorThings/FROST via
    `connectors/_sensorthings.py`
  - `transformer.py`, `converter.py`, `geo_utils.py` — harmonization (datum
    reprojection, unit conversion, datetime standardization, spatial filtering)
  - `unifier.py` — drives a source through fetch → transform for summary and/or
    timeseries output (`unify_source_both`, `unify_source_multi`)
  - `persisters/` — GeoPandas-backed serialization: OGC GeoJSON product dumpers
    (`ogc_features.py`), GeoDataFrame/GeoParquet/GeoPackage helpers
    (`geodataframe.py`)
- **`orchestration/`** — the Dagster code location: products are declared in
  `orchestration/config/products.yaml` and expand into a
  `sources → combine → geoserver` asset graph with schedules. See
  [orchestration/AGENTS.md](orchestration/AGENTS.md) for how to run, validate,
  and deploy it.

## Sources

Data comes from the following sources. We are continuously adding new sources as
we learn of them and they become available. If you have data that you would like
to be part of the Data Integration Engine please get in touch at
newmexicowaterdata@nmt.edu.

- [Bernalillo County (BernCo)](https://st2.newmexicowaterdata.org/FROST-Server/v1.1/Locations?$filter=properties/agency%20eq%20%27BernCo%27)
  - Available data: `water levels`
- [Bureau of Reclamation (BoR)](https://data.usbr.gov/)
  - Available data: `water quality`
- [City of Albuquerque (CABQ)](https://st2.newmexicowaterdata.org/FROST-Server/v1.1/Locations?$filter=properties/agency%20eq%20%27CABQ%27)
  - Available data: `water levels`
- [Elephant Butte Irrigation District (EBID)](https://st2.newmexicowaterdata.org/FROST-Server/v1.1/Locations?$filter=properties/agency%20eq%20%27EBID%27)
  - Available data: `water levels`
- [New Mexico Bureau of Geology and Mineral Resources (NMBGMR) Aquifer Mapping Program (AMP)](https://waterdata.nmt.edu/)
  - Available data: `water levels`, `water quality`
- [New Mexico Environment Department Drinking Water Bureau (NMED DWB)](https://nmenv.newmexicowaterdata.org/FROST-Server/v1.1/)
  - Available data: `water quality`
- [New Mexico Office of the State Engineer Points of Diversions (NMOSE PODs)](https://services2.arcgis.com/qXZbWTdPDbTjl7Dy/ArcGIS/rest/services/OSE_Points_of_Diversion/FeatureServer/0)
  - Available data: site/well information only
- [New Mexico Office of the State Engineer ISC Seven Rivers (NMOSE ISC Seven Rivers)](https://nmisc-wf.gladata.com/api/getMonitoringPoints.ashx)
  - Available data: `water levels`, `water quality`
- [New Mexico Office of the State Engineer Roswell District Office (NMOSE Roswell)](https://st2.newmexicowaterdata.org/FROST-Server/v1.1/Locations?$filter=properties/agency%20eq%20%27OSE-Roswell%27)
  - Available data: `water levels`
- [Pecos Valley Artesian Conservancy District (PVACD)](https://st2.newmexicowaterdata.org/FROST-Server/v1.1/Locations?$filter=properties/agency%20eq%20%27PVACD%27)
  - Available data: `water levels`
- [USGS (NWIS)](https://api.waterdata.usgs.gov/docs/)
  - Available data: `water levels`
  - **IMPORTANT:** the USGS water data API is heavily rate-limited without an
    API key. [Acquire one](https://api.waterdata.usgs.gov/signup/) and provide
    it via the `USGS_API_KEY` environment variable.
- [Water Quality Portal (WQP)](https://www.waterqualitydata.us/)
  - Available data: `water levels`, `water quality`

### Available Parameters

|                            | waterlevels | arsenic | bicarbonate | conductivity | calcium | carbonate | chloride | fluoride | magnesium | nitrate | ph  | potassium | silica | sodium | specific conductance | sulfate | tds | uranium |
| -------------------------- | ----------- | ------- | ----------- | ------------ | ------- | --------- | -------- | -------- | --------- | ------- | --- | --------- | ------ | ------ | -------------------- |-------- | --- | ------- |
| **bernco**                 | X           | -       | -           | -            | -       | -         | -        | -        | -         | -       | -   | -         | -      | -      | -                    | -       | -   | -       |
| **bor**                    | -           | X       | -           | X            | X       | -         | X        | X        | X         | X       | X   | X         | X      | X      | -                    | X       | X   | X       |
| **cabq**                   | X           | -       | -           | -            | -       | -         | -        | -        | -         | -       | -   | -         | -      | -      | -                    | -       | -   | -       |
| **ebid**                   | X           | -       | -           | -            | -       | -         | -        | -        | -         | -       | -   | -         | -      | -      | -                    | -       | -   | -       |
| **nmbgmr-amp**             | X           | X       | X           | -            | X       | X         | X        | X        | X         | X       | X   | X         | X      | X      | X                    | X       | X   | X       |
| **nmed-dwb**               | -           | X       | X           | -            | X       | -         | X        | X        | X         | X       | X   | X         | X      | X      | X                    | X       | X   | X       |
| **nmose-isc-seven-rivers** | X           | -       | X           | X            | X       | -         | X        | X        | X         | X       | X   | X         | X      | X      | X                    | X       | X   | -       |
| **nmose-pod**              | -           | -       | -           | -            | -       | -         | -        | -        | -         | -       | -   | -         | -      | -      | -                    | -       | -   | -       |
| **nmose-roswell**          | X           | -       | -           | -            | -       | -         | -        | -        | -         | -       | -   | -         | -      | -      | -                    | -       | -   | -       |
| **nwis**                   | X           | -       | -           | -            | -       | -         | -        | -        | -         | -       | -   | -         | -      | -      | -                    | -       | -   | -       |
| **pvacd**                  | X           | -       | -           | -            | -       | -         | -        | -        | -         | -       | -   | -         | -      | -      | -                    | -       | -   | -       |
| **wqp**                    | X           | X       | X           | X            | X       | X         | X        | X        | X         | X       | X   | X         | X      | X      | X                    | X       | X*  | X       |

<sup>*TDS data from WQP may contain duplicates. Duplicates are identified when they have the same ActivityIdentifier. If duplicates are identified, only one is kept as identified by its USGS pCode. The order of preference for the pCodes is: [70300](https://help.waterdata.usgs.gov/code/parameter_cd_nm_query?parm_nm_cd=70300&fmt=html), [70301](https://help.waterdata.usgs.gov/code/parameter_cd_nm_query?parm_nm_cd=70301&fmt=html), [70303](https://help.waterdata.usgs.gov/code/parameter_cd_nm_query?parm_nm_cd=70303&fmt=html).</sup>

<sup>**While conductivity and specific conductance are often used interchangeably, they are distinguished here by the methods with which they are determined. A record is defined as `specific conductance` if it was determined at the standard 25&deg;C (e.g. [EPA method 120.1](https://www.epa.gov/sites/default/files/2015-08/documents/method_120-1_1982.pdf)), otherwise it is defined as `conductivity`</sup>

## Data Products

Products are declared in
[orchestration/config/products.yaml](orchestration/config/products.yaml); each
becomes a scheduled Dagster job that publishes an OGC GeoJSON FeatureCollection
to GCS and a GeoServer layer. Product families include:

- **summaries** — per-well summary statistics for a parameter (see field
  reference below)
- **timeseries** — flat one-feature-per-observation collections
- **chemistry-derived** — major-ion chemistry, MCL exceedance, hardness,
  hydrochemical (Piper) water type, SAR, ion balance, CCME WQI
- **water-level analytics** — trends (Mann-Kendall/Theil-Sen), change, status,
  seasonal amplitude, depletion projection, monitoring recency, data density
- **infrastructure** — cross-agency well correlation, well density by
  county/basin, OSE POD age

## Running

Local development uses [uv](https://docs.astral.sh/uv/):

```bash
uv sync                      # install
uv run pytest                # unit tests (fast, no network)
```

The live per-connector integration harness (network, excluded from the default
run):

```bash
uv run pytest tests/test_sources --override-ini="norecursedirs="
```

Dagster (from `orchestration/`):

```bash
uv run dg dev                                    # local dev UI
uv run dg list defs                              # list assets/jobs/schedules
uv run dg launch --assets '*<product_id>/geoserver'   # run one product
```

See [orchestration/AGENTS.md](orchestration/AGENTS.md) for validation, deploy
(Dagster+ serverless), and the required environment variables
(`USGS_API_KEY`, `GCP_SERVICE_ACCOUNT_KEY`, `GEOSERVER_*`).

## Record Field Reference

The standardized record schemas that flow through the engine and into the
products:

### Summary

| field | description | data type | always present |
| :---- | :---------- | :-------- | :------------- |
| source | the organization/source for the site | string | Y |
| id | the id of the site. The id is used as the key to join the site and timeseries tables | string | Y |
| name | the colloquial name for the site | string | Y |
| usgs_site_id | USGS site id | string | N |
| alternate_site_id | alternate site id | string | N |
| latitude | latitude in decimal degrees | float | Y |
| longitude | longitude in decimal degrees | float | Y |
| horizontal_datum | horizontal datum of the latitude and longitude. Defaults to WGS84 | string | Y |
| elevation<sup>*</sup> | ground surface elevation of the site | float | Y |
| elevation_units | the units of the ground surface elevation. Defaults to ft | string | Y |
| well_depth | depth of well | float | N |
| well_depth_units | units of well depth. Defaults to ft | string | N |
| parameter_name | the name of the parameter whose measurements are reported | string | Y |
| parameter_units | units of the observation | string | Y |
| nrecords | number of records at the site for the parameter | integer | Y |
| min | the minimum observation | float | Y |
| max | the maximum observation | float | Y |
| mean | the mean value of the observations | float | Y |
| earliest_date | date of the earliest record in YYYY-MM-DD | string | Y |
| earliest_time | time of the earliest record in HH:MM:SS or HH:MM:SS.mmm | string | N |
| earliest_value | value of the earliest record | float | Y |
| earliest_units | units of the earliest record | string | Y |
| latest_date | date of the latest record in YYYY-MM-DD | string | Y |
| latest_time | time of the latest record in HH:MM:SS or HH:MM:SS.mmm | string | N |
| latest_value | value of the latest record | float | Y |
| latest_units | units of the latest record | string | Y |

<sup>*CABQ elevation is calculated as [elevation at top of casing] - [stickup height]; if stickup height < 0 the measuring point is assumed to be beneath the ground surface</sup>

### Site

| field | description | data type | always present |
| :---- | :---------- | :-------- | :------------- |
| source | the organization/source for the site | string | Y |
| id | the id of the site. The id is used as the key to join the site and timeseries tables | string | Y |
| name | the colloquial name for the site | string | Y |
| latitude | latitude in decimal degrees | float | Y |
| longitude | longitude in decimal degrees | float | Y |
| elevation<sup>*</sup> | ground surface elevation of the site | float | Y |
| elevation_units | the units of the ground surface elevation. Defaults to ft | string | Y |
| horizontal_datum | horizontal datum of the latitude and longitude. Defaults to WGS84 | string | Y |
| vertical_datum | vertical datum of the elevation | string | N |
| usgs_site_id | USGS site id | string | N |
| alternate_site_id | alternate site id | string | N |
| formation | geologic formation in which the well terminates | string | N |
| aquifer | aquifer from which the well draws water | string | N |
| well_depth | depth of well | float | N |
| well_depth_units | units of well depth. Defaults to ft | string | N |

<sup>*CABQ elevation is calculated as [elevation at top of casing] - [stickup height]; if stickup height < 0 the measuring point is assumed to be beneath the ground surface</sup>

### Time Series Observation

| field | description | data type | always present |
| :---- | :---------- | :-------- | :------------- |
| source | the organization/source for the site | string | Y |
| id | the id of the site. The id is used as the key to join the site and timeseries tables | string | Y |
| parameter_name | the name of the parameter whose measurements are reported | string | Y |
| parameter_value | value of the observation | float | Y |
| parameter_units | units of the observation | string | Y |
| date_measured | date of measurement in YYYY-MM-DD | string | Y |
| time_measured | time of measurement in HH:MM:SS or HH:MM:SS.mmm | string | N |
| source_parameter_name | the name of the parameter from the source | string | Y |
| source_parameter_units | the unit of measurement from the source | string | Y |
| conversion_factor | the factor applied to the result to convert the measurement to standardized units | float or int | Y |

Unit-conversion rules are documented in [UNIT_CONVERSIONS.md](UNIT_CONVERSIONS.md).
