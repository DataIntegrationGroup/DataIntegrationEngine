# New Mexico Unified Water Data: Data Integration Engine
[![Format code](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/format_code.yml/badge.svg?branch=main)](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/format_code.yml)
[![Publish Python 🐍 distributions 📦 to PyPI and TestPyPI](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/publish-to-pypi.yml/badge.svg)](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/publish-to-pypi.yml)
[![CI/CD](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/cicd.yml/badge.svg)](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/cicd.yml)


![NMWDI](https://newmexicowaterdata.org/wp-content/uploads/2023/11/newmexicowaterdatalogoNov2023.png)
![NMBGMR](https://waterdata.nmt.edu/static/nmbgmr_logo_resized.png)


This package provides a command line interface to New Mexico Water Data Initiaive's Data Integration Engine. This tool is used to integrate the water data from multiple sources.

## Installation
```bash
pip install nmuwd
```

## Sources
Data comes from the following sources. We are continuously adding new sources as we learn of them and they become available. If you have data that you would like to be part of the Data Integration Engine please get in touch at newmexicowaterdata@nmt.edu.

- [Bernalillo County (BernCo)](https://st2.newmexicowaterdata.org/FROST-Server/v1.1/Locations?$filter=properties/agency%20eq%20%27BernCo%27)
  - Available data: `water levels`
- [Bureau of Reclamation (BoR)](https://data.usbr.gov/) 
  - Available data: `water quality`
- [New Mexico Bureau of Geology and Mineral Resources (NMBGMR) Aquifer Mapping Program (AMP)](https://waterdata.nmt.edu/)
  - Available data: `water levels`, `water quality`
- [New Mexico Environment Department Drinking Water Bureau (NMED DWB)](https://nmenv.newmexicowaterdata.org/FROST-Server/v1.1/)
  - Available data: `water quality`
- [New Mexico Office of the State Engineer ISC Seven Rivers (NMOSE ISC Seven Rivers)](https://nmisc-wf.gladata.com/api/getMonitoringPoints.ashx)
  - Available data: `water levels`, `water quality`
- [New Mexico Office of the State Engineer Roswell District Office (NMOSE Roswell)](https://catalog.newmexicowaterdata.org/dataset/pecos_region_manual_groundwater_levels)
  - Available data: `water levels`
- [Pecos Valley Artesian Conservancy District (PVACD)](https://st2.newmexicowaterdata.org/FROST-Server/v1.1/Locations?$filter=properties/agency%20eq%20%27PVACD%27)
  - Available data: `water levels`
- [USGS (NWIS)](https://waterdata.usgs.gov/nwis)
  - Available data: `water levels`
- [Water Quality Portal (WQP)](https://www.waterqualitydata.us/)
  - Available data: `water quality`

## Usage

### Parameter Data

To obtain parameter summary or time series data, use
```
die weave {parameter}
```

where `{parameter}` is the name of the parameter whose data is to be retrieved, followed by the desired output type, excluded data sources, date filters, and geographic filters. `{parameter}` is case-insensitive.


#### Available Parameters
The following parameters are currently available for retrieval:
- waterlevels
- arsenic
- bicarbonate
- calcium
- carbonate
- chloride
- magnesium
- nitrate
- ph
- potassium
- silica
- sodium
- sulfate
- tds
- uranium

### Output
The `--output` option is required and used to set the output type:

```
--output summary
```
- A summary table consisting of location information as well as summary statistics for the parameter of interest for every location that has observations.

```
--output timeseries_unified
```
- A single table consisting of time series data for all locations for the parameter of interest.
- A single table of site data that contains information such as latitude, longitude, and elevation

```
--output timeseries_separated
```
- Separate time series tables for all locations for the parameter of interest.
- A single table of site data that contains information such as latitude, longitude, and elevation

The data is saved to a directory titled `output` in the current working directory. If the directory `output` already exists, then the output directory will be called `output_1`. If enumerated output directories already exist, then the output directory will be called `output_{n}` where `n` is equal to the greatest existing integer suffix +1.

A log of the inputs and processes, called `die.log`, is also saved to the output directory.

#### Summary Table

| field/header | description | data type | always present |
| :----------- | :---------- | :-------- | :------------- |
| source | the organization/source for the site | string | Y |
| id | the id of the site. The id is used as the key to join the site and timeseries tables | string | Y |
| location | the colloquial name for the site | string | Y |
| usgs_site_id | USGS site id | string | N |
| alternate_site_id | alternate site id | string | N | 
| latitude | latitude in decimal degrees | float | Y |
| longitude | longitude in decimal degrees | float | Y |
| horizontal_datum | horizontal datum of the latitude and longitude. Defaults to WGS84 | string | Y |
| elevation | ground surface elevation of the site | float | Y |
| elevation_units | the units of the ground surface elevation. Defaults to ft | string | Y |
| well_depth | depth of well | float | N |
| well_depth_units | units of well depth. Defaults to ft | float | N |
| parameter | the name of the parameter whose measurements are reported in the table | string | Y |
| pramater_units | units of the observation | float | Y |
| nrecords | number of records at the site for the parameter | integer | Y |
| min | the minimum observation | float | Y |
| max | the maximum observation | float | Y |
| mean | the mean value of the observations | float | Y |
| most_recent_date| date of most recent record in YYYY-MM-DD | string | Y |
| most_recent_time | time of most recent record in HH:MM:SS or HH:MM:SS.mmm | string | N |
| most_recent_value | value of the most recent record  | float | Y |
| most_recent_units | units of the most recent record | string | Y |


#### Sites Table

| field/header | description | data type | always present |
| :----------- | :---------- | :-------- | :------------- |
| source | the organization/source for the site | string | Y |
| id | the id of the site. The id is used as the key to join the site and timeseries tables | string | Y |
| name | the colloquial name for the site | string | Y |
| latitude | latitude in decimal degrees | float | Y |
| longitude | longitude in decimal degrees | float | Y |
| elevation | ground surface elevation of the site | float | Y |
| elevation_units | the units of the ground surface elevation. Defaults to ft | string | Y |
| horizontal_datum | horizontal datum of the latitude and longitude. Defaults to WGS84 | string | Y |
| vertical_datum | vertical datum of the elevation | string | N |
| usgs_site_id | USGS site id | string | N |
| alternate_site_id | alternate site id | string | N | 
| formation | geologic formation in which the well terminates | string | N |
| aquifer | aquifer from which the well draws water | string | N |
| well_depth | depth of well | float | N |


#### Time Series Table(s)

| field/header | description | data type | always present |
| :----------- | :---------- | :-------- | :------------- |
| source | the organization/source for the site | string | Y |
| id | the id of the site. The id is used as the key to join the site and timeseries tables | string | Y |
| parameter | the name of the parameter whose measurements are reported in the table | string | Y |
| parameter_value | value of the observation | float | Y |
| pramater_units | units of the observation | float | Y |
| date_measured | date of measurement in YYYY-MM-DD | string | Y |
| time_measured | time of measurement in HH:MM:SS or HH:MM:SS.mmm | string | N |

### Source Inclusion & Exclusion
The Data Integration Engine enables the user to obtain groundwater level and groundwater quality data from a variety of sources. Data from sources are automatically included in the output if available unless specifically excluded. The following flags are available to exclude specific data sources:

- `--no-bernco` to exclude Bernalillo County (BernCo) data
- `--no-bor` to exclude Bureaof of Reclamation (Bor) data
- `--no-nmbgmr-amp` to exclude New Mexico Bureau of Geology and Mineral Resources (NMBGMR) Aquifer Mapping Program (AMP) data
- `--no-nmed-dwb` to exclude New Mexico Environment Department (NMED) Drinking Water Bureau (DWB) data
- `--no-nmose-isc-seven-rivers` to exclude New Mexico Office of State Engineer (NMOSE) Interstate Stream Commission (ISC) Seven Rivers data
- `--no-nmose-roswell` to exclude New Mexico Office of State Engineer (NMOSE) Roswell data
- `--no-nwis` to exclude USGS NWIS data
- `--no-pvacd` to exclude Pecos Valley Artesian Convservancy District (PVACD) data
- `--no-wqp` to exclude Water Quality Portal (WQP) data

### Geographic Filters

The following flags can be used to geographically filter data:

```
-- county {county name}
```

```
-- bbox 'x1 y1, x2 y2'
```

### Date Filters

The following flags can be used to filter by dates:

```
--start-date YYYY-MM-DD 
```

```
--end-date YYYY-MM-DD
```

### Source Enumeration [In Development]

Use

```
die sources {parameter}
```

to print the sources that report that parameter to the terminal.

### Wells [In Development]

Use

```
die wells
```

to print wells to the terminal.