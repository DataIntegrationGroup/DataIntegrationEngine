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
- [Bureau of Reclamation (BoR)](https://data.usbr.gov/) 
- [New Mexico Bureau of Geology and Mineral Resources (AMP)](https://waterdata.nmt.edu/)
- [New Mexico Environment Department Drinking Water Bureau (DWB)](https://nmenv.newmexicowaterdata.org/FROST-Server/v1.1/)
- [New Mexico Office of the State Engineer ISC Seven Rivers (ISC Seven Rivers)](https://nmisc-wf.gladata.com/api/getMonitoringPoints.ashx)
- [New Mexico Office of the State Engineer Roswell District Office (OSE Roswell)](https://catalog.newmexicowaterdata.org/dataset/pecos_region_manual_groundwater_levels)
- [Pecos Valley Artesian Conservancy District (PVACD)](https://st2.newmexicowaterdata.org/FROST-Server/v1.1/Locations?$filter=properties/agency%20eq%20%27PVACD%27)
- [USGS (NWIS)](https://waterdata.usgs.gov/nwis)
- [Water Quality Portal (WQP)](https://www.waterqualitydata.us/)
  - USGS
  - EPA
  - and over 400 state, federal, tribal, and local agencies


### Source Inclusion & Exclusion
The Data Integration Engine enables the user to obtain groundwater level and groundwater quality data from a variety of sources. Data from sources are included in the output unless specifically excluded. The following flags are available to exclude a specific data source:

- `--no-amp` to exclude New Mexico Bureau of Geology and Mineral Resources Aquifer Mapping Program (AMP) data
- `--no-bor` to exclude Bureaof of Reclamation data
- `--no-nwis` to exclude USGS NWIS data
- `--no-pvacd` to exclude Pecos Valley Artesian Convservancy District (PVACD) data
- `--no-isc-seven-rivers` to exclude Interstate Stream Commission (ISC) Seven Rivers data
- `--no-wqp` to exclude Water Quality Portal (WQP) data
- `--no-ckan` to exclude NM OSE Roswell data that is hosted on CKAN
- `--no-dwb` to exclude New Mexico Environment Department Drinking Water Bureau (DWB) data
- `--no-bernco` to exclude Bernalillo County (BernCo) data

### Water Levels

To obtain groundwater levels, use 

```
weave waterlevels
```

followed by the desired output type, source filters, date filters, geographic filters, and excluded data sources.

#### Available Data Sources
The following data sources are available for groundwater levels:

- amp
- bor
- ckan
- dwb
- isc-seven-rivers
- nwis
- pvacd
- bernco

### Water Quality
To obtain groundwater quality, use

```
weave analytes {analyte}
```

where `{analyte}` is the name of the analyte whose data is to be retrieved.

#### Available Analytes
The following analytes are currently available for retrieval:
- Arsenic
- Bicarbonate
- Calcium
- Carbonate
- Chloride
- Magnesium
- Nitrate
- pH
- Potassium
- Silica
- Sodium
- Sulfate
- TDS
- Uranium

#### Available Data Sources
The follow data sources are available for analytes, though not every source has measurements for every analyte:
- bor
- wqp
- isc-seven-rivers
- amp
- dwb

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

## Output
The data is saved to the current working directory. A log of the inputs and processes, called `die.log`, is also saved to the current working directory. If a subsquent process is run and the log from the previous process has not been moved or stored elsewhere, the log for the subsequent process will be appended to the existing log.

### Timeseries Data
The flag `--separated_timeseries` exports timeseries for every location in their own file in the directory output_series (e.g. `AB-0002.csv`, `AB-0003.csv`). Locations with only one observation are gathered and exported to the file `output.combined.csv`.

The flag `--unified_timeseries` exports all timeseries for all locations in one file titled `output.timeseries.csv`. It also exports a file titled `output.sites.csv` that contains site information, such as latitude, longitude, and elevation.

#### Table Headers: Unified

The table headers for unified timeseries data are as follows:

**output.sites.csv**
- `source`: the organization/source for the site
- `id`: the id of the site. The id is used as the key to join the output.timeseries.csv table
- `name`: the colloquial name for the site if it exists
- `latitude`: latitude in decimal degrees
- `longitude`: the longitude in decimal degrees
- `elevation` ground surface elevation of the site in feet
- `elevation_units`: the units of the ground surface elevation. Defaults to ft
- `horizontal_datum`: horizontal datum of the latitude and longitude. Defaults to WGS84
- `vertical_datum`: the vertical datum of the elevation
- `usgs_site_id`: USGS site id if it exists
- `alternate_site_id`: alternate site id if it exists
- `formation`: geologic formation in which the well terminates if it exists
- `aquifer`: aquifer from which the well draws water if it exists
- `well_depth`: depth of well if it exists

**output.timeseries.csv - waterlevels**
- `source`: the organization/sources for the site
- `id`: the id of the site. The id is used as the key to join the output.sites.csv table
- `depth_to_water_ft_below_ground_surface`: depth to water below ground surface in ft
- `date_measured`: date of measurement in YYYY-MM-DD format
- `time_measured`: time of measurement if it exists

**output.timeseries.csv - analytes**
- `source`: the organization/sources for the site
- `id`: the id of the site. The id is used as the key to join the output.sites.csv table
- `parameter`: the name of the analyte whose measurements are reported in the table. This corresponds the requested analyte
- `parameter_value`: value of the measurement
- `parameter_units`: units of the measurement
- `date_measured`: date of measurement in YYYY-MM-DD format
- `time_measured`: time of measurement if it exists

#### Table Headers: Separated

The files for the individual sites contain the same headers as **output.timeseries.csv** from the unified time series tables.

**output.combined.csv - waterlevels**
- `source`: the organization/source for the site
- `id`: the id of the site. The id is used as the key to join the output.timeseries.csv table
- `name`: the colloquial name for the site if it exists
- `latitude`: latitude in decimal degrees
- `longitude`: the longitude in decimal degrees
- `elevation` ground surface elevation of the site in feet
- `elevation_units`: the units of the ground surface elevation. Defaults to ft
- `horizontal_datum`: horizontal datum of the latitude and longitude. Defaults to WGS84
- `vertical_datum`: the vertical datum of the elevation
- `usgs_site_id`: USGS site id if it exists
- `alternate_site_id`: alternate site id if it exists
- `formation`: geologic formation in which the well terminates if it exists
- `aquifer`: aquifer from which the well draws water if it exists
- `well_depth`: depth of well if it exists
- `depth_to_water_ft_below_ground_surface`: depth to water below ground surface in ft
- `date_measured`: date of measurement in YYYY-MM-DD format
- `time_measured`: time of measurement if it exists

**output.combined.csv - analytes**
- `source`: the organization/source for the site
- `id`: the id of the site. The id is used as the key to join the output.timeseries.csv table
- `name`: the colloquial name for the site if it exists
- `latitude`: latitude in decimal degrees
- `longitude`: the longitude in decimal degrees
- `elevation` ground surface elevation of the site in feet
- `elevation_units`: the units of the ground surface elevation. Defaults to ft
- `horizontal_datum`: horizontal datum of the latitude and longitude. Defaults to WGS84
- `vertical_datum`: the vertical datum of the elevation
- `usgs_site_id`: USGS site id if it exists
- `alternate_site_id`: alternate site id if it exists
- `formation`: geologic formation in which the well terminates if it exists
- `aquifer`: aquifer from which the well draws water if it exists
- `well_depth`: depth of well if it exists
- `parameter`: the name of the analyte whose measurements are reported in the table. This corresponds the requested analyte
- `parameter_value`: value of the measurement
- `parameter_units`: units of the measurement
- `date_measured`: date of measurement in YYYY-MM-DD format
- `time_measured`: time of measurement if it exists

### Summary Data

If neither of the above flags are specified, a summary table called `output.csv` is exported. The summary table consists of location information as well as summary statistics for the parameter of interest for every location that has observations.

#### Table Headers: Summary

**output.csv - waterlevels and analytes**
- `source`: the organization/source for the site
- `id`: the id of the site. The id is used as the key to join the output.timeseries.csv table
- `location`: the colloquial name for the site if it exists
- `usgs_site_id`: USGS site id if it exists
- `alternate_site_id`: alternate site id if it exists
- `latitude`: latitude in decimal degrees
- `longitude`: the longitude in decimal degrees
- `horizontal_datum`: horizontal datum of the latitude and longitude. Defaults to WGS84
- `elevation` ground surface elevation of the site in feet
- `elevation_units`: the units of the ground surface elevation. Defaults to ft
- `well_depth`: depth of well if it exists
- `well_depth_units`: units of well depth. Defaults to ft
- `parameter`: the name of the analyte whose measurements are reported in the table. This corresponds the requested analyte
- `parameter_value`: value of the measurement
- `parameter_units`: units of the measurement
- `nrecords`: the number of records for the site
- `min`: the minimum record for the site
- `max`: the maximum record for the site
- `mean`: the mean value for the records at the site
- `most_recent_date`: date of most recent record
- `most_recent_time`: time of most recent record if it exists
- `most_recent_value` the value of the most recent record
- `most_recent_units`: the units of the most recent record