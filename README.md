# New Mexico Unified Water Data: Data Integration Engine
[![Format code](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/format_code.yml/badge.svg?branch=main)](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/format_code.yml)
[![Publish Python 🐍 distributions 📦 to PyPI and TestPyPI](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/publish-to-pypi.yml/badge.svg)](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/publish-to-pypi.yml)
[![CI/CD](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/cicd.yml/badge.svg)](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/cicd.yml)


![NMWDI](https://newmexicowaterdata.org/wp-content/uploads/2023/11/newmexicowaterdatalogoNov2023.png)
![NMBGMR](https://waterdata.nmt.edu/latest/static/nmbgmr_logo_resized.png)


This package provides a command line interface to New Mexico Water Data Initiaive's Data Integration Engine. This tool is used to integrate the water data from multiple sources.

## Installation
```bash
pip install nmuwd
```

## Sources
Data comes from the following sources. We are continuously adding new sources as we learn of them and they become available. If you have data that you would like to be part of the Data Integration Engine please get in touch at newmexicowaterdata@nmt.edu.

 - [Bureau of Reclamation](https://data.usbr.gov/) 
 - [USGS (NWIS)](https://waterdata.usgs.gov/nwis)
 - [ST2 (NMWDI)](https://st2.newmexicowaterdata.org/FROST-Server/v1.1/)
   - Pecos Valley Artesian Conservancy District
   - Bernalillo County
   - New Mexico Environment Department Drinking Water Bureau
 - [NM Water Data CKAN catalog](https://catalog.newmexicowaterdata.org/)
   - OSE Roswell District Office
 - ISC Seven Rivers
 - [New Mexico Bureau of Geology and Mineral Resources (AMP)](https://waterdata.nmt.edu/)
 - [Water Quality Portal](https://www.waterqualitydata.us/)
   - USGS
   - EPA
   - and over 400 state, federal, tribal, and local agencies


### Source Inclusion & Exclusion
The Data Integration Engine enables the user to obtain groundwater level and groundwater quality data from a variety of sources. Data from sources are included in the output unless sources are specifically excluded. The following flags are available to exclude a specific data source:

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

## Usage
The data is saved to the current working directory. A log of the inputs and processes, called `die.log`, is also saved to the current working directory. If a subsquent process is run and the log from the previous process has not been moved or stored elsewhere, the log for the subsequent process will be appended to the existing log.

### Timeseries & Summary
The flag `--separated_timeseries` exports timeseries for every location in their own file in the directory output_series (e.g. `AB-0002.csv`, `AB-0003.csv`). Locations with only one observation are gathered and exported to the file `output.combined.csv`.

The flag `--unified_timeseries` exports all timeseries for all locations in one file titled `output.timeseries.csv`. It also exports a file titled `output.sites.csv` that contains site information, such as latitude, longitude, and elevation.

If neither of the above flags are specified, a summary table called `output.csv` is exported. The summary table consists of location information as well as summary statistics for the parameter of interest for every location that has observations.

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