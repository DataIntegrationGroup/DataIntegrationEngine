# New Mexico Unified Water Data: Data Integration Engine
[![Format code](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/format_code.yml/badge.svg?branch=main)](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/format_code.yml)
[![Publish Python 🐍 distributions 📦 to PyPI and TestPyPI](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/publish-to-pypi.yml/badge.svg)](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/publish-to-pypi.yml)
[![CI/CD](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/cicd.yml/badge.svg)](https://github.com/DataIntegrationGroup/PyWeaver/actions/workflows/cicd.yml)


![NMWDI](https://newmexicowaterdata.org/wp-content/uploads/2023/11/newmexicowaterdatalogoNov2023.png)
![NMBGMR](https://waterdata.nmt.edu/static/nmbgmr_logo_resized.png)


This package provides a command line interface to New Mexico Water Data Initiaive's Data Integration Engine. This tool is used to integrate the water data from multiple sources.


## Sources
 - [Bureau of Reclamation](https://data.usbr.gov/) 
 - [USGS (NWIS)](https://waterdata.usgs.gov/nwis)
 - [ST2 (NMWDI)](https://st2.newmexicowaterdata.org/FROST-Server/v1.1/)
   - Pecos Valley Artesian Conservancy District
   - Elephant Butte Irrigation District
   - Bernalillo County
 - [NM Water Data CKAN catalog](https://catalog.newmexicowaterdata.org/)
   - OSE Roswell District Office
 - ISC Seven Rivers
 - [New Mexico Bureau of Geology and Mineral Resources (AMP)](https://waterdata.nmt.edu/)
 - [Water Quality Portal](https://www.waterqualitydata.us/)
   - USGS
   - EPA
   - and over 400 state, federal, tribal, and local agencies

## Installation

```bash
pip install nmuwd
```

## Usage
### Water Levels

Get water levels for a county. Return a summary csv
```bash
weave waterlevels --county eddy
```
Get water levels for a bounding box. Return a summary csv
```bash
weave waterlevels --bbox -106.5 32.5 -106.0 33.0
```


Get water levels for a county. Return timeseries of water levels for each site
```bash
weave waterlevels --county eddy --timeseries
```

Exclude a specific data source
```bash
weave waterlevels --county eddy --no-amp
```

Exclude multiple data sources
```bash
weave waterlevels --county eddy --no-amp --no-nwis
```

Available data source flags:
 - --no-amp
 - --no-bor
 - --no-ckan
 - --no-dwb
 - --no-isc-seven-rivers
 - --no-nwis
 - --no-pvacd
 - --no-wqp
 - --no-bernco



### Water Quality
```bash
weave analytes TDS --county eddy
```
```bash
weave analytes TDS --county eddy --no-bor
```

Available analytes:
- Arsenic
- Bicarbonate
- Calcium
- Carbonate
- Chloride
- Magnesium
- Nitrate
- pH
- Potassium
- Sodium
- Sulfate
- TDS
- Uranium