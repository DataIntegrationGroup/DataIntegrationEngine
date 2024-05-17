# New Mexico Unified Water Data: Data Integration Engine

This package provides a command line interface to New Mexico Water Data Initiaive's data integration engine.
Data Integration Engine. This tool is used to integrate the water data from multiple sources.


## Sources
 - Bureau of Reclamation
 - USGS (NWIS)
 - ST2 (NMWDI)
   - Pecos Valley Artesian Conservancy District
   - Elephant Butte Irrigation District
 - OSE Roswell District Office
 - ISC Seven Rivers
 - New Mexico Bureau of Geology and Mineral Resources (AMP)
 

## Installation

```bash
pip install nmuwd
```

## Usage

```bash
weave waterlevels --county eddy
```
```bash
weave analytes TDS --county eddy
```
```bash
weave analytes TDS --county eddy --no-bor
```
