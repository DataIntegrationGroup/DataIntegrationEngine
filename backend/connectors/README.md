# How to add a source

The following are necessary for adding a source:
- a directory within **/backend/connectors** that contain `__init__.py`, `source.py`, and `transformer.py`
- analyte mappings from the standard names to how they are called in the source in **/backend/mappings.py**
- it needs to be added to `SOURCE_KEYS` in **/backend/config.py**
- `use_source_<source>` flag needs to be added to the `Config` class in **/backend/config.py**
- the `use_source_<source>` flag needs to be added to the `analyte_sources` method in the `Config` class in **/backend/config.py** if analytes are available for that source
- the `use_source_<source>` flag needs to be added to the `water_level_sources` method in the `Config` class in **/backend/config.py** if water levels are available for that source

For the sake of discription, the example source is called Faux.

# /backend/connectors/faux

Make `__init__.py`, `source.py`, `transformer.py`

## source.py
At the very least, every source needs a site class. If the source reports analytes, an analyte class is needed. If the source reports water levels, a water level class is needed.
When a new source is added, the methods that need to be defined don't necessarily require doc strings because they are already written in the base classes. However, comments are encouraged for clarity and understanding.

### FauxSiteSource(BaseSiteSource)
`FauxSiteSource` inherits from `BaseSiteSource`, which is defined in **/backend/source.py**. 
The following methods need to be defined for Faux. See `BaseSiteSource` for doc strings for doc strings for each of the methods:

- `health`
- `get_records`

### FauxAnalyteSource(BaseAnalyteSource)
`FauxAnalyteSource` inherits from `BaseAnalyteSource`, which is defined in **/backend/source.py**.
The following methods need to be defined for Faux. See `BaseAnalyteSource` for doc strings for each of the methods:

- `get_records`
- `_extract_parent_records`
- `_extract_parameter_units`
- `_extract_most_recent`
- `_extract_parameter_result`
- `_extract_parameter_record`

The following method is optional:

- `_clean_records`

### FauxWaterLevelSource(BaseWaterLevelSource)
`FauxWaterLevelSource` inherits from `BaseWaterLevelSource`, which is defined in **/backend/source.py**
The following methods need to be defined for Faux. See `BaseWaterLevelSource` for doc strings for each of the methods:

- `get_records`
- `_extract_parent_records`
- `_extract_parameter_units`
- `_extract_most_recent`
- `_extract_parameter_result`
- `_extract_parameter_record`

The following method is optional:

- `_clean_records`

## transformer.py

Each source - site, analyte, water level - needs a transformer to coerce the results into the standard format. 
This transformation adds fields that have standard naming conventions across all sources. It also changes the records to correspond with record classes in **/backend/record.py**. These function the same as dictionaries, but have the method `to_row()` so that a record can be written to a table row.

### FauxSiteTransformer(SiteTransformer)
`FauxSiteTransformer` inherits from `SiteTransformer`, which is defined in **/backend/transformer.py**.
The following methods need to be defined for Faux. See `BaseTransformer` for doc strings for the methods:

- `_transform`

### FauxAnalyteTransformer(AnalyteTransformer)
`FauxAnalyteTransformer` inherits from `AnalyteTransformer`, which is defined in **/backend/transformer.py**.
No methods need to be defiend for Faux, but the `source_tag` attribute needs to be set to `Faux`

### FauxWaterLevelTransformer(WaterLevelTransformer)
`FauxWaterLevelTransformer` inherits from `WaterLevelTransformer`, which is defined in **/backend/transformer.py**.
No methods need to be defiend for Faux, but the `source_tag` attribute needs to be set to `Faux`

# /backend/mappings.py

Create a dictionary mapping called `FAUX_ANALYTE_MAPPING` that maps the standardized names to how they are reported in Faux.

# /backend/config.py

Import the sources as

`from .connectors.source import FauxSiteSource, FauxAnalyteSource, FauxWaterLevelSource`

## SOURCE_KEYS

Add `"faux"` to `SOURCE_KEYS`

## get_source(source)
Add the following to the function `get_source`
```
elif source == "faux":
    return FauxSiteSource()
```

## Config

Add `use_source_faux: bool = True` to the `Config` class

Add the following to the `analyte_sources` method if Faux has analytes
```
if self.use_source_faux:
    sources.append((FauxSiteSource(), FauxAnalyteSource()))
```

Add the following to the `water_level_sources` method if Faux has water levels
```
if self.use_source_faux:
    sources.append((FauxSiteSource(), FauxWaterLevelSource()))
```