# ===============================================================================
# Copyright 2024 Jake Ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================
"""
Ocotillo OGC API - Features (pygeoapi/PostGIS) connector mappings.

The Ocotillo API (ocotillo-api.newmexicowaterdata.org/ogcapi) is intended as a
replacement for the NMBGMR AMP API. Unlike AMP, it does NOT expose raw
per-observation time series. It only publishes "latest" / "summary" snapshot
collections. Therefore this connector supports SUMMARY output only; time series
output is unsupported (see OcotilloWaterLevelSource/OcotilloAnalyteSource).

Because the API pre-aggregates, several DIG summary columns cannot be populated
and are intentionally left null (per project decision): mean, earliest_* for all
parameters; nrecords/min/max for chemistry and TDS (only water levels expose
count/min/max via water_well_summary).
"""
from backend.constants import (
    ARSENIC,
    BICARBONATE,
    CALCIUM,
    CARBONATE,
    CHLORIDE,
    FLUORIDE,
    MAGNESIUM,
    NITRATE,
    PH,
    POTASSIUM,
    SILICA,
    SODIUM,
    SPECIFIC_CONDUCTANCE,
    SULFATE,
    TDS,
    URANIUM,
)

# Collection ids on the Ocotillo OGC API
SITE_COLLECTION = "water_wells"
WATERLEVEL_SUMMARY_COLLECTION = "water_well_summary"
MAJOR_CHEMISTRY_COLLECTION = "major_chemistry_results"
MINOR_CHEMISTRY_COLLECTION = "minor_chemistry_wells"
TDS_COLLECTION = "latest_tds_wells"

# DIG analyte -> (Ocotillo collection, property/column holding the latest value).
# Major-ion / field chemistry live in major_chemistry_results as static analyte
# columns; trace metals live in minor_chemistry_wells; TDS has its own
# latest_tds_wells collection with bespoke field names (handled in the source).
OCOTILLO_ANALYTE_MAPPING: dict = {
    CALCIUM: (MAJOR_CHEMISTRY_COLLECTION, "calcium"),
    MAGNESIUM: (MAJOR_CHEMISTRY_COLLECTION, "magnesium"),
    SODIUM: (MAJOR_CHEMISTRY_COLLECTION, "sodium"),
    POTASSIUM: (MAJOR_CHEMISTRY_COLLECTION, "potassium"),
    BICARBONATE: (MAJOR_CHEMISTRY_COLLECTION, "bicarbonate"),
    CARBONATE: (MAJOR_CHEMISTRY_COLLECTION, "carbonate"),
    SULFATE: (MAJOR_CHEMISTRY_COLLECTION, "sulfate"),
    CHLORIDE: (MAJOR_CHEMISTRY_COLLECTION, "chloride"),
    NITRATE: (MAJOR_CHEMISTRY_COLLECTION, "nitrate"),
    FLUORIDE: (MAJOR_CHEMISTRY_COLLECTION, "fluoride"),
    SILICA: (MAJOR_CHEMISTRY_COLLECTION, "silica"),
    PH: (MAJOR_CHEMISTRY_COLLECTION, "ph"),
    SPECIFIC_CONDUCTANCE: (MAJOR_CHEMISTRY_COLLECTION, "specific_conductance"),
    ARSENIC: (MINOR_CHEMISTRY_COLLECTION, "arsenic"),
    URANIUM: (MINOR_CHEMISTRY_COLLECTION, "uranium"),
    TDS: (TDS_COLLECTION, "latest_tds_value"),
}

# ============= EOF =============================================
