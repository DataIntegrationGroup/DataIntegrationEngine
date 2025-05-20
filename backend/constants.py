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
EARLIEST = "earliest"
LATEST = "latest"


WATERLEVELS = "waterlevels"
ARSENIC = "arsenic"
BICARBONATE = "bicarbonate"
CALCIUM = "calcium"
CARBONATE = "carbonate"
CHLORIDE = "chloride"
FLUORIDE = "fluoride"
MAGNESIUM = "magnesium"
NITRATE = "nitrate"
PH = "ph"
POTASSIUM = "potassium"
SILICA = "silica"
SODIUM = "sodium"
SULFATE = "sulfate"
TDS = "tds"
URANIUM = "uranium"


MILLIGRAMS_PER_LITER = "mg/L"
MICROGRAMS_PER_LITER = "ug/L"
PARTS_PER_MILLION = "ppm"
PARTS_PER_BILLION = "ppb"
TONS_PER_ACRE_FOOT = "tons/ac ft"
FEET = "ft"
METERS = "m"
WGS84 = "WGS84"

DT_MEASURED = "datetime_measured"

DTW = "depth_to_water_below_ground_surface"
DTW_UNITS = FEET

PARAMETER_NAME = "parameter_name"
PARAMETER_UNITS = "parameter_units"
PARAMETER_VALUE = "parameter_value"

SOURCE_PARAMETER_NAME = "source_parameter_name"
SOURCE_PARAMETER_UNITS = "source_parameter_units"
CONVERSION_FACTOR = "conversion_factor"

USGS_PCODE_30210 = "30210"
USGS_PCODE_70300 = "70300"
USGS_PCODE_70301 = "70301"
USGS_PCODE_70303 = "70303"

ANALYTE_OPTIONS = sorted(
    [
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
        SULFATE,
        TDS,
        URANIUM,
    ]
)

PARAMETER_OPTIONS = [WATERLEVELS] + ANALYTE_OPTIONS
# ============= EOF =============================================
