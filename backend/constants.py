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

TDS = "tds"
ARSENIC = "arsenic"
BICARBONATE = "bicarbonate"
CALCIUM = "calcium"
CARBONATE = "carbonate"
CHLORIDE = "chloride"
FLUORIDE = "fluoride"
MAGNESIUM = "magnesium"
NITRATE = "nitrate"
POTASSIUM = "potassium"
SILICA = "silica"
SODIUM = "sodium"
SULFATE = "sulfate"
URANIUM = "uranium"
WATERLEVELS = "waterlevels"


PH = "ph"


MILLIGRAMS_PER_LITER = "mg/L"
MICROGRAMS_PER_LITER = "ug/L"
PARTS_PER_MILLION = "ppm"
TONS_PER_ACRE_FOOT = "tons/ac ft"
FEET = "ft"
METERS = "m"
WGS84 = "WGS84"

DT_MEASURED = "datetime_measured"

DTW = "depth_to_water_below_ground_surface"
DTW_UNITS = FEET

PARAMETER = "parameter"
PARAMETER_UNITS = "parameter_units"
PARAMETER_VALUE = "parameter_value"

ANALYTE_OPTIONS = sorted(
    [
        ARSENIC,
        BICARBONATE,
        CALCIUM,
        CARBONATE,
        CHLORIDE,
        # FLUORIDE,
        MAGNESIUM,
        NITRATE,
        POTASSIUM,
        SILICA,
        SODIUM,
        SULFATE,
        TDS,
        URANIUM,
        PH,
    ]
)

PARAMETER_OPTIONS = [WATERLEVELS] + ANALYTE_OPTIONS
# ============= EOF =============================================
