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

TDS = "TDS"
ARSENIC = "Arsenic"
BICARBONATE = "Bicarbonate"
CALCIUM = "Calcium"
CARBONATE = "Carbonate"
CHLORIDE = "Chloride"
FLUORIDE = "Fluoride"
MAGNESIUM = "Magnesium"
NITRATE = "Nitrate"
POTASSIUM = "Potassium"
SODIUM = "Sodium"
SULFATE = "Sulfate"
URANIUM = "Uranium"


PH = "pH"


MILLIGRAMS_PER_LITER = "mg/L"
MICROGRAMS_PER_LITER = "ug/L"
PARTS_PER_MILLION = "ppm"
TONS_PER_ACRE_FOOT = "tons/ac ft"
FEET = "ft"
METERS = "m"
WGS84 = "WGS84"

DTW = "depth_to_water_ft_below_ground_surface"
DTW_UNITS = FEET

DTW_DT_MEASURED = "datetime_measured"

ANALYTE_CHOICES = [
    ARSENIC,
    BICARBONATE,
    CALCIUM,
    CARBONATE,
    CHLORIDE,
    # FLUORIDE,
    MAGNESIUM,
    NITRATE,
    POTASSIUM,
    SODIUM,
    SULFATE,
    TDS,
    URANIUM,
    PH,
]
# ============= EOF =============================================
