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
from typing import Protocol, runtime_checkable

from backend.constants import (
    MILLIGRAMS_PER_LITER,
    PARTS_PER_MILLION,
    PARTS_PER_BILLION,
    FEET,
    METERS,
    TONS_PER_ACRE_FOOT,
    MICROGRAMS_PER_LITER,
)


@runtime_checkable
class UnitConverter(Protocol):
    def convert(
        self,
        input_value: float,
        input_units: str,
        output_units: str,
        source_parameter_name: str,
        die_parameter_name: str,
        dt: str | None = None,
    ) -> tuple[float, float | None, str]: ...


class StandardUnitConverter:
    def convert(
        self,
        input_value: float,
        input_units: str,
        output_units: str,
        source_parameter_name: str,
        die_parameter_name: str,
        dt: str | None = None,
    ) -> tuple[float, float | None, str]:
        warning = ""
        conversion_factor = None

        input_value = float(input_value)
        input_units = input_units.strip().lower()
        output_units = output_units.strip().lower()
        source_parameter_name = source_parameter_name.strip().lower()
        die_parameter_name = die_parameter_name.strip().lower()

        mgl = MILLIGRAMS_PER_LITER.lower()
        ugl = MICROGRAMS_PER_LITER.lower()
        ppm = PARTS_PER_MILLION.lower()
        ppb = PARTS_PER_BILLION.lower()
        tpaf = TONS_PER_ACRE_FOOT.lower()
        ft = FEET.lower()
        m = METERS.lower()

        if die_parameter_name == "ph":
            conversion_factor = 1.0
        elif die_parameter_name in ["conductivity", "specific_conductance"]:
            if input_units in ["μmhos/cm", "umho/cm", "cm-1", "micromhos per centimeter", "mg/l", "su", "us/cm", "us/cm @25c", "µs/cm", "μs/cm"]:
                conversion_factor = 1.0
        elif output_units == mgl:
            if input_units in ["mg/l caco3", "mg/l caco3**"]:
                if die_parameter_name == "bicarbonate":
                    conversion_factor = 1.22
                elif die_parameter_name == "calcium":
                    conversion_factor = 0.4
                elif die_parameter_name == "carbonate":
                    conversion_factor = 0.6
            elif input_units == "mg/l as n":
                conversion_factor = 4.427
            elif input_units in ["mg/l asno3", "mg/l as no3"]:
                conversion_factor = 1.0
            elif input_units == "ug/l as n":
                conversion_factor = 0.004427
            elif input_units == "pci/l":
                conversion_factor = 0.00149
            elif input_units in (ugl, ppb):
                conversion_factor = 0.001
            elif input_units == tpaf:
                conversion_factor = 735.47
            elif input_units == ppm:
                conversion_factor = 1.0
            elif input_units == output_units:
                if source_parameter_name in ["nitrate as n", "nitrate (as n)"]:
                    conversion_factor = 4.427
                else:
                    conversion_factor = 1.0
        elif output_units == ft:
            if input_units in [m, "meters"]:
                conversion_factor = 3.28084
            elif input_units in [ft, "feet"]:
                conversion_factor = 1.0
        elif output_units == m:
            if input_units in [ft, "feet"]:
                conversion_factor = 0.3048
            elif input_units in [m, "meters"]:
                conversion_factor = 1.0

        if conversion_factor:
            return input_value * conversion_factor, conversion_factor, warning
        warning = f"Failed to convert {input_value} {input_units} {source_parameter_name} (source) to {output_units} {die_parameter_name} (die) on {dt}"
        return input_value, conversion_factor, warning


# ============= EOF =============================================
