# ===============================================================================
# Copyright 2024 ross
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
from backend.config import Config
from backend.connectors.st2.source import PVACDSiteSource, PVACDWaterLevelSource
from matplotlib import pyplot as plt

from backend.constants import DTW


def hydrograph():
    config = Config()
    ss = PVACDSiteSource(config=config)

    for si in ss.read_sites():
        parameter_source = PVACDWaterLevelSource(config=config)
        srecords = parameter_source.load(si, False)

        xs = [r.date_measured for site, records in srecords for r in records]
        ys = [getattr(r, DTW) for site, records in srecords for r in records]
        for xi, yi in zip(xs, ys):
            print(xi, yi)
        plt.plot(xs, ys)

        break
    plt.show()


if __name__ == "__main__":
    hydrograph()
# ============= EOF =============================================
