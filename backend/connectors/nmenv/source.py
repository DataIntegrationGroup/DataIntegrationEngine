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
from backend.connectors.nmenv.transformer import (
    DWBSiteTransformer,
    DWBAnalyteTransformer,
)
from backend.connectors.st_connector import STSiteSource, STAnalyteSource
from backend.source import get_analyte_search_param

URL = "https://nmenv.newmexicowaterdata.org/FROST-Server/v1.1/"


class DWBSiteSource(STSiteSource):
    url = URL
    transformer_klass = DWBSiteTransformer

    def get_records(self, *args, **kw):
        # rs = super(DWBSiteSource, self).get_records(*args, **kw)
        # return rs[:10]
        service = self.get_service()
        analyte = get_analyte_search_param(self.config.analyte, ANALYTE_MAP)
        ds = service.datastreams()
        q = ds.query()
        q = q.filter(f"ObservedProperty/id eq {analyte}")
        q = q.expand("Thing/Locations")
        return [ds.thing.locations.entities[0] for ds in q.list()]


ANALYTE_MAP = {
    "Arsenic": 3,
    "Chloride": 15,
    "Fluoride": 19,
    "Nitrate": 35,
    "Sulfate": 41,
    "TDS": 90,
    "Uranium-238": 386,
    "Combined Uranium": 385,
}


class DWBAnalyteSource(STAnalyteSource):
    url = URL
    transformer_klass = DWBAnalyteTransformer

    def _parse_result(self, result):
        return float(result.split(" ")[0])

    def get_records(self, site, *args, **kw):
        service = self.get_service()

        analyte = get_analyte_search_param(self.config.analyte, ANALYTE_MAP)
        ds = service.datastreams()
        q = ds.query()
        q = q.expand("Thing/Locations, ObservedProperty, Observations")
        q = q.filter(
            f"Thing/Locations/id eq {site.id} and ObservedProperty/id eq {analyte}"
        )

        ds = q.list().entities[0]
        rs = []
        for obs in ds.get_observations().query().list():
            rs.append(
                {
                    "location": site,
                    "datastream": ds,
                    "observation": obs,
                }
            )

        return rs

    def _extract_parameter_results(self, records):
        return [self._parse_result(r["observation"].result) for r in records]

    def _extract_parameter_units(self, records):
        return [r["datastream"].unit_of_measurement.symbol for r in records]


# ============= EOF =============================================
