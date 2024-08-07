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
from backend.connectors import NM_STATE_BOUNDING_POLYGON
from backend.connectors.mappings import DWB_ANALYTE_MAPPING
from backend.connectors.nmenv.transformer import (
    DWBSiteTransformer,
    DWBAnalyteTransformer,
)
from backend.connectors.st_connector import STSiteSource, STAnalyteSource
from backend.constants import PARAMETER, PARAMETER_UNITS, DT_MEASURED, PARAMETER_VALUE
from backend.source import get_analyte_search_param

URL = "https://nmenv.newmexicowaterdata.org/FROST-Server/v1.1/"


class DWBSiteSource(STSiteSource):
    url = URL
    transformer_klass = DWBSiteTransformer
    bounding_polygon = NM_STATE_BOUNDING_POLYGON

    def health(self):
        return self.get_records(top=10, analyte="TDS")

    def get_records(self, *args, **kw):
        analyte = None
        if "analyte" in kw:
            analyte = kw["analyte"]
        elif self.config:
            analyte = self.config.analyte

        analyte = get_analyte_search_param(analyte, DWB_ANALYTE_MAPPING)
        if analyte is None:
            return []

        service = self.get_service()
        ds = service.datastreams()
        q = ds.query()
        fs = [f"ObservedProperty/id eq {analyte}"]
        if self.config:
            if self.config.has_bounds():
                fs.append(
                    f"st_within(Thing/Location/location, geography'{self.config.bounding_wkt()}')"
                )

        q = q.filter(" and ".join(fs))
        q = q.expand("Thing/Locations")
        return [ds.thing.locations.entities[0] for ds in q.list()]


class DWBAnalyteSource(STAnalyteSource):
    url = URL
    transformer_klass = DWBAnalyteTransformer

    def _parse_result(self, result):
        return float(result.split(" ")[0])

    def get_records(self, site, *args, **kw):
        service = self.get_service()

        analyte = get_analyte_search_param(self.config.analyte, DWB_ANALYTE_MAPPING)
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

    def _extract_parameter_record(self, record):
        record[PARAMETER_VALUE] = self._parse_result(record["observation"].result)
        record[PARAMETER_UNITS] = record["datastream"].unit_of_measurement.symbol
        record[DT_MEASURED] = record["observation"].phenomenon_time
        return record

    def _extract_parameter_results(self, records):
        return [self._parse_result(r["observation"].result) for r in records]

    def _extract_parameter_units(self, records):
        return [r["datastream"].unit_of_measurement.symbol for r in records]


# ============= EOF =============================================
