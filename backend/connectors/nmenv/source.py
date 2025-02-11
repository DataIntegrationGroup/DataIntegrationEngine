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
from backend.constants import (
    PARAMETER_NAME,
    PARAMETER_VALUE,
    PARAMETER_UNITS,
    DT_MEASURED,
    SOURCE_PARAMETER_NAME,
    SOURCE_PARAMETER_UNITS,
)
from backend.source import get_analyte_search_param, get_most_recent

URL = "https://nmenv.newmexicowaterdata.org/FROST-Server/v1.1/"

import sys


class DWBSiteSource(STSiteSource):
    url = URL
    transformer_klass = DWBSiteTransformer
    bounding_polygon = NM_STATE_BOUNDING_POLYGON

    def __repr__(self):
        return "DWBSiteSource"

    def health(self):
        return self.get_records(top=10, analyte="TDS")

    def get_records(self, *args, **kw):
        analyte = None
        if "analyte" in kw:
            analyte = kw["analyte"]
        elif self.config:
            analyte = self.config.parameter

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

    def __repr__(self):
        return "DWBAnalyteSource"

    def _parse_result(
        self, result, result_dt=None, result_id=None, result_location=None
    ):
        if "< mrl" in result.lower() or "< mdl" in result.lower():
            if self.config.output_summary:
                self.warn(
                    f"Non-detect found: {result} for {result_location} on {result_dt} (observation {result_id}). Setting to 0 for summary."
                )
                return 0.0
            else:
                # return the results for timeseries, regardless of format (None/Null/non-detect)
                return result
        else:
            return float(result.split(" ")[0])

    def get_records(self, site, *args, **kw):
        service = self.get_service()

        analyte = get_analyte_search_param(self.config.parameter, DWB_ANALYTE_MAPPING)
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
        # this is only used for time series
        record[PARAMETER_NAME] = self.config.parameter
        record[PARAMETER_VALUE] = self._parse_result(record["observation"].result)
        record[PARAMETER_UNITS] = self.config.analyte_output_units
        record[DT_MEASURED] = record["observation"].phenomenon_time
        record[SOURCE_PARAMETER_NAME] = record["datastream"].observed_property.name
        record[SOURCE_PARAMETER_UNITS] = record["datastream"].unit_of_measurement.symbol
        return record

    def _extract_source_parameter_results(self, records):
        # this is only used in summary output
        return [
            self._parse_result(
                r["observation"].result,
                r["observation"].phenomenon_time,
                r["observation"].id,
                r["location"].id,
            )
            for r in records
        ]

    def _extract_source_parameter_units(self, records):
        # this is only used in summary output
        return [r["datastream"].unit_of_measurement.symbol for r in records]

    def _extract_parameter_dates(self, records: list) -> list:
        return [r["observation"].phenomenon_time for r in records]

    def _extract_source_parameter_names(self, records: list) -> list:
        return [r["datastream"].observed_property.name for r in records]

    def _extract_most_recent(self, records):
        # this is only used in summary output
        record = get_most_recent(
            records, tag=lambda x: x["observation"].phenomenon_time
        )

        return {
            "value": self._parse_result(
                record["observation"].result,
                record["observation"].phenomenon_time,
                record["observation"].id,
                record["location"].id,
            ),
            "datetime": record["observation"].phenomenon_time,
            "source_parameter_units": record["datastream"].unit_of_measurement.symbol,
            "source_parameter_name": record["datastream"].observed_property.name,
        }


# ============= EOF =============================================
