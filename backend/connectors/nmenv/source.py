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
    TDS,
)
from backend.source import get_analyte_search_param, get_terminal_record

URL = "https://nmenv.newmexicowaterdata.org/FROST-Server/v1.1/"

import sys


class DWBSiteSource(STSiteSource):
    url = URL
    transformer_klass = DWBSiteTransformer
    bounding_polygon = NM_STATE_BOUNDING_POLYGON

    def __repr__(self):
        return "DWBSiteSource"

    def health(self):
        return self.get_records(top=10, analyte=TDS)

    def get_records(self, *args, **kw):

        analyte = None
        if "analyte" in kw:
            analyte = kw["analyte"]
        elif self.config:
            analyte = self.config.parameter

        service = self.get_service()
        if self.config.sites_only:
            ds = service.things()
            q = ds.query()
            fs = []
            if self.config.has_bounds():
                fs.append(
                    f"st_within(Locations/location, geography'{self.config.bounding_wkt()}')"
                )
            q = q.expand("Locations")
            if fs:
                q = q.filter(" and ".join(fs))
            return [thing.locations.entities[0] for thing in q.list()]
        else:
            analyte = get_analyte_search_param(analyte, DWB_ANALYTE_MAPPING)
            if analyte is None:
                return []

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

            # NM ENV has multiple datastreams per parameter per location (e.g. id 8 and arsenic)
            # because of this duplicative site information is retrieved (we operated under the assumption one datastream per location per parameter)
            # so we need to filter out duplicates, otherwise there will be multiple site records and duplicative parameter records
            all_sites = [di.thing.locations.entities[0] for di in q.list()]

            # can't do list(set(all_sites)) because the Location entities are not hashable
            site_dictionary = {}
            for site in all_sites:
                site_id = site.id
                if site_id not in site_dictionary.keys():
                    site_dictionary[site_id] = site

            distinct_sites = list(site_dictionary.values())
            # print(
            #     f"Found {len(all_sites)} datastreams for {analyte} and {len(distinct_sites)} distinct sites."
            # )
            return distinct_sites


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

        # NMED DWB has multiple datastreams per parameter per location (e.g. id 8 and arsenic)
        # print(
        #     f"Found {len(q.list().entities)} datastreams for {site.id} and {analyte}."
        # )
        rs = []
        for datastream in q.list().entities:
            for obs in datastream.get_observations().query().list():
                rs.append(
                    {
                        "location": site,
                        "datastream": datastream,
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

    def _extract_terminal_record(self, records, bookend):
        # this is only used in summary output
        record = get_terminal_record(
            records, tag=lambda x: x["observation"].phenomenon_time, bookend=bookend
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
