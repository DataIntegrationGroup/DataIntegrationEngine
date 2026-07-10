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
from backend.connectors._sensorthings import sta_query
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


class DWBSiteSource(STSiteSource):
    url = URL
    bounding_polygon = NM_STATE_BOUNDING_POLYGON

    def __init__(self):
        super().__init__(transformer=DWBSiteTransformer())

    def health(self):
        try:
            resp = self.get_records(top=10, analyte=TDS)
            return bool(resp)
        except Exception:
            return False
            
    def get_records(self, *args, **kw):

        analyte = None
        if "analyte" in kw:
            analyte = kw["analyte"]
        elif self.config:
            analyte = self.config.parameter

        if self.config.sites_only:
            fs = []
            if self.config.has_bounds():
                fs.append(
                    f"st_within(Locations/location, geography'{self.config.bounding_wkt()}')"
                )
            things = sta_query(
                self.url,
                "Things",
                expand="Locations",
                filter=" and ".join(fs) if fs else None,
                top=kw.get("top"),
            )
            return [t["Locations"][0] for t in things if t.get("Locations")]
        else:
            analyte = get_analyte_search_param(analyte, DWB_ANALYTE_MAPPING)
            if analyte is None:
                return []

            fs = [f"ObservedProperty/id eq {analyte}"]
            if self.config and self.config.has_bounds():
                fs.append(
                    f"st_within(Thing/Location/location, geography'{self.config.bounding_wkt()}')"
                )

            datastreams = sta_query(
                self.url,
                "Datastreams",
                expand="Thing/Locations",
                filter=" and ".join(fs),
                top=kw.get("top"),
            )

            # NM ENV has multiple datastreams per parameter per location (e.g. id 8 and arsenic)
            # because of this duplicative site information is retrieved (we operated under the assumption one datastream per location per parameter)
            # so we need to filter out duplicates, otherwise there will be multiple site records and duplicative parameter records
            all_sites = [
                di["Thing"]["Locations"][0]
                for di in datastreams
                if di.get("Thing", {}).get("Locations")
            ]

            # dedupe by location id (the JSON dicts are not hashable)
            site_dictionary = {}
            for site in all_sites:
                site_id = site["@iot.id"]
                if site_id not in site_dictionary:
                    site_dictionary[site_id] = site

            return list(site_dictionary.values())


class DWBAnalyteSource(STAnalyteSource):
    url = URL

    def __init__(self):
        super().__init__(transformer=DWBAnalyteTransformer())

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
        analyte = get_analyte_search_param(self.config.parameter, DWB_ANALYTE_MAPPING)
        datastreams = sta_query(
            self.url,
            "Datastreams",
            expand="Thing/Locations, ObservedProperty",
            filter=f"Thing/Locations/id eq {site.id} and ObservedProperty/id eq {analyte}",
        )

        # NMED DWB has multiple datastreams per parameter per location (e.g. id 8 and arsenic)
        rs = []
        for datastream in datastreams:
            obs_list = sta_query(
                self.url, f"Datastreams({datastream['@iot.id']})/Observations"
            )
            for obs in obs_list:
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
        record[PARAMETER_VALUE] = self._parse_result(record["observation"]["result"])
        record[PARAMETER_UNITS] = self.config.analyte_output_units
        record[DT_MEASURED] = record["observation"]["phenomenonTime"]
        record[SOURCE_PARAMETER_NAME] = record["datastream"]["ObservedProperty"]["name"]
        record[SOURCE_PARAMETER_UNITS] = record["datastream"]["unitOfMeasurement"]["symbol"]
        return record

    def _extract_source_parameter_results(self, records):
        # this is only used in summary output
        return [
            self._parse_result(
                r["observation"]["result"],
                r["observation"]["phenomenonTime"],
                r["observation"]["@iot.id"],
                r["location"].id,
            )
            for r in records
        ]

    def _extract_source_parameter_units(self, records):
        # this is only used in summary output
        return [r["datastream"]["unitOfMeasurement"]["symbol"] for r in records]

    def _extract_parameter_dates(self, records: list) -> list:
        return [r["observation"]["phenomenonTime"] for r in records]

    def _extract_source_parameter_names(self, records: list) -> list:
        return [r["datastream"]["ObservedProperty"]["name"] for r in records]

    def _extract_terminal_record(self, records, position):
        # this is only used in summary output
        record = get_terminal_record(
            records, tag=lambda x: x["observation"]["phenomenonTime"], position=position
        )

        return {
            "value": self._parse_result(
                record["observation"]["result"],
                record["observation"]["phenomenonTime"],
                record["observation"]["@iot.id"],
                record["location"].id,
            ),
            "datetime": record["observation"]["phenomenonTime"],
            "source_parameter_units": record["datastream"]["unitOfMeasurement"]["symbol"],
            "source_parameter_name": record["datastream"]["ObservedProperty"]["name"],
        }


# ============= EOF =============================================
