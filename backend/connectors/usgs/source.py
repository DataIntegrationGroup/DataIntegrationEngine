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
from datetime import datetime
import httpx

from backend.connectors import NM_STATE_BOUNDING_POLYGON
from backend.constants import (
    FEET,
    DTW,
    DTW_UNITS,
    DT_MEASURED,
    PARAMETER_NAME,
    PARAMETER_VALUE,
    PARAMETER_UNITS,
    SOURCE_PARAMETER_NAME,
    SOURCE_PARAMETER_UNITS,
    EARLIEST,
    LATEST,
)
from backend.connectors.usgs.transformer import (
    NWISSiteTransformer,
    NWISWaterLevelTransformer,
)
from backend.source import (
    BaseSource,
    BaseWaterLevelSource,
    BaseSiteSource,
    make_site_list,
    get_terminal_record,
)


def parse_rdb(text):
    """'
    Parses rdb tab-delimited responses for NWIS Site Services
    """

    def line_generator():
        header = None
        for line in text.split("\n"):
            if line.startswith("#"):
                continue
            elif line.startswith("agency_cd"):
                header = [h.strip() for h in line.split("\t")]
                continue
            elif line.startswith("5s"):
                continue
            elif line == "":
                continue

            vals = [v.strip() for v in line.split("\t")]
            if header and any(vals):
                yield dict(zip(header, vals))

    return list(line_generator())


def parse_json(data):
    """
    Parses JSON responses for NWIS Groundwater Level Services
    """
    records = []

    for location in data["timeSeries"]:
        site_code = location["sourceInfo"]["siteCode"][0]["value"]
        agency = location["sourceInfo"]["siteCode"][0]["agencyCode"]
        source_parameter_name = location["variable"]["variableName"]
        source_parameter_units = location["variable"]["unit"]["unitCode"]
        for value in location["values"][0]["value"]:
            record = {
                "site_id": f"{agency}-{site_code}",
                "source_parameter_name": source_parameter_name,
                "value": value["value"],
                "datetime_measured": value["dateTime"],
                # "date_measured": value["dateTime"].split("T")[0],
                # "time_measured": value["dateTime"].split("T")[1],
                "source_parameter_units": source_parameter_units,
            }
            records.append(record)
    return records


class NWISSiteSource(BaseSiteSource):
    transformer_klass = NWISSiteTransformer
    chunk_size = 500
    bounding_polygon = NM_STATE_BOUNDING_POLYGON

    def __repr__(self):
        return "NWISSiteSource"

    @property
    def tag(self):
        return "nwis"

    def health(self):
        try:
            self._execute_text_request(
                "https://waterservices.usgs.gov/nwis/site/",
                {
                    "format": "rdb",
                    "siteOutput": "expanded",
                    "siteType": "GW",
                    "site": "325754103461301",
                },
            )
            return True
        except httpx.HTTPStatusError:
            pass

    def get_records(self):
        params = {"format": "rdb", "siteOutput": "expanded", "siteType": "GW"}
        config = self.config

        if config.has_bounds():
            bbox = config.bbox_bounding_points()
            params["bBox"] = ",".join([str(b) for b in bbox])
        else:
            params["stateCd"] = "NM"

        if config.start_date:
            params["startDt"] = config.start_dt.date().isoformat()
        if config.end_date:
            params["endDt"] = config.end_dt.date().isoformat()

        text = self._execute_text_request(
            "https://waterservices.usgs.gov/nwis/site/", params
        )
        if text:
            records = parse_rdb(text)
            self.log(f"Retrieved {len(records)} records")
            return records


class NWISWaterLevelSource(BaseWaterLevelSource):
    transformer_klass = NWISWaterLevelTransformer

    def __repr__(self):
        return "NWISWaterLevelSource"

    def get_records(self, site_record):
        # query sites with the agency, which need to be in the form of "{agency}:{site number}"
        sites = make_site_list(site_record)
        sites_with_colons = [s.replace("-", ":") for s in sites]

        params = {
            "format": "json",
            "siteType": "GW",
            "siteStatus": "all",
            "parameterCd": "72019",
            "sites": ",".join(sites_with_colons),
        }

        config = self.config
        if config.start_date:
            params["startDt"] = config.start_dt.date().isoformat()
        else:
            params["startDt"] = "1900-01-01"

        if config.end_date:
            params["endDt"] = config.end_dt.date().isoformat()

        data = self._execute_json_request(
            url="https://waterservices.usgs.gov/nwis/gwlevels/",
            params=params,
            tag="value",
        )
        if data:
            records = parse_json(data)
            self.log(f"Retrieved {len(records)} records")
            return records

    def _extract_site_records(self, records, site_record):
        return [ri for ri in records if ri["site_id"] == site_record.id]

    def _clean_records(self, records):
        return [
            r
            for r in records
            if r["value"] is not None and r["value"].strip() and r["value"] != "-999999"
        ]

    def _extract_source_parameter_results(self, records):
        return [float(r["value"]) for r in records]

    def _extract_parameter_dates(self, records: list) -> list:
        return [r["datetime_measured"] for r in records]

    def _extract_source_parameter_names(self, records: list) -> list:
        return [r["source_parameter_name"] for r in records]

    def _extract_source_parameter_units(self, records):
        return [r["source_parameter_units"] for r in records]

    def _extract_terminal_record(self, records, bookend):
        record = get_terminal_record(records, "datetime_measured", bookend=bookend)
        return {
            "value": float(record["value"]),
            # "datetime": (record["date_measured"], record["time_measured"]),
            "datetime": record["datetime_measured"],
            "source_parameter_units": record["source_parameter_units"],
            "source_parameter_name": record["source_parameter_name"],
        }

    def _extract_parameter_record(self, record):
        record[PARAMETER_NAME] = DTW
        record[PARAMETER_VALUE] = float(record["value"])
        record[PARAMETER_UNITS] = self.config.waterlevel_output_units
        record[DT_MEASURED] = record["datetime_measured"]
        record[SOURCE_PARAMETER_NAME] = record["source_parameter_name"]
        record[SOURCE_PARAMETER_UNITS] = record["source_parameter_units"]

        return record


# ============= EOF =============================================
