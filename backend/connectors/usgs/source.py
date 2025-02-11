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
    PARAMETER,
    PARAMETER_VALUE,
    PARAMETER_UNITS,
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
    get_most_recent,
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
        for value in location["values"][0]["value"]:
            record = {
                "site_code": site_code,
                "value": value["value"],
                "datetime_measured": value["dateTime"],
                # "date_measured": value["dateTime"].split("T")[0],
                # "time_measured": value["dateTime"].split("T")[1],
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
        params = {
            "format": "json",
            "siteType": "GW",
            "siteStatus": "all",
            "parameterCd": "72019",
            "sites": ",".join(make_site_list(site_record)),
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
        return [ri for ri in records if ri["site_code"] == site_record.id]

    def _clean_records(self, records):
        return [r for r in records if r["value"] is not None and r["value"].strip()]

    def _extract_parameter_results(self, records):
        return [float(r["value"]) for r in records]

    def _extract_parameter_dates(self, records: list) -> list:
        return [r["datetime_measured"] for r in records]

    def _extract_most_recent(self, records):
        record = get_most_recent(records, "datetime_measured")
        return {
            "value": float(record["value"]),
            # "datetime": (record["date_measured"], record["time_measured"]),
            "datetime": record["datetime_measured"],
            "units": FEET,
        }

    def _extract_parameter_record(self, record):
        record[PARAMETER] = DTW
        record[PARAMETER_VALUE] = float(record["value"])
        record[PARAMETER_UNITS] = FEET
        # record[DT_MEASURED] = (record["date_measured"], record["time_measured"])
        record[DT_MEASURED] = record["datetime_measured"]
        return record


# ============= EOF =============================================
