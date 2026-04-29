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
import httpx

from backend.connectors import NM_STATE_BOUNDING_POLYGON
from backend.constants import (
    DTW,
    DT_MEASURED,
    PARAMETER_NAME,
    PARAMETER_VALUE,
    PARAMETER_UNITS,
    SOURCE_PARAMETER_NAME,
    SOURCE_PARAMETER_UNITS,
)
from backend.connectors.usgs.transformer import (
    NWISSiteTransformer,
    NWISWaterLevelTransformer,
)
from backend.source import (
    BaseWaterLevelSource,
    BaseSiteSource,
    make_site_list,
    get_terminal_record,
)


KEY = "55MILtQrayXw1NgufxcqRfkkRrg4Rg6KNCyJZ004"
LIMIT = 50000    
TIMEOUT=1800

class NWISSiteSource(BaseSiteSource):
    transformer_klass = NWISSiteTransformer
    chunk_size = 500
    bounding_polygon = NM_STATE_BOUNDING_POLYGON
    sites_url: str = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/combined-metadata/items"

    def __repr__(self):
        return "NWISSiteSource"

    @property
    def tag(self):
        return "nwis"

    def health(self):
        try:
            self._execute_json_request(
                url=self.sites_url,
                params={"limit": 1, "parameter_code": "72019", "site_type_code": "GW", "state_code": "35"},
                timeout=TIMEOUT,
                headers={"X-API-Key": KEY},
            )
            return True
        except httpx.HTTPStatusError:
            return False

    def get_records(self):
        # TODO: handle date filters
        # config = self.config

        # if config.has_bounds():
        #     bbox = config.bbox_bounding_points()
        #     params["bbox"] = ",".join([str(b) for b in bbox])
        # else:
        #     params["state_code"] = "35"

        # if config.start_date:
        #     params["startDt"] = config.start_dt.date().isoformat()
        # if config.end_date:
        #     params["endDt"] = config.end_dt.date().isoformat()

        finished_request: bool = False
        while not finished_request:
            try:
                data = self._execute_json_request(
                    url=self.sites_url,
                    params={"limit": LIMIT, "parameter_code": "72019", "site_type_code": "GW", "state_code": "35"},
                    timeout=TIMEOUT,
                    headers={"X-API-Key": KEY},
                )
                # _execute_json_request returns None for non-200 responses, so we need to check for that as well
                if data is None:
                    self.warn("Retrying...")
                else:
                    finished_request = True
            except Exception as e:
                self.warn(f"Error retrieving site records: {e}. Retrying...")

        records: list = data.get("features", [])

        return records


class NWISWaterLevelSource(BaseWaterLevelSource):
    transformer_klass = NWISWaterLevelTransformer
    # USGS complex queries allow up to 250 sites to be queried at once
    # https://api.waterdata.usgs.gov/docs/ogcapi/complex-queries
    num_sites = 250

    def __repr__(self):
        return "NWISWaterLevelSource"

    def get_records(self, site_record):
        # TODO: handle date filters
        # config = self.config
        # if config.start_date:
        #     params["startDt"] = config.start_dt.date().isoformat()
        # else:
        #     params["startDt"] = "1900-01-01"

        # if config.end_date:
        #     params["endDt"] = config.end_dt.date().isoformat()

        records: list = []
        sites: list = make_site_list(site_record)

        # group sites into batches of num_sites to pass to the API
        # USGS APIs allow up to 250 sites to be queried at once with complex queries
        list_of_lists_of_sites: list = []
        for i in range(0, len(sites), self.num_sites):
            list_of_lists_of_sites.append(sites[i:i + self.num_sites]) 


        for list_of_sites in list_of_lists_of_sites:
            json_data: dict = {
                "op": "in",
                "args": [
                    {"property": "monitoring_location_id"},
                    list_of_sites
                ]
            }
            finished_request: bool = False
            while not finished_request:
                try:
                    response = httpx.post(
                        url="https://api.waterdata.usgs.gov/ogcapi/v0/collections/field-measurements/items",
                        json=json_data,
                        headers={"X-API-Key": KEY, "Content-Type": "application/query-cql-json"},
                        params={"limit": LIMIT, "parameter_code": "72019"},
                        timeout=TIMEOUT,
                    )
                    if response.status_code != 200:
                        self.warn(f"Received status code {response.status_code}. Retrying...")
                    else:
                        finished_request = True
                except Exception as e:
                    self.warn(f"Error retrieving water level records: {e}. Retrying...")

            data: dict = response.json()
            features: list[dict] = data.get("features", [])

            standard_features: list[dict] = [self._standardize_record(feature) for feature in features]
            records.extend(standard_features)
            
            """
            The following commented-out code handles pagination for cases where there are more than LIMIT records for a given batch of sites.
            However, in testing, I have not encountered any cases where this is necessary. Furthermore, cursor-based pagination is broken as
            of 4/29/26 when the limit query parameter is used, and it can't be used in combination with other parameters via complex queries.
            If we do encounter cases where there are more than LIMIT records, we can use the following code to handle pagination (when it is fixed).
            
            found_next_link: bool = False
            links: list[dict] = data.get("links", [])
            for link in links:
                if link["rel"] == "next":
                    next_link_url = link["href"]
                    found_next_link = True
                    break

            # use GET requests for the paginated responses after the initial POST to avoid issues with httpx and long URLs with many site ids
            # USGS APIs use cursor pagination, so we can just follow the "next" links until there are no more
            while found_next_link:
                finished_request: bool = False
                while not finished_request:
                    try:
                        response = httpx.get(
                                url=next_link_url,
                                headers={"X-API-Key": KEY, "Content-Type": "application/query-cql-json"},
                                timeout=TIMEOUT,
                            )
                        if response.status_code != 200:
                            self.warn(f"Received status code {response.status_code} for paginated request. Retrying...")
                        else:
                            finished_request = True
                    except Exception as e:
                        self.warn(f"Error retrieving paginated water level records: {e}. Retrying...
                        
                data: dict = response.json()
                features: list[dict] = data.get("features", [])
                standard_features: list[dict] = [self._standardize_record(feature) for feature in features]
                records.extend(standard_features)
                
                found_next_link: bool = False
                links: list = data.get("links", [])
                for link in links:
                    if link["rel"] == "next":
                        next_link_url = link["href"]
                        found_next_link = True
                        break
            """

        self.log(f"Retrieved {len(records)} records")

        return records
    
    def _standardize_record(self, record: dict) -> dict:
        return {
            "site_id": record["properties"]["monitoring_location_id"],
            "source_parameter_name": "Water level, depth LSD",
            "value": record["properties"]["value"],
            "datetime_measured": record["properties"]["time"],
            "source_parameter_units": record["properties"]["unit_of_measure"]
        }

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
