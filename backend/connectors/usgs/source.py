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

"""
-- sites --
https://api.waterdata.usgs.gov/ogcapi/v0/collections/combined-metadata/items?
state_code=35
site_type_code=GW
parameter_code=72019

-- water levels --
https://api.waterdata.usgs.gov/ogcapi/v0/collections/field-measurements/items?
monitoring_location_id=<>&monitoring_location_id=<>...

parameter_code=72019
"""

KEY = "55MILtQrayXw1NgufxcqRfkkRrg4Rg6KNCyJZ004"

def transform_usgs_waterlevels_record(record: dict) -> dict:
    return {
        "site_id": record["properties"]["monitoring_location_id"],
        "source_parameter_name": "Water level, depth LSD",
        "value": record["properties"]["value"],
        "datetime_measured": record["properties"]["time"],
        "source_parameter_units": record["properties"]["unit_of_measure"]
    }

def retrieve_usgs_data(
    url: str,
    json_data: dict,
    headers: dict = None,
    params: dict = None,
    timeout: int = None,
    transformation_hook=None
) -> list:
    """
    Start with a POST request to retrieve the initial batch of data using complex queries, then
    follow the "next" links in the response to retrieve all paginated data with GET requests.

    The transformation_hook can be used to transform each batch of records as they are retrieved
    """
    records: list = []

    response = httpx.post(
        url=url,
        json=json_data,
        headers=headers,
        params=params,
        timeout=timeout,
    )
    data: dict = response.json()
    features: list[dict] = data.get("features", [])

    if transformation_hook:
        transformed_features = [transformation_hook(feature) for feature in features]
        records.extend(transformed_features)
    else:
        records.extend(features)
    
    # print(f"Retrieved {len(records)} records")

    found_next_link: bool = False
    links: list = data.get("links", [])
    for link in links:
        if link["rel"] == "next":
            next_link_url = link["href"]
            found_next_link = True
            break

    # use GET requests for the paginated responses after the initial POST to avoid issues with httpx and long URLs with many site ids
    # USGS APIs use cursor pagination, so we can just follow the "next" links until there are no more
    while found_next_link:
        # print(f"Following next link: {next_link_url}")
        response = httpx.get(
            url=next_link_url,
            headers=headers,
            timeout=timeout,
        )
        data: dict = response.json()
        features = data.get("features", [])
        if transformation_hook:
            transformed_features = [transformation_hook(feature) for feature in features]
            records.extend(transformed_features)
        else:
            records.extend(features)
        
        # print(f"Retrieved {len(records)} records")

        found_next_link: bool = False
        links: list = data.get("links", [])
        for link in links:
            if link["rel"] == "next":
                next_link_url = link["href"]
                found_next_link = True
                break

    return records


class NWISSiteSource(BaseSiteSource):
    transformer_klass = NWISSiteTransformer
    chunk_size = 500
    bounding_polygon = NM_STATE_BOUNDING_POLYGON
    json_data: dict = {
        "op": "and",
        "args": [
            {
                "op": "in",
                "args": [
                    {"property": "state_code"},
                    ["35"]
                ]
            },
            {
                "op": "in",
                "args": [
                    {"property": "site_type_code"},
                    ["GW"]
                ]
            },
            {
                "op": "in",
                "args": [
                    {"property": "parameter_code"},
                    ["72019"]
                ]
            }
        ]
    }

    def __repr__(self):
        return "NWISSiteSource"

    @property
    def tag(self):
        return "nwis"

    def health(self):
        try:
            httpx.post(
                url="https://api.waterdata.usgs.gov/ogcapi/v0/collections/combined-metadata/items",
                data=self.json_data,
                headers={"X-API-Key": KEY, "Content-Type": "application/query-cql-json"},
                timeout=None
            )
            return True
        except httpx.HTTPStatusError:
            pass

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
        sites_url: str = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/combined-metadata/items"

        data = self._execute_json_request(
            url=sites_url,
            params={"limit": 50000, "parameter_code": "72019", "site_type_code": "GW", "state_code": "35"},
            timeout=None,
            headers={"X-API-Key": KEY},
        )

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
        # since USGS APIs allow up to 250 sites to be queried at once with complex queries
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

            records_batch: list = retrieve_usgs_data(
                url="https://api.waterdata.usgs.gov/ogcapi/v0/collections/field-measurements/items",
                json_data=json_data,
                headers={"X-API-Key": KEY, "Content-Type": "application/query-cql-json"},
                params={"limit": 50000, "parameter_code": "72019"},
                timeout=None,
                transformation_hook=transform_usgs_waterlevels_record
            )
            records.extend(records_batch)

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
