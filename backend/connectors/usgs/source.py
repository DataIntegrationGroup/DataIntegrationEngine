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
https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items?
state_code=35
site_type_code=GW

-- water levels --
https://api.waterdata.usgs.gov/ogcapi/v0/collections/field-measurements/items?
monitoring_location_id=<>&monitoring_location_id=<>...

parameter_code=72019
"""

KEY = "55MILtQrayXw1NgufxcqRfkkRrg4Rg6KNCyJZ004"

def parse_waterlevels_json(data):
    """
    Parses JSON responses for USGS field measurements (water levels) into a list of records with standardized keys.
    """
    records = []

    


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
            params = {
                "state_code": "35",
                "site_type_code": "GW"
            }
            self._execute_json_request(
                url="https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items",
                params=params
            )
            return True
        except httpx.HTTPStatusError:
            pass

    def get_records(self):
        params = {
            "site_type_code": "GW",
            "limit": self.chunk_size,
        }
        config = self.config

        if config.has_bounds():
            bbox = config.bbox_bounding_points()
            params["bbox"] = ",".join([str(b) for b in bbox])
        else:
            params["state_code"] = "35"

        # if config.start_date:
        #     params["startDt"] = config.start_dt.date().isoformat()
        # if config.end_date:
        #     params["endDt"] = config.end_dt.date().isoformat()

        reached_end: bool = False
        records: list = []
        sites_url: str = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items"

        """
        TODO

        update the site transformer to transform into standardized format
        """

        while not reached_end:
            response = self._execute_json_request(
                url=sites_url,
                params=params,
                headers={"X-API-Key": KEY}
            )

            records.extend(response.get("features", []))

            found_next_link: bool = False
            for link in response["links"]:
                if link["rel"] == "next":
                    sites_url = link["href"]
                    params = None  # next link already has params encoded
                    found_next_link = True
                    break
            
            if not found_next_link  :
                reached_end = True
           

        return records


# TODO: IMPLEMENT! and transform as necessary. keep in mind "next" links for pagination

class NWISWaterLevelSource(BaseWaterLevelSource):
    transformer_klass = NWISWaterLevelTransformer
    # chunk_size=5 to avoid URI length and httpx read timed out issue
    chunk_size = 5

    def __repr__(self):
        return "NWISWaterLevelSource"

    def get_records(self, site_record):
        records: list = []

        # if more than 5 sites are provided the URI is too long
        sites: list = make_site_list(site_record)

        # chunk the sites into groups of 5 to avoid URI length issues
        chunks_of_sites: list = []
        for i in range(0, len(sites), self.chunk_size):
            chunks_of_sites.append(sites[i:i + self.chunk_size]) 


        for chunked_sites in chunks_of_sites:
            delineated_sites: str = ",".join(chunked_sites)
        
            obs_url: str = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/field-measurements/items"

            reached_end: bool = False

            params: dict = {
                "parameter_code": "72019",
                "monitoring_location_id": delineated_sites,
                "limit": 500,
            }

            config = self.config
            # if config.start_date:
            #     params["startDt"] = config.start_dt.date().isoformat()
            # else:
            #     params["startDt"] = "1900-01-01"

            # if config.end_date:
            #     params["endDt"] = config.end_dt.date().isoformat()

            while not reached_end:
                response = self._execute_json_request(
                    url=obs_url,
                    params=params,
                    headers={"X-API-Key": KEY}
                )

                data: list[dict] = response.get("features", [])
                if data:
                    for feature in data:
                        record = {
                            "site_id": feature["properties"]["monitoring_location_id"],
                            "source_parameter_name": "Water level, depth LSD",
                            "value": feature["properties"]["value"],
                            "datetime_measured": feature["properties"]["time"],
                            "source_parameter_units": feature["properties"]["unit_of_measure"]
                        }
                        records.append(record)

                found_next_link: bool = False
                for link in response["links"]:
                    if link["rel"] == "next":
                        obs_url = link["href"]
                        params = None  # next link already has params encoded
                        found_next_link = True
                        break
                
                if not found_next_link  :
                    reached_end = True
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
