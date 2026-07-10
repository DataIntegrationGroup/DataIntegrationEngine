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
import os

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
from dlt.sources.helpers.rest_client.paginators import (
    JSONLinkPaginator,
    SinglePagePaginator,
)
from jsonpath_ng.ext import parse

from backend.connectors._dlt import fetch_json_records
from backend.source import (
    BaseWaterLevelSource,
    BaseSiteSource,
    make_site_list,
    get_terminal_record,
)

LIMIT = 50000

# The USGS OGC API paginates with a cursor exposed as a `rel="next"` link.
# dlt's JSONLinkPaginator follows it across every page — this is what fixes the
# old silent truncation (the code used to *refuse* any response advertising a
# next page). Compiled once; the filter selects the href of the next link.
_NEXT_LINK = parse("links[?rel='next'].href")


def _new_paginator() -> JSONLinkPaginator:
    # A paginator instance is stateful (tracks the cursor), so build a fresh one
    # per fetch — do not share across requests.
    return JSONLinkPaginator(next_url_path=_NEXT_LINK)


def _usgs_headers(extra: dict | None = None) -> dict:
    """Request headers for the USGS water data API. Adds the X-API-Key header
    when a USGS_API_KEY is set (env var, e.g. a Dagster+ secret). Without a key
    the API is heavily rate-limited."""
    headers = dict(extra or {})
    key = os.environ.get("USGS_API_KEY")
    if key:
        headers["X-API-Key"] = key
    return headers


class NWISSiteSource(BaseSiteSource):
    chunk_size = 500

    def __init__(self):
        super().__init__(transformer=NWISSiteTransformer())
    bounding_polygon = NM_STATE_BOUNDING_POLYGON
    sites_url: str = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/combined-metadata/items"

    @property
    def tag(self):
        return "nwis"

    def health(self):
        try:
            fetch_json_records(
                self.sites_url,
                params={"limit": 1, "parameter_code": "72019", "site_type_code": "GW", "state_code": "35"},
                data_selector="features",
                paginator=SinglePagePaginator(),
                headers=_usgs_headers(),
            )
            return True
        except Exception:
            return False

    def get_records(self):
        params: dict = {
            "limit": LIMIT,
            "site_type_code": "GW",
        }

        if self.config.has_bounds():
            bbox: tuple = self.config.bbox_bounding_points()
            params["bbox"] = ",".join([str(b) for b in bbox])
        else:
            params["state_code"] = "35"

        if self.config.start_date:
            begin: str = self.config.start_dt.date().isoformat()
            begin = f"{begin}T00:00:00Z"
            params["begin"] = begin
        if self.config.end_date:
            end: str = self.config.end_dt.date().isoformat()
            end = f"{end}T23:59:59Z"
            params["end"] = end

        if not self.config.sites_only:
            params["parameter_code"] = "72019"

        # dlt follows the OGC `rel=next` cursor across every page, so the full
        # result set is returned instead of the old first-page-then-refuse.
        records: list = fetch_json_records(
            self.sites_url,
            params=params,
            data_selector="features",
            paginator=_new_paginator(),
            headers=_usgs_headers(),
        )

        # combined-metadata returns one feature per time series (data_type /
        # statistic), so a location with multiple series (e.g. field
        # measurements + daily mean/max/min) appears several times with the
        # same monitoring_location_id and identical site metadata. Left as-is,
        # read_timeseries iterates each duplicate site and re-emits that well's
        # readings once per series, producing exact-duplicate observations
        # downstream. Keep the first feature per location; readings come only
        # from the field-measurements collection regardless of series.
        deduped: list = []
        seen: set = set()
        for feature in records:
            site_id = feature.get("properties", {}).get("monitoring_location_id")
            if site_id in seen:
                continue
            seen.add(site_id)
            deduped.append(feature)

        removed = len(records) - len(deduped)
        if removed:
            self.warn(f"Dropped {removed} duplicate site time-series features ({len(deduped)} unique locations)")

        return deduped


class NWISWaterLevelSource(BaseWaterLevelSource):
    def __init__(self):
        super().__init__(transformer=NWISWaterLevelTransformer())
    # USGS complex queries allow up to 250 sites to be queried at once
    # https://api.waterdata.usgs.gov/docs/ogcapi/complex-queries
    num_sites = 250
    field_measurements_url = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/field-measurements/items"

    def get_records(self, site_record):
        params: dict = {
            "limit": LIMIT,
            "parameter_code": "72019",
        }

        begin: str = ""
        end: str = ""

        if self.config.start_date:
            begin = self.config.start_dt.date().isoformat()
            begin = f"{begin}T00:00:00Z"
        if self.config.end_date:
            end = self.config.end_dt.date().isoformat()
            end = f"{end}T23:59:59Z"

        if begin and end:
            params["datetime"] = f"{begin}/{end}"
        elif begin:
            params["datetime"] = f"{begin}/.."
        elif end:
            params["datetime"] = f"../{end}"

        records: list = []
        sites: list = make_site_list(site_record)

        # if make_site_list returns a site id as a string, convert to list for consistency with the batch processing logic below
        if isinstance(sites, str):
            sites = [sites]

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

            # POST CQL complex query, paginated: dlt follows the `rel=next`
            # cursor across every page per batch (the old code refused a paged
            # response, truncating large batches).
            features: list[dict] = fetch_json_records(
                self.field_measurements_url,
                params=params,
                json_data=json_data,
                method="POST",
                data_selector="features",
                paginator=_new_paginator(),
                headers=_usgs_headers({"Content-Type": "application/query-cql-json"}),
            )

            standard_features: list[dict] = [self._standardize_record(feature) for feature in features]
            records.extend(standard_features)

        self.log(f"Retrieved {len(records)} records")

        return records
    
    def _standardize_record(self, record: dict) -> dict:
        return {
            "site_id": record["properties"]["monitoring_location_id"],
            "source_parameter_name": "Water level, depth LSD",
            "value": None if record["properties"]["value"] is None else str(record["properties"]["value"]),
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

    def _extract_terminal_record(self, records, position):
        record = get_terminal_record(records, "datetime_measured", position=position)
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
