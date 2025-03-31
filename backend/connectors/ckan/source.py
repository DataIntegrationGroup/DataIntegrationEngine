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
from itertools import groupby

import httpx

from backend.connectors import (
    OSE_ROSWELL_HONDO_BOUNDING_POLYGON,
    OSE_ROSWELL_FORT_SUMNER_BOUNDING_POLYGON,
    OSE_ROSWELL_ROSWELL_BOUNDING_POLYGON,
)
from backend.connectors.ckan import (
    HONDO_RESOURCE_ID,
    FORT_SUMNER_RESOURCE_ID,
    ROSWELL_RESOURCE_ID,
)
from backend.connectors.ckan.transformer import (
    OSERoswellSiteTransformer,
    OSERoswellWaterLevelTransformer,
)
from backend.constants import (
    FEET,
    DTW,
    DTW_UNITS,
    DT_MEASURED,
    PARAMETER_NAME,
    PARAMETER_UNITS,
    PARAMETER_VALUE,
    SOURCE_PARAMETER_NAME,
    SOURCE_PARAMETER_UNITS,
)
from backend.source import (
    BaseSource,
    BaseSiteSource,
    BaseWaterLevelSource,
    get_terminal_record,
)


class CKANSource:
    base_url: str
    _cached_response = None

    def get_records(self, *args, **kw):
        return self._parse_response(self.get_response(*args, **kw))

    def get_response(self):
        if self.base_url is None:
            raise NotImplementedError("base_url is not set")

        if self._cached_response is None:
            self._cached_response = httpx.get(self.base_url, params=self._get_params())

        return self._cached_response

    def _get_params(self):
        return {}

    def _parse_response(self, *args, **kw):
        raise NotImplementedError("parse_response not implemented")


class NMWDICKANSource(CKANSource):
    base_url = "https://catalog.newmexicowaterdata.org/api/3/action/datastore_search"


class OSERoswellSource(NMWDICKANSource):
    resource_id = None

    def __init__(self, resource_id, **kw):
        self.resource_id = resource_id
        super().__init__(**kw)

    def _get_params(self):
        return {
            "resource_id": self.resource_id,
        }


class OSERoswellSiteSource(OSERoswellSource, BaseSiteSource):
    transformer_klass = OSERoswellSiteTransformer

    def __init__(self, resource_id, **kw):
        super().__init__(resource_id, **kw)
        if resource_id == HONDO_RESOURCE_ID:
            self.bounding_polygon = OSE_ROSWELL_HONDO_BOUNDING_POLYGON
        elif resource_id == FORT_SUMNER_RESOURCE_ID:
            self.bounding_polygon = OSE_ROSWELL_FORT_SUMNER_BOUNDING_POLYGON
        elif resource_id == ROSWELL_RESOURCE_ID:
            self.bounding_polygon = OSE_ROSWELL_ROSWELL_BOUNDING_POLYGON

    def __repr__(self):
        return "NMOSERoswellSiteSource"

    def health(self):
        params = self._get_params()
        params["limit"] = 1
        resp = httpx.get(self.base_url, params=params)
        return resp.status_code == 200

    def _parse_response(self, resp):
        records = resp.json()["result"]["records"]
        # group records by site_no
        records = sorted(records, key=lambda x: x["Site_ID"])
        records = [
            next(records)
            for site_id, records in groupby(records, key=lambda x: x["Site_ID"])
        ]
        return records


class OSERoswellWaterLevelSource(OSERoswellSource, BaseWaterLevelSource):
    transformer_klass = OSERoswellWaterLevelTransformer

    def __repr__(self):
        return "NMOSERoswellWaterLevelSource"

    def get_records(self, site_record):
        return self._parse_response(site_record, self.get_response())

    def _parse_response(self, site_record, resp):
        records = resp.json()["result"]["records"]
        return [record for record in records if record["Site_ID"] == site_record.id]

    def _extract_source_parameter_results(self, records):
        return [float(r["DTWGS"]) for r in records]

    def _extract_terminal_record(self, records, bookend):
        record = get_terminal_record(records, tag="Date", bookend=bookend)
        return {
            "value": record["DTWGS"],
            "datetime": record["Date"],
            "source_parameter_units": FEET,
            "source_parameter_name": "DTWGS",
        }

    def _extract_parameter_dates(self, records: list) -> list:
        return [r["Date"] for r in records]

    def _extract_source_parameter_names(self, records):
        return ["" for r in records]

    def _extract_parameter_record(self, record):
        record[PARAMETER_NAME] = DTW
        record[PARAMETER_VALUE] = float(record["DTWGS"])
        record[PARAMETER_UNITS] = self.config.waterlevel_output_units
        record[DT_MEASURED] = record["Date"]
        record[SOURCE_PARAMETER_NAME] = "DTWGS"
        record[SOURCE_PARAMETER_UNITS] = FEET
        return record

    def _clean_records(self, records: list) -> list:
        return [r for r in records if r["DTWGS"] is not None and r["Date"] is not None]


# ============= EOF =============================================
