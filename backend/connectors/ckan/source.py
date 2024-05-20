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

from backend.connectors.ckan.transformer import (
    OSERoswellSiteTransformer,
    OSERoswellWaterLevelTransformer,
)
from backend.constants import FEET, DTW, DTW_UNITS, DTW_DT_MEASURED
from backend.source import (
    BaseSource,
    BaseSiteSource,
    BaseWaterLevelSource,
    get_most_recent,
)


class CKANSource:
    base_url = None
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

    def __init__(self, resource_id):
        self.resource_id = resource_id
        super().__init__()

    def _get_params(self):
        return {
            "resource_id": self.resource_id,
        }


class OSERoswellSiteSource(OSERoswellSource, BaseSiteSource):
    transformer_klass = OSERoswellSiteTransformer

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

    def get_records(self, parent_record):
        return self._parse_response(parent_record, self.get_response())

    def _parse_response(self, parent_record, resp):
        records = resp.json()["result"]["records"]
        return [record for record in records if record["Site_ID"] == parent_record.id]

    def _extract_parameter_results(self, records):
        return [float(r["DTWGS"]) for r in records]

    def _extract_most_recent(self, records):
        record = get_most_recent(records, tag="Date")
        return {"value": record["DTWGS"], "datetime": record["Date"], "units": FEET}

    def _extract_parameter_record(self, record):
        record[DTW] = float(record["DTWGS"])
        record[DTW_DT_MEASURED] = record["Date"]
        record[DTW_UNITS] = FEET
        return record


# ============= EOF =============================================
