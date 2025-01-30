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

from backend.connectors import ISC_SEVEN_RIVERS_BOUNDING_POLYGON
from backend.connectors.mappings import ISC_SEVEN_RIVERS_ANALYTE_MAPPING
from backend.constants import (
    TDS,
    FEET,
    URANIUM,
    SULFATE,
    FLUORIDE,
    CHLORIDE,
    DT_MEASURED,
    DTW_UNITS,
    DTW,
    PARAMETER,
    PARAMETER_VALUE,
    PARAMETER_UNITS,
)
from backend.connectors.isc_seven_rivers.transformer import (
    ISCSevenRiversSiteTransformer,
    ISCSevenRiversWaterLevelTransformer,
    ISCSevenRiversAnalyteTransformer,
)
from backend.source import (
    BaseSource,
    BaseSiteSource,
    BaseWaterLevelSource,
    BaseAnalyteSource,
    get_most_recent,
    get_analyte_search_param,
)


def get_date_range(config):
    params = {}

    def to_milliseconds(dt):
        return int(dt.timestamp() * 1000)

    if config.start_date:
        params["start"] = to_milliseconds(config.start_dt)
    if config.end_date:
        params["end"] = to_milliseconds(config.end_dt)
    return params


def get_datetime(record):
    return datetime.fromtimestamp(record["dateTime"] / 1000)


def _make_url(endpoint):
    return f"https://nmisc-wf.gladata.com/api/{endpoint}"


class ISCSevenRiversSiteSource(BaseSiteSource):
    transformer_klass = ISCSevenRiversSiteTransformer
    bounding_polygon = ISC_SEVEN_RIVERS_BOUNDING_POLYGON

    def __repr__(self):
        return "ISCSevenRiversSiteSource"

    def health(self):
        try:
            self.get_records()
            return True
        except Exception as e:
            print("Failed to get records", e)
            return False

    def get_records(self):
        return self._execute_json_request(
            _make_url("getMonitoringPoints.ashx"),
        )


class ISCSevenRiversAnalyteSource(BaseAnalyteSource):
    transformer_klass = ISCSevenRiversAnalyteTransformer
    _analyte_ids = None

    def __repr__(self):
        return "ISCSevenRiversAnalyteSource"

    def _get_analyte_id(self, analyte):
        """ """
        if self._analyte_ids is None:

            resp = self._execute_json_request(_make_url("getAnalytes.ashx"))
            if resp:
                self._analyte_ids = {r["name"]: r["id"] for r in resp}

        analyte = get_analyte_search_param(analyte, ISC_SEVEN_RIVERS_ANALYTE_MAPPING)
        if analyte:
            return self._analyte_ids.get(analyte)

    def _extract_parameter_record(self, record):
        record[PARAMETER] = self.config.parameter
        record[PARAMETER_VALUE] = record["result"]
        record[PARAMETER_UNITS] = record["units"]
        record[DT_MEASURED] = get_datetime(record)
        return record

    def _extract_most_recent(self, records):
        record = get_most_recent(records, "dateTime")

        return {
            "value": record["result"],
            "datetime": get_datetime(record),
            "units": record["units"],
        }

    def _clean_records(self, records):
        return [r for r in records if r["result"] is not None]

    def _extract_parameter_results(self, records):
        return [r["result"] for r in records]

    def _extract_parameter_units(self, records):
        return [r["units"] for r in records]

    def _extract_parameter_dates(self, records: list) -> list:
        return [get_datetime(r) for r in records]

    def get_records(self, site_record):
        config = self.config
        analyte_id = self._get_analyte_id(config.parameter)
        if analyte_id:
            params = {
                "monitoringPointId": site_record.id,
                "analyteId": analyte_id,
                "start": 0,
                "end": config.now_ms(days=1),
            }
            params.update(get_date_range(config))

            return self._execute_json_request(
                _make_url("getReadings.ashx"), params=params
            )


class ISCSevenRiversWaterLevelSource(BaseWaterLevelSource):
    transformer_klass = ISCSevenRiversWaterLevelTransformer

    def get_records(self, site_record):
        params = {
            "id": site_record.id,
            "start": 0,
            "end": self.config.now_ms(days=1),
        }
        params.update(get_date_range(self.config))

        return self._execute_json_request(
            _make_url("getWaterLevels.ashx"),
            params=params,
        )

    def _clean_records(self, records):
        return [r for r in records if r["depthToWaterFeet"] is not None]

    def _extract_parameter_record(self, record):
        record[PARAMETER] = DTW
        record[PARAMETER_VALUE] = record["depthToWaterFeet"]
        record[PARAMETER_UNITS] = FEET
        record[DT_MEASURED] = get_datetime(record)
        return record

    def _extract_parameter_results(self, records):
        return [
            r["depthToWaterFeet"] for r in records if not r["invalid"] and not r["dry"]
        ]

    def _extract_parameter_dates(self, records: list) -> list:
        return [get_datetime(r) for r in records]

    def _extract_most_recent(self, records):
        record = get_most_recent(records, "dateTime")
        t = get_datetime(record)
        return {"value": record["depthToWaterFeet"], "datetime": t, "units": FEET}


# ============= EOF =============================================
