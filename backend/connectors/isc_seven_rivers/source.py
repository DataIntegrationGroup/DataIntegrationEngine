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
    FEET,
    DT_MEASURED,
    DTW,
    PARAMETER_NAME,
    PARAMETER_VALUE,
    PARAMETER_UNITS,
    SOURCE_PARAMETER_NAME,
    SOURCE_PARAMETER_UNITS,
    EARLIEST,
    LATEST,
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
    get_terminal_record,
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
    _source_parameter_name = None

    def __repr__(self):
        return "ISCSevenRiversAnalyteSource"

    def _get_analyte_id_and_name(self, analyte):
        """ """
        if self._analyte_ids is None:

            resp = self._execute_json_request(_make_url("getAnalytes.ashx"))
            if resp:
                self._analyte_ids = {r["name"]: r["id"] for r in resp}

        analyte = get_analyte_search_param(analyte, ISC_SEVEN_RIVERS_ANALYTE_MAPPING)
        if analyte:
            id_and_name = {
                "id": self._analyte_ids.get(analyte),
                "name": analyte,
            }
            return id_and_name

    def _extract_parameter_record(self, record):
        record[PARAMETER_NAME] = self.config.parameter
        record[PARAMETER_VALUE] = record["result"]
        record[PARAMETER_UNITS] = self.config.analyte_output_units
        record[DT_MEASURED] = get_datetime(record)
        record[SOURCE_PARAMETER_NAME] = self._source_parameter_name
        record[SOURCE_PARAMETER_UNITS] = record["units"]

        return record

    def _extract_terminal_record(self, records, bookend):
        record = get_terminal_record(records, "dateTime", bookend=bookend)

        return {
            "value": record["result"],
            "datetime": get_datetime(record),
            "source_parameter_units": record["units"],
            "source_parameter_name": self._source_parameter_name,
        }

    def _clean_records(self, records):
        return [r for r in records if r["result"] is not None]

    def _extract_source_parameter_results(self, records):
        return [r["result"] for r in records]

    def _extract_source_parameter_units(self, records):
        return [r["units"] for r in records]

    def _extract_parameter_dates(self, records: list) -> list:
        return [get_datetime(r) for r in records]

    def _extract_source_parameter_names(self, records: list) -> list:
        return [self._source_parameter_name for r in records]

    def get_records(self, site_record):
        config = self.config
        analyte_id_and_name = self._get_analyte_id_and_name(config.parameter)
        if analyte_id_and_name:
            analyte_id = analyte_id_and_name["id"]
            params = {
                "monitoringPointId": site_record.id,
                "analyteId": analyte_id,
                "start": 0,
                "end": config.now_ms(days=1),
            }
            params.update(get_date_range(config))

            if self._source_parameter_name is None:
                self._source_parameter_name = analyte_id_and_name["name"]

            return self._execute_json_request(
                _make_url("getReadings.ashx"), params=params
            )


class ISCSevenRiversWaterLevelSource(BaseWaterLevelSource):
    transformer_klass = ISCSevenRiversWaterLevelTransformer
    _source_parameter_name = "depthToWaterFeet"
    _source_parameter_units = FEET

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
        return [
            r
            for r in records
            if r["depthToWaterFeet"] is not None and not r["invalid"] and not r["dry"]
        ]

    def _extract_parameter_record(self, record):
        record[PARAMETER_NAME] = DTW
        record[PARAMETER_VALUE] = record["depthToWaterFeet"]
        record[PARAMETER_UNITS] = self.config.waterlevel_output_units
        record[DT_MEASURED] = get_datetime(record)
        record[SOURCE_PARAMETER_NAME] = self._source_parameter_name
        record[SOURCE_PARAMETER_UNITS] = self._source_parameter_units
        return record

    def _extract_source_parameter_results(self, records):
        return [r["depthToWaterFeet"] for r in records]

    def _extract_parameter_dates(self, records: list) -> list:
        return [get_datetime(r) for r in records]

    def _extract_source_parameter_names(self, records):
        return [self._source_parameter_name for r in records]

    def _extract_source_parameter_units(self, records):
        return [self._source_parameter_units for r in records]

    def _extract_terminal_record(self, records, bookend):
        record = get_terminal_record(records, "dateTime", bookend=bookend)
        t = get_datetime(record)
        return {
            "value": record["depthToWaterFeet"],
            "datetime": t,
            "source_parameter_units": self._source_parameter_units,
            "source_parameter_name": self._source_parameter_name,
        }


# ============= EOF =============================================
