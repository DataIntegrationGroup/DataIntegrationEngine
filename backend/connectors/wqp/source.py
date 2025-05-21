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
import pprint

import httpx

from backend.connectors import NM_STATE_BOUNDING_POLYGON
from backend.connectors.mappings import WQP_ANALYTE_MAPPING
from backend.constants import (
    PARAMETER_NAME,
    PARAMETER_VALUE,
    PARAMETER_UNITS,
    SOURCE_PARAMETER_NAME,
    SOURCE_PARAMETER_UNITS,
    DT_MEASURED,
    EARLIEST,
    LATEST,
    TDS,
    WATERLEVELS,
    USGS_PCODE_30210,
    USGS_PCODE_70300,
    USGS_PCODE_70301,
    USGS_PCODE_70303,
)
from backend.connectors.wqp.transformer import (
    WQPSiteTransformer,
    WQPAnalyteTransformer,
    WQPWaterLevelTransformer,
)
from backend.source import (
    BaseSiteSource,
    BaseAnalyteSource,
    BaseWaterLevelSource,
    BaseParameterSource,
    make_site_list,
    get_terminal_record,
    get_analyte_search_param,
)


def parse_tsv(text):
    rows = text.split("\n")
    header = rows[0].split("\t")
    return [dict(zip(header, row.split("\t"))) for row in rows[1:]]


def get_date_range(config):
    params = {}
    if config.start_date:
        params["startDateLo"] = config.start_dt.strftime("%m-%d-%Y")
    if config.end_date:
        params["end"] = config.end_dt.strftime("%m-%d-%Y")
    return params


class WQPSiteSource(BaseSiteSource):
    transformer_klass = WQPSiteTransformer
    chunk_size = 50

    bounding_polygon = NM_STATE_BOUNDING_POLYGON

    def __repr__(self):
        return "WQPSiteSource"

    def health(self):
        try:
            r = httpx.get(
                "https://www.waterqualitydata.us/data/Station/search",
                params={"mimeType": "tsv", "siteid": "325754103461301"},
            )
            return r.status_code == 200
        except Exception as e:
            return False

    def get_records(self):
        config = self.config
        params = {
            "mimeType": "tsv",
            "siteType": "Well",
            "sampleMedia": "Water",
            "statecode": "US:35",
        }
        if config.has_bounds():
            params["bBox"] = ",".join([str(b) for b in config.bbox_bounding_points()])
        if not config.sites_only:
            if config.parameter.lower() != "waterlevels":
                params["characteristicName"] = get_analyte_search_param(
                    config.parameter, WQP_ANALYTE_MAPPING
                )
            else:
                # every record with pCode 30210 (depth in m) has a corresponding
                # record with pCode 72019 (depth in ft) but not vice versa
                params["pCode"] = USGS_PCODE_30210

        params.update(get_date_range(config))

        text = self._execute_text_request(
            "https://www.waterqualitydata.us/data/Station/search?", params, timeout=30
        )
        if text:
            return parse_tsv(text)


class WQPParameterSource(BaseParameterSource):

    def _extract_parameter_record(self, record):
        record[PARAMETER_NAME] = self.config.parameter
        record[PARAMETER_VALUE] = record["ResultMeasureValue"]
        record[PARAMETER_UNITS] = self._parameter_units_hook()
        record[DT_MEASURED] = (
            f"{record['ActivityStartDate']} {record['ActivityStartTime/Time']}"
        )
        record[SOURCE_PARAMETER_NAME] = record["CharacteristicName"]
        record[SOURCE_PARAMETER_UNITS] = record["ResultMeasure/MeasureUnitCode"]
        return record

    def _extract_site_records(self, records, site_record):
        return [
            ri for ri in records if ri["MonitoringLocationIdentifier"] == site_record.id
        ]

    def _extract_source_parameter_results(self, records):
        return [ri["ResultMeasureValue"] for ri in records]

    def _clean_records(self, records):
        clean_records = [r for r in records if r["ResultMeasureValue"]]

        if self.config.parameter == TDS and len(clean_records) > 1:
            site_id = clean_records[0]["MonitoringLocationIdentifier"]
            return_records = []
            activity_identifiers = [record["ActivityIdentifier"] for record in records]
            activity_identifiers = list(set(activity_identifiers))
            for activity_identifier in activity_identifiers:
                # get all records for this activity identifier
                ai_records = {
                    record["USGSPCode"]: record
                    for record in records
                    if record["ActivityIdentifier"] == activity_identifier
                }
                if len(ai_records.items()) > 1:
                    if USGS_PCODE_70300 in ai_records.keys():
                        kept_record = ai_records[USGS_PCODE_70300]
                        pcode = USGS_PCODE_70300
                    elif USGS_PCODE_70301 in ai_records.keys():
                        kept_record = ai_records[USGS_PCODE_70301]
                        pcode = USGS_PCODE_70301
                    elif USGS_PCODE_70303 in ai_records.keys():
                        kept_record = ai_records[USGS_PCODE_70303]
                        pcode = USGS_PCODE_70303
                    else:
                        raise ValueError(
                            f"Multiple TDS records found for {site_id} with ActivityIdentifier {activity_identifier} but no 70300, 70301, or 70303 pcodes found."
                        )
                    record_date = kept_record["ActivityStartDate"]
                    self.log(
                        f"Removing duplicates for {site_id} on {record_date} with ActivityIdentifier {activity_identifier}. Keeping record with pcode {pcode}."
                    )
                else:
                    kept_record = list(ai_records.values())[0]
                return_records.append(kept_record)
            return return_records
        else:
            return clean_records

    def _extract_source_parameter_units(self, records):
        return [ri["ResultMeasure/MeasureUnitCode"] for ri in records]

    def _extract_parameter_dates(self, records):
        return [ri["ActivityStartDate"] for ri in records]

    def _extract_source_parameter_names(self, records):
        return [ri["CharacteristicName"] for ri in records]

    def _extract_terminal_record(self, records, bookend):
        record = get_terminal_record(records, "ActivityStartDate", bookend=bookend)
        return {
            "value": record["ResultMeasureValue"],
            "datetime": record["ActivityStartDate"],
            "source_parameter_units": record["ResultMeasure/MeasureUnitCode"],
            "source_parameter_name": record["CharacteristicName"],
        }

    def get_records(self, site_record):
        config = self.config
        sites = make_site_list(site_record)

        params = {
            "siteid": sites,
            "mimeType": "tsv",
        }
        params.update(get_date_range(self.config))

        if config.parameter.lower() != WATERLEVELS:
            params["characteristicName"] = get_analyte_search_param(
                config.parameter, WQP_ANALYTE_MAPPING
            )
        else:
            # every record with pCode 30210 (depth in m) has a corresponding
            # record with pCode 72019 (depth in ft) but not vice versa
            params["pCode"] = "30210"

        params.update(get_date_range(config))

        text = self._execute_text_request(
            "https://www.waterqualitydata.us/data/Result/search?", params, timeout=30
        )
        if text:
            return parse_tsv(text)

    def _parameter_units_hook(self):
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _parameter_units_hook"
        )


class WQPAnalyteSource(WQPParameterSource, BaseAnalyteSource):
    transformer_klass = WQPAnalyteTransformer

    def __repr__(self):
        return "WQPAnalyteSource"

    def _parameter_units_hook(self):
        return self.config.analyte_output_units


# inherit from WQPParameterSource first so that its _extract_souce_parameter_units method is used instead of BaseWaterLevelSource's method
class WQPWaterLevelSource(WQPParameterSource, BaseWaterLevelSource):
    transformer_klass = WQPWaterLevelTransformer

    def __repr__(self):
        return "WQPWaterLevelSource"

    def _parameter_units_hook(self):
        return self.config.waterlevel_output_units


# ============= EOF =============================================
