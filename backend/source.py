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
from json import JSONDecodeError
from typing import Any, Optional, Union, List, Callable, Dict, cast

import httpx
import shapely.wkt
from shapely import MultiPoint
import time

from backend.constants import (
    FEET,
    DT_MEASURED,
    PARAMETER_NAME,
    PARAMETER_UNITS,
    PARAMETER_VALUE,
    EARLIEST,
    LATEST,
)
from backend.logger import make_logger
from backend.record import (
    ParameterRecord,
    SiteRecord,
    SummaryRecord,
)
from backend.transformer import BaseTransformer
from backend.exceptions import PartialOrNoDataError


# =============================================================================
# Record validation strategies
# =============================================================================

class RecordValidator:
    config: Any = None

    def set_config(self, config) -> None:
        self.config = config

    def validate(self, record: dict) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} must implement validate")


class AnalyteRecordValidator(RecordValidator):
    def validate(self, record: dict) -> None:
        record[PARAMETER_NAME] = self.config.parameter
        for k in (PARAMETER_VALUE, PARAMETER_UNITS, DT_MEASURED):
            if k not in record:
                raise ValueError(f"Invalid record. Missing {k}")


class WaterLevelRecordValidator(RecordValidator):
    def validate(self, record: dict) -> None:
        for k in (PARAMETER_VALUE, PARAMETER_UNITS, DT_MEASURED):
            if k not in record:
                raise ValueError(f"Invalid record. Missing {k}")


class _SubclassValidatorShim(RecordValidator):
    """Shim: delegates to source._validate_record() for subclasses that override it."""
    def __init__(self, source):
        self._source = source

    def set_config(self, config) -> None:
        pass  # source._validate_record uses self.config directly

    def validate(self, record: dict) -> None:
        self._source._validate_record(record)


# =============================================================================
# Record summarization strategy
# =============================================================================

class RecordSummarizer:
    def __init__(self, source):
        self._source = source

    def summarize(self, site, cleaned: list):
        s = self._source
        source_results = s._extract_source_parameter_results(cleaned)
        source_units = s._extract_source_parameter_units(cleaned)
        dates = s._extract_parameter_dates(cleaned)
        source_names = s._extract_source_parameter_names(cleaned)

        kept_items = []
        skipped_items = []
        for source_result, source_unit, date, source_name in zip(
            source_results, source_units, dates, source_names
        ):
            try:
                converted_result, _factor, warning_msg = s.transformer.converter.convert(
                    float(source_result),
                    source_unit,
                    s._get_output_units(),
                    source_name,
                    s.config.parameter,
                    date,
                )
                if warning_msg == "":
                    kept_items.append(converted_result)
                else:
                    s.warn(f"{warning_msg} for {site.id}")
            except (TypeError, ValueError):
                skipped_items.append((site.id, source_result, source_unit))

        if skipped_items:
            s.warn(f"Skipped results because of formatting: {skipped_items}")
        if not kept_items:
            return None

        n = len(kept_items)
        earliest_result = s._extract_earliest_record(cleaned)
        latest_result = s._extract_latest_record(cleaned)
        if not latest_result:
            return None

        rec = {
            "nrecords": n,
            "min": min(kept_items),
            "max": max(kept_items),
            "mean": sum(kept_items) / n,
            "earliest_datetime": earliest_result["datetime"],
            "earliest_value": earliest_result["value"],
            "earliest_source_units": earliest_result["source_parameter_units"],
            "earliest_source_name": earliest_result["source_parameter_name"],
            "latest_datetime": latest_result["datetime"],
            "latest_value": latest_result["value"],
            "latest_source_units": latest_result["source_parameter_units"],
            "latest_source_name": latest_result["source_parameter_name"],
        }
        rec.update(s._summary_extra(cleaned))
        return s.transformer.do_transform(rec, site)


# =============================================================================
# Module-level helpers
# =============================================================================

def make_site_list(site_record: list[SiteRecord] | SiteRecord) -> list | str:
    if isinstance(site_record, list):
        return [r.id for r in site_record]
    return site_record.id


def get_terminal_record(records: list, tag: Union[str, Callable], position: str) -> dict:
    if callable(tag):
        func = tag
    elif "." in tag:
        def func(x):
            for t in tag.split("."):
                x = x[t]
            return x
    else:
        def func(x):
            return x[tag]

    if position == EARLIEST:
        return sorted(records, key=func)[0]
    elif position == LATEST:
        return sorted(records, key=func)[-1]
    raise ValueError(f"Invalid position {position}. Must be either {EARLIEST} or {LATEST}")


def get_analyte_search_param(parameter: str, mapping: dict) -> str:
    try:
        return mapping[parameter]
    except KeyError:
        raise ValueError(
            f"Invalid parameter name {parameter}. Valid parameters are {list(mapping.keys())}"
        )


# =============================================================================
# Base source classes
# =============================================================================

_FETCH_UNSET = object()  # sentinel: site fetch not yet cached


class BaseSource:
    transformer_klass = BaseTransformer  # deprecated: pass transformer= to __init__

    def __init__(self, transformer: Optional[BaseTransformer] = None, http_client: httpx.Client | None = None):
        self.transformer = transformer if transformer is not None else self.transformer_klass()
        self._http_client = http_client if http_client is not None else httpx.Client(timeout=900)
        _l = make_logger(self.__class__.__name__)
        self.log = _l.log
        self.warn = _l.warn
        self.debug = _l.debug
        # Opt-in shared-fetch cache. Off by default, so CLI/API behavior is
        # unchanged. unify_source_both turns it on so a source unified for both
        # summary and timeseries pulls the API only once (see
        # backend/unifier.py:unify_source_both). The two passes issue identical
        # fetches (same parameter/scope/dates), so the second reuses the first.
        self._fetch_cache_enabled = False
        self._records_cache: dict = {}      # site-id key -> get_records() result
        self._sites_cache = _FETCH_UNSET    # BaseSiteSource.read() result

    def _fetch_records(self, site_record):
        """get_records() with optional caching (see _fetch_cache_enabled). Keyed
        by the site ids requested so repeated chunks reuse the same fetch."""
        if not self._fetch_cache_enabled:
            return self.get_records(site_record)
        sites = site_record if isinstance(site_record, list) else [site_record]
        key = tuple(sorted(str(getattr(s, "id", s)) for s in sites))
        if key not in self._records_cache:
            self._records_cache[key] = self.get_records(site_record)
        return self._records_cache[key]

    @property
    def tag(self):
        return self.__class__.__name__.lower()

    def set_config(self, config):
        self.config = config
        self.transformer.set_config(config)
        if hasattr(self, "_validator"):
            self._validator.set_config(config)

    def check(self, *args, **kw):
        return True

    def discover(self, *args, **kw):
        return []

    def _execute_text_request(self, url: str, params: dict | None = None, max_tries: int = 7, **kw) -> str:
        tries, last_err = 0, ""
        while tries < max_tries:
            t0 = time.monotonic()
            try:
                resp = self._http_client.get(url, params=params, **kw)
                elapsed = int((time.monotonic() - t0) * 1000)
                self.log(f"HTTP GET source={self.tag} status={resp.status_code} attempt={tries+1}/{max_tries} elapsed_ms={elapsed} url={url}")
                if resp.status_code == 200:
                    return resp.text
                last_err = f"status {resp.status_code}: {resp.text[:200]}"
                self.warn(f"Received status code {resp.status_code}. Retrying... {tries+1}/{max_tries}")
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.RequestError) as e:
                elapsed = int((time.monotonic() - t0) * 1000)
                last_err = str(e)
                self.warn(f"Request error attempt={tries+1}/{max_tries} elapsed_ms={elapsed} url={url}: {e}")
            tries += 1
            time.sleep(min(2 ** tries, 60))
        self.warn(f"Failed to retrieve records after {max_tries} attempts. Last error: {last_err}")
        raise PartialOrNoDataError(f"Failed to retrieve records after {max_tries} attempts. Last error: {last_err}")

    def _execute_json_request(self, url: str, params: dict | None = None, tag: str | None = None, max_retries: int = 7, **kw) -> dict:
        tries, last_err = 0, ""
        while tries < max_retries:
            t0 = time.monotonic()
            try:
                resp = self._http_client.get(url, params=params, **kw)
                elapsed = int((time.monotonic() - t0) * 1000)
                self.log(f"HTTP GET source={self.tag} status={resp.status_code} attempt={tries+1}/{max_retries} elapsed_ms={elapsed} url={url}")
                if resp.status_code == 200:
                    try:
                        obj = resp.json()
                        if tag and isinstance(obj, dict):
                            return obj[tag]
                        return obj
                    except JSONDecodeError as e:
                        last_err = f"JSONDecodeError: {e}. Response: {resp.text[:200]}"
                        self.warn(f"Invalid JSON response attempt={tries+1}/{max_retries} url={url}: {last_err}")
                else:
                    last_err = f"status {resp.status_code}: {resp.text[:200]}"
                    self.warn(f"Received status code {resp.status_code}. Retrying... {tries+1}/{max_retries}")
            except (httpx.HTTPStatusError, httpx.TimeoutException, httpx.RequestError) as e:
                elapsed = int((time.monotonic() - t0) * 1000)
                last_err = str(e)
                self.warn(f"Request error attempt={tries+1}/{max_retries} elapsed_ms={elapsed} url={url}: {e}")
            tries += 1
            time.sleep(min(2 ** tries, 60))
        self.warn(f"Failed to retrieve records after {max_retries} attempts. Last error: {last_err}")
        raise PartialOrNoDataError(f"Failed to retrieve records after {max_retries} attempts. Last error: {last_err}")

    def read(self, *args, **kw) -> list | None:
        raise NotImplementedError(f"read not implemented by {self.__class__.__name__}")

    def get_records(self, *args, **kw) -> List[Dict]:
        raise NotImplementedError(f"get_records not implemented by {self.__class__.__name__}")

    def health(self) -> bool:
        raise NotImplementedError(f"test not implemented by {self.__class__.__name__}")


class BaseSiteSource(BaseSource):
    chunk_size = 1
    bounding_polygon = None

    @property
    def tag(self):
        return self.__class__.__name__.lower().replace("sitesource", "")

    def generate_bounding_polygon(self):
        records = self.read_sites()
        self.log(str(records[0].latitude))
        mpt = MultiPoint([(r.longitude, r.latitude) for r in records])
        self.log(mpt.convex_hull.buffer(1 / 60.0).wkt)

    def intersects(self, wkt: str) -> bool:
        if self.bounding_polygon:
            wkt = shapely.wkt.loads(wkt)
            return self.bounding_polygon.intersects(wkt)
        return True

    def read(self, *args, **kw) -> List[SiteRecord] | None:
        if self._fetch_cache_enabled and self._sites_cache is not _FETCH_UNSET:
            return self._sites_cache
        self.log("Gathering site records")
        records = self.get_records()
        if records:
            self.log(f"total records={len(records)}")
            result: List[SiteRecord] | None = self._transform_sites(records)
        else:
            self.warn("No site records returned")
            result = None
        if self._fetch_cache_enabled:
            self._sites_cache = result
        return result

    def _transform_sites(self, records: list) -> List[SiteRecord]:
        transformed_records: List[SiteRecord] = []
        for record in records:
            transformed = self.transformer.do_transform(record)
            if transformed:
                site_record = cast(SiteRecord, transformed)
                site_record.chunk_size = self.chunk_size
                transformed_records.append(site_record)
        self.log(f"processed nrecords={len(transformed_records)}")
        return transformed_records

    def chunks(self, records: list, chunk_size: int | None = None) -> list:
        if chunk_size is None:
            chunk_size = self.chunk_size
        if chunk_size > 1:
            return [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]
        return records


class BaseParameterSource(BaseSource):
    name = ""

    def __init__(self, transformer=None, validator: Optional[RecordValidator] = None, http_client: httpx.Client | None = None):
        super().__init__(transformer=transformer, http_client=http_client)
        self._validator = validator if validator is not None else _SubclassValidatorShim(self)
        self._summarizer = RecordSummarizer(self)

    def _extract_earliest_record(self, records: list) -> dict:
        return self._extract_terminal_record(records, position=EARLIEST)

    def _extract_latest_record(self, records: list) -> dict:
        return self._extract_terminal_record(records, position=LATEST)

    def read(self, site_record: SiteRecord | list, use_summarize: bool, start_ind: int, end_ind: int) -> List[ParameterRecord | SummaryRecord] | None:
        # read_summary/read_timeseries return homogeneous lists; cast to the
        # mixed-element list type the signature advertises (List is invariant).
        if use_summarize:
            return cast("List[ParameterRecord | SummaryRecord] | None", self.read_summary(site_record, start_ind, end_ind))
        return cast("List[ParameterRecord | SummaryRecord] | None", self.read_timeseries(site_record))

    def read_summary(self, site_record: SiteRecord | list, start_ind: int, end_ind: int) -> List[SummaryRecord] | None:
        if isinstance(site_record, list):
            self.log(f"Gathering {self.name} summary for {len(site_record)} sites. {start_ind}-{end_ind}")
        else:
            self.log(f"{site_record.id}: Gathering {self.name} data")

        all_records = self._fetch_records(site_record)
        if not all_records:
            names = [str(r.id) for r in site_record] if isinstance(site_record, list) else [str(site_record.id)]
            self.warn(f"{','.join(names)}: No records found")
            return None

        if not isinstance(site_record, list):
            site_record = [site_record]

        ret = []
        for site in site_record:
            site_records = self._extract_site_records(all_records, site)
            if not site_records:
                self.warn(f"{site.id}: No records found")
                continue
            cleaned = self._clean_records(site_records)
            if not cleaned:
                self.warn(f"{site.id} No clean records found")
                continue
            result = self._summarize_records(site, cleaned)
            if result is not None:
                ret.append(result)
        return ret

    def read_timeseries(self, site_record: SiteRecord | list) -> List[ParameterRecord] | None:
        if isinstance(site_record, list):
            self.log(f"Gathering {self.name} timeseries for {len(site_record)} sites")
        else:
            self.log(f"{site_record.id}: Gathering {self.name} data")

        all_records = self._fetch_records(site_record)
        if not all_records:
            names = [str(r.id) for r in site_record] if isinstance(site_record, list) else [str(site_record.id)]
            self.warn(f"{','.join(names)}: No records found")
            return None

        if not isinstance(site_record, list):
            site_record = [site_record]

        ret = []
        for site in site_record:
            site_records = self._extract_site_records(all_records, site)
            if not site_records:
                self.warn(f"{site.id}: No records found")
                continue
            cleaned = self._clean_records(site_records)
            if not cleaned:
                self.warn(f"{site.id} No clean records found")
                continue
            result = self._build_timeseries_records(site, cleaned)
            if result is not None:
                ret.append(result)
        return ret

    def _summarize_records(self, site, cleaned: list):
        return self._summarizer.summarize(site, cleaned)

    def _build_timeseries_records(self, site, cleaned: list):
        records = []
        for record in cleaned:
            transformed = self.transformer.do_transform(self._extract_parameter(record), site)
            if transformed is not None:
                records.append(transformed)
        if not records:
            self.warn(f"{site.id}: No clean records found")
            return None
        return (site, sorted(records, key=self._sort_func))

    def _get_output_units(self) -> str:
        raise NotImplementedError(f"{self.__class__.__name__} Must implement _get_output_units")

    def _extract_site_records(self, records: list[dict], site_record) -> list:
        if site_record.chunk_size == 1:
            return records
        raise NotImplementedError(f"{self.__class__.__name__} Must implement _extract_site_records")

    def _clean_records(self, records: list) -> list:
        return records

    def _summary_extra(self, cleaned: list) -> dict:
        """Extra fields to merge into a site's summary record. Default none;
        sources with a non-normalized source-series link (e.g. st2 SensorThings)
        override to add a source_datastream_link."""
        return {}

    def _extract_terminal_record(self, records, position: str):
        raise NotImplementedError(f"{self.__class__.__name__} Must implement _extract_terminal_record")

    def _extract_source_parameter_units(self, records: list) -> list:
        raise NotImplementedError(f"{self.__class__.__name__} Must implement _extract_source_parameter_units")

    def _extract_parameter_dates(self, records: list) -> list:
        raise NotImplementedError(f"{self.__class__.__name__} Must implement _extract_parameter_dates")

    def _extract_source_parameter_names(self, records: list) -> list:
        raise NotImplementedError(f"{self.__class__.__name__} Must implement _extract_source_parameter_names")

    def _extract_parameter_record(self, record: dict) -> dict:
        raise NotImplementedError(f"{self.__class__.__name__} Must implement _extract_parameter_record")

    def _extract_source_parameter_results(self, records: list) -> list:
        raise NotImplementedError(f"{self.__class__.__name__} Must implement _extract_source_parameter_results")

    def _extract_parameter(self, record: dict) -> dict:
        record = self._extract_parameter_record(record)
        self._validator.validate(record)
        return record

    def _sort_func(self, x):
        return x.date_measured

    # deprecated: override via validator= __init__ arg instead
    def _validate_record(self, record: dict) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} Must implement _validate_record")


class BaseAnalyteSource(BaseParameterSource):
    name = "analyte"

    def __init__(self, transformer=None, http_client: httpx.Client | None = None):
        super().__init__(transformer=transformer, validator=AnalyteRecordValidator(), http_client=http_client)

    def _get_output_units(self):
        return self.config.analyte_output_units


class BaseWaterLevelSource(BaseParameterSource):
    name = "water levels"

    def __init__(self, transformer=None, http_client: httpx.Client | None = None):
        super().__init__(transformer=transformer, validator=WaterLevelRecordValidator(), http_client=http_client)

    def _get_output_units(self):
        return self.config.waterlevel_output_units

    def _extract_source_parameter_units(self, records):
        return [FEET for _ in records]


# ============= EOF =============================================
