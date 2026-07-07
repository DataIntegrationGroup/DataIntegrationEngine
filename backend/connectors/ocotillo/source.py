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
"""
Ocotillo OGC API - Features connector.

Ocotillo (ocotillo-api.newmexicowaterdata.org/ogcapi) is a pygeoapi/PostGIS
service intended to replace the NMBGMR AMP API. It publishes only pre-aggregated
"latest"/"summary" feature collections and exposes NO raw observation time
series. Consequently this connector produces SUMMARY output only; requesting
time series output logs a warning and yields nothing.

Sites are seeded from the ``water_wells`` collection. Water-level summaries come
from ``water_well_summary`` (count/min/max/latest); chemistry and TDS come from
their respective latest-value collections. See mappings.py for details and for
the list of summary columns that are intentionally null.
"""
import os

from backend.connectors import NM_STATE_BOUNDING_POLYGON
from backend.connectors.ocotillo.mappings import (
    OCOTILLO_ANALYTE_MAPPING,
    SITE_COLLECTION,
    WATERLEVEL_SUMMARY_COLLECTION,
)
from backend.connectors.ocotillo.transformer import (
    OcotilloSiteTransformer,
    OcotilloWaterLevelTransformer,
    OcotilloAnalyteTransformer,
)
from backend.source import (
    BaseSiteSource,
    BaseWaterLevelSource,
    BaseAnalyteSource,
)

TIMEOUT = 15 * 60
DEFAULT_URL = "https://ocotillo-api.newmexicowaterdata.org/ogcapi"


def _base_url():
    return os.getenv("OCOTILLO_URL", DEFAULT_URL).rstrip("/")


def _bbox_params(config):
    """OGC API bbox filter (minx,miny,maxx,maxy) from the configured bounds.
    Rectangular only; finer WKT/county filtering happens in the transformer's
    geographic filter."""
    params = {}
    if config.has_bounds():
        x1, y1, x2, y2 = config.bbox_bounding_points()
        params["bbox"] = f"{x1},{y1},{x2},{y2}"
    return params


def _fetch_all_features(source, collection, params):
    """Page through an OGC API - Features collection, returning all features."""
    url = f"{_base_url()}/collections/{collection}/items"
    limit = 1000
    offset = 0
    features = []
    while True:
        page_params = {"f": "json", "limit": limit, "offset": offset}
        page_params.update(params)
        fc = source._execute_json_request(url, page_params, timeout=TIMEOUT)
        page = fc.get("features", []) if isinstance(fc, dict) else []
        features.extend(page)
        if len(page) < limit:
            break
        offset += limit
    return features


class OcotilloSiteSource(BaseSiteSource):
    chunk_size = 100
    bounding_polygon = NM_STATE_BOUNDING_POLYGON

    def __init__(self):
        super().__init__(transformer=OcotilloSiteTransformer())

    def __repr__(self):
        return "OcotilloSiteSource"

    def health(self):
        try:
            url = f"{_base_url()}/collections/{SITE_COLLECTION}/items"
            resp = self._execute_json_request(url, {"f": "json", "limit": 1})
            return bool(resp)
        except Exception:
            return False

    def get_records(self):
        return _fetch_all_features(self, SITE_COLLECTION, _bbox_params(self.config))


class _OcotilloSummaryParameterSource:
    """Shared behavior for Ocotillo parameter sources.

    The whole (bbox-filtered) collection is fetched once and cached keyed by
    well name; per-site extraction is a dict lookup. Summarization is a direct
    passthrough to the transformer because the API already aggregated the data.
    Time series output is unsupported.
    """

    def _get_collection(self):
        raise NotImplementedError

    def get_records(self, site_record):
        cache = getattr(self, "_feature_cache", None)
        if cache is None:
            cache = {}
            for feature in _fetch_all_features(
                self, self._get_collection(), _bbox_params(self.config)
            ):
                name = feature.get("properties", {}).get("name")
                if name is not None:
                    cache[name] = feature
            self._feature_cache = cache
        return cache

    def _extract_site_records(self, records, site_record):
        feature = records.get(site_record.id)
        return [feature] if feature is not None else []

    def _summarize_records(self, site, cleaned):
        # cleaned is the single pre-aggregated feature for this site.
        return self.transformer.do_transform(cleaned[0], site)

    def read_timeseries(self, site_record):
        self.warn(
            "Ocotillo source supports summary output only; the API exposes no "
            "raw time series. Skipping timeseries output."
        )
        return None


class OcotilloWaterLevelSource(_OcotilloSummaryParameterSource, BaseWaterLevelSource):
    def __init__(self):
        super().__init__(transformer=OcotilloWaterLevelTransformer())

    def __repr__(self):
        return "OcotilloWaterLevelSource"

    def _get_collection(self):
        return WATERLEVEL_SUMMARY_COLLECTION


class OcotilloAnalyteSource(_OcotilloSummaryParameterSource, BaseAnalyteSource):
    def __init__(self):
        super().__init__(transformer=OcotilloAnalyteTransformer())

    def __repr__(self):
        return "OcotilloAnalyteSource"

    def _get_collection(self):
        try:
            collection, _column = OCOTILLO_ANALYTE_MAPPING[self.config.parameter]
        except KeyError:
            raise ValueError(
                f"Ocotillo source does not provide parameter "
                f"{self.config.parameter!r}. Valid: {sorted(OCOTILLO_ANALYTE_MAPPING)}"
            )
        return collection


# ============= EOF =============================================
