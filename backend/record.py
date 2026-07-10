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
from backend.constants import (
    PARAMETER_NAME,
    PARAMETER_VALUE,
    PARAMETER_UNITS,
    SOURCE_PARAMETER_NAME,
    SOURCE_PARAMETER_UNITS,
    CONVERSION_FACTOR,
    APPROVAL_STATUS,
    APPROVAL_STATUS_NORMALIZED,
    QUALIFIER,
    SOURCE_DATASTREAM_LINK,
    FEET,
)


class BaseRecord:
    # Set by the source after transform; not all record types use it.
    chunk_size: int | None = None

    def to_csv(self):
        raise NotImplementedError

    def __init__(self, payload):
        self._payload = payload

    def to_row(self, keys=None):
        if keys is None:
            keys = self.keys

        return [self._get_sigfig_formatted_value(k) for k in keys]

    def to_dict(self, keys=None):
        if keys is None:
            keys = self.keys
        return {k: self._get_sigfig_formatted_value(k) for k in keys}

    def update(self, **kw):
        self._payload.update(kw)

    def _get_sigfig_formatted_value(self, attr):
        v = self.__getattr__(attr)

        field_sigfigs = [
            ("elevation", 2),
            ("well_depth", 2),
            ("latitude", 6),
            ("longitude", 6),
            ("min", 2),
            ("max", 2),
            ("mean", 2),
        ]

        # both analyte and water level tables have the same fields, but the
        # rounding should only occur for water level tables
        if self._payload.get("record_type") == "waterlevels":
            field_sigfigs.append((PARAMETER_VALUE, 2))

        for field, sigfigs in field_sigfigs:
            if v is not None and field == attr:
                try:
                    v = round(v, sigfigs)
                except TypeError as e:
                    raise TypeError(f"rounding failed for field={field!r} attr={attr!r}") from e
                break
        return v

    def __getattr__(self, attr):
        # Guard against recursion when the instance has no _payload yet — e.g.
        # during unpickling, when pickle probes getattr(obj, "__setstate__")
        # before __dict__ is restored. Reading via __dict__ (not self._payload)
        # avoids re-entering __getattr__, and dunder lookups raise cleanly so
        # pickle falls back to its default state restore.
        if attr.startswith("__") and attr.endswith("__"):
            raise AttributeError(attr)
        payload = self.__dict__.get("_payload")
        if payload is None:
            raise AttributeError(attr)
        v = payload.get(attr)
        if v is None and self.defaults:
            v = self.defaults.get(attr)
        return v


class ParameterRecord(BaseRecord):
    keys: tuple = (
        "source",
        "id",
        PARAMETER_NAME,
        PARAMETER_VALUE,
        PARAMETER_UNITS,
        "date_measured",
        "time_measured",
        SOURCE_PARAMETER_NAME,
        SOURCE_PARAMETER_UNITS,
        CONVERSION_FACTOR,
        # per-observation source quality metadata: the provider's raw status,
        # its normalized form (approved/provisional/unknown), and the qualifier.
        # None/unknown when a source doesn't report it.
        APPROVAL_STATUS,
        APPROVAL_STATUS_NORMALIZED,
        QUALIFIER,
    )

    defaults: dict = {}


class SummaryRecord(BaseRecord):
    keys: tuple = (
        "source",
        "id",
        "name",
        "usgs_site_id",
        "alternate_site_id",
        "latitude",
        "longitude",
        "horizontal_datum",
        "elevation",
        "elevation_units",
        "well_depth",
        "well_depth_units",
        PARAMETER_NAME,
        PARAMETER_UNITS,
        "nrecords",
        "min",
        "max",
        "mean",
        "earliest_date",
        "earliest_time",
        "earliest_value",
        "earliest_units",
        "latest_date",
        "latest_time",
        "latest_value",
        "latest_units",
        SOURCE_DATASTREAM_LINK,
    )
    defaults: dict = {}


class SiteRecord(BaseRecord):
    keys: tuple = (
        "source",
        "id",
        "name",
        "latitude",
        "longitude",
        "elevation",
        "elevation_units",
        "horizontal_datum",
        "vertical_datum",
        "usgs_site_id",
        "alternate_site_id",
        "formation",
        "aquifer",
        "well_depth",
        "well_depth_units",
    )

    defaults: dict = {
        "source": None,
        "id": None,
        "name": "",
        "latitude": None,
        "longitude": None,
        "elevation": None,
        "elevation_units": FEET,
        "horizontal_datum": "WGS84",
        "vertical_datum": "",
        "usgs_site_id": "",
        "alternate_site_id": "",
        "formation": "",
        "aquifer": "",
        "well_depth": None,
        "well_depth_units": FEET,
    }


# ============= EOF =============================================
