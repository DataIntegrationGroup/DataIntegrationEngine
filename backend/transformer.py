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
from datetime import datetime

import shapely
from shapely import Point

from backend.constants import (
    MILLIGRAMS_PER_LITER,
    PARTS_PER_MILLION,
    FEET,
    METERS,
    TONS_PER_ACRE_FOOT,
    MICROGRAMS_PER_LITER,
)
from backend.geo_utils import datum_transform
from backend.record import (
    WaterLevelSummaryRecord,
    WaterLevelRecord,
    SiteRecord,
    AnalyteSummaryRecord,
    SummaryRecord,
)


def transform_horizontal_datum(x, y, in_datum, out_datum):
    if in_datum and in_datum != out_datum:
        nx, ny = datum_transform(x, y, in_datum, out_datum)
        return nx, ny, out_datum
    else:
        return x, y, out_datum


def transform_units(e, unit, out_unit):
    try:
        e = float(e)
    except (ValueError, TypeError):
        return None, unit

    if unit != out_unit:
        if unit == "ft" and out_unit == "m":
            e = e * 0.3048
            unit = "m"
        elif unit == "m" and out_unit == "ft":
            e = e * 3.28084
            unit = "ft"
    return e, unit


def convert_units(input_value, input_units, output_units):
    input_value = float(input_value)
    input_units = input_units.lower()
    output_units = output_units.lower()

    mgl = MILLIGRAMS_PER_LITER.lower()
    ugl = MICROGRAMS_PER_LITER.lower()
    ppm = PARTS_PER_MILLION.lower()
    tpaf = TONS_PER_ACRE_FOOT.lower()

    if input_units == output_units:
        return input_value

    if input_units == tpaf and output_units == mgl:
        return input_value * 735.47

    if (
        input_units == mgl
        and output_units == ppm
        or input_units == ppm
        and output_units == mgl
    ):
        return input_value * 1.0

    if input_units == ugl and output_units == mgl:
        return input_value * 0.001

    ft = FEET.lower()
    m = METERS.lower()

    if input_units == ft and output_units == m:
        return input_value * 0.3048
    if input_units == m and output_units == ft:
        return input_value * 3.28084

    print(f"Failed to convert {input_value} {input_units} to {output_units}")
    return input_value


def standardize_datetime(dt):
    if isinstance(dt, tuple):
        dt = [di for di in dt if di is not None]
        dt = " ".join(dt)
    fmt = None
    if isinstance(dt, str):
        dt = dt.strip()
        for fmt in [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S+00:00",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y-%m",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
        ]:
            try:
                dt = datetime.strptime(dt.split(".")[0], fmt)
                break
            except ValueError as e:
                pass
        else:
            raise ValueError(f"Failed to parse datetime {dt}")

    if fmt == "%Y-%m-%d":
        return dt.strftime("%Y-%m-%d"), ""
    elif fmt == "%Y/%m/%d":
        return dt.strftime("%Y-%m-%d"), ""
    elif fmt == "%Y-%m":
        return dt.strftime("%Y-%m"), ""

    tt = dt.strftime("%H:%M:%S")
    if tt == "00:00:00":
        tt = ""
    return dt.strftime("%Y-%m-%d"), tt


class BaseTransformer:
    _cached_polygon = None
    config = None

    def do_transform(self, record, *args, **kw):
        record = self._transform(record, *args, **kw)
        if not record:
            return

        self._post_transform(record, *args, **kw)

        dt = record.get("datetime_measured")
        if dt:
            d, t = standardize_datetime(dt)
            record["date_measured"] = d
            record["time_measured"] = t
        else:
            mrd = record.get("most_recent_datetime")
            if mrd:
                d, t = standardize_datetime(mrd)
                record["date_measured"] = d
                record["time_measured"] = t

        # convert to proper record type
        klass = self._get_record_klass()
        record = klass(record)
        if isinstance(record, (SiteRecord, SummaryRecord)):
            x = float(record.latitude)
            y = float(record.longitude)
            datum = record.horizontal_datum

            lng, lat, datum = transform_horizontal_datum(
                x,
                y,
                datum,
                self.config.output_horizontal_datum,
            )
            record.update(latitude=lat)
            record.update(longitude=lng)
            record.update(horizontal_datum=datum)

            e, eunit = transform_units(
                record.elevation,
                record.elevation_units,
                self.config.output_elevation_units,
            )
            record.update(elevation=e)
            record.update(elevation_units=eunit)

            wd, wdunit = transform_units(
                record.well_depth,
                record.well_depth_units,
                self.config.output_well_depth_units,
            )
            record.update(well_depth=wd)
            record.update(well_depth_units=wdunit)

        return record

    def _transform(self, *args, **kw):
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _transform"
        )

    def _post_transform(self, *args, **kw):
        pass

    def contained(
        self,
        lng,
        lat,
    ):
        config = self.config
        if config.has_bounds():
            if not self._cached_polygon:
                poly = shapely.wkt.loads(config.bounding_wkt())
                self._cached_polygon = poly
            else:
                poly = self._cached_polygon

            pt = Point(lng, lat)
            return poly.contains(pt)

        return True

    def _get_record_klass(self):
        raise NotImplementedError


class SiteTransformer(BaseTransformer):
    def _get_record_klass(self):
        return SiteRecord


class ParameterTransformer(BaseTransformer):
    source_tag = None

    def _get_parameter(self):
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _get_parameter"
        )

    def _transform(self, record, site_record):
        if self.source_tag is None:
            raise NotImplementedError(
                f"{self.__class__.__name__} source_tag is not set"
            )

        rec = {}
        if self.config.output_summary:
            self._transform_most_recents(record)

            p, u = self._get_parameter()
            rec = {
                "source": self.source_tag,
                "id": site_record.id,
                "location": site_record.name,
                "usgs_site_id": site_record.id,
                "latitude": site_record.latitude,
                "longitude": site_record.longitude,
                "elevation": site_record.elevation,
                "elevation_units": site_record.elevation_units,
                "well_depth": site_record.well_depth,
                "well_depth_units": site_record.well_depth_units,
                "parameter": p,
                "parameter_units": u,
            }
        rec.update(record)
        return rec

    def _transform_most_recents(self, record):
        # convert most_recents
        dt, tt = standardize_datetime(record["most_recent_datetime"])
        record["most_recent_date"] = dt
        record["most_recent_time"] = tt
        p, u = self._get_parameter()
        record["most_recent_value"] = convert_units(
            record["most_recent_value"], record["most_recent_units"], u
        )
        record["most_recent_units"] = u


class WaterLevelTransformer(ParameterTransformer):
    def _get_record_klass(self):
        if self.config.output_summary:
            return WaterLevelSummaryRecord
        else:
            return WaterLevelRecord

    def _get_parameter(self):
        return "DTW BGS", self.config.waterlevel_output_units


class AnalyteTransformer(ParameterTransformer):
    def _get_record_klass(self):
        return AnalyteSummaryRecord

    def _get_parameter(self):
        return self.config.analyte, self.config.analyte_output_units


# ============= EOF =============================================
