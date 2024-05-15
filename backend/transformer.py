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

from backend.geo_utils import datum_transform
from backend.record import WaterLevelSummaryRecord, WaterLevelRecord, SiteRecord


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


class BaseTransformer:
    _cached_polygon = None

    def do_transform(self, record, config, *args, **kw):
        record = self.transform(record, config, *args, **kw)
        if not record:
            return

        dt = record.get("datetime_measured")
        if dt:
            d, t = self._standardize_datetime(dt)
            record["date_measured"] = d
            record["time_measured"] = t
        else:
            mrd = record.get("most_recent_datetime")
            if mrd:
                d, t = self._standardize_datetime(mrd)
                record["date_measured"] = d
                record["time_measured"] = t

        # convert to proper record type
        klass = self._get_record_klass(config)
        record = klass(record)

        x = record.latitude
        y = record.longitude
        datum = record.horizontal_datum

        lng, lat, datum = transform_horizontal_datum(
            x,
            y,
            datum,
            config.output_horizontal_datum,
        )
        record.update(latitude=lat)
        record.update(longitude=lng)
        record.update(horizontal_datum=datum)

        e, eunit = transform_units(
            record.elevation, record.elevation_units, config.output_elevation_units
        )
        record.update(elevation=e)
        record.update(elevation_units=eunit)

        wd, wdunit = transform_units(
            record.well_depth, record.well_depth_units, config.output_well_depth_units
        )
        record.update(well_depth=wd)
        record.update(well_depth_units=wdunit)

        return record

    def transform(self, *args, **kw):
        raise NotImplementedError

    def contained(self, lng, lat, config):
        if config.has_bounds():
            if not self._cached_polygon:
                poly = shapely.wkt.loads(config.bounding_wkt())
                self._cached_polygon = poly
            else:
                poly = self._cached_polygon

            pt = Point(lng, lat)
            return poly.contains(pt)

        return True

    def _standardize_datetime(self, dt):
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
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
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

        return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")

    def _get_record_klass(self, config):
        raise NotImplementedError


class SiteTransformer(BaseTransformer):
    def _get_record_klass(self, config):
        return SiteRecord


class WaterLevelTransformer(BaseTransformer):
    def _get_record_klass(self, config):
        if config.output_summary_waterlevel_stats:
            return WaterLevelSummaryRecord
        else:
            return WaterLevelRecord


# ============= EOF =============================================
