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
from backend.geo_utils import datum_transform


def transform_horizontal_datum(x, y, in_datum, out_datum):
    if in_datum != out_datum:
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
        if unit == 'ft' and out_unit == 'm':
            e = e * 0.3048
            unit = 'm'
        elif unit == 'm' and out_unit == 'ft':
            e = e * 3.28084
            unit = 'ft'
    return e, unit


class BaseTransformer:
    def do_transform(self, record, config):
        record = self.transform(record, config)
        x = record.latitude
        y = record.longitude
        datum = record.horizontal_datum
        lng, lat, datum = transform_horizontal_datum(x, y,
                                                     datum,
                                                     config.output_horizontal_datum,
                                                     )
        record.update(latitude=lat)
        record.update(longitude=lng)
        record.update(horizontal_datum=datum)

        e, eunit = transform_units(record.elevation, record.elevation_unit, config.output_elevation_unit)
        record.update(elevation=e)
        record.update(elevation_unit=eunit)

        wd, wdunit = transform_units(record.well_depth, record.well_depth_unit, config.output_well_depth_unit)
        record.update(well_depth=wd)
        record.update(well_depth_unit=wdunit)

        return record

    def transform(self, *args, **kw):
        raise NotImplementedError

# ============= EOF =============================================
