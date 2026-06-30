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
from datetime import datetime, date, timedelta

import shapely
from shapely import Point

from backend.bounding_polygons import NM_BOUNDARY_BUFFERED
from backend.constants import (
    FEET,
    METERS,
    DT_MEASURED,
    DTW,
    EARLIEST,
    LATEST,
    WATERLEVELS,
    ANALYTES,
)
from backend.geo_utils import datum_transform, ALLOWED_DATUMS
from backend.logger import make_logger
from backend.record import (
    ParameterRecord,
    SiteRecord,
    SummaryRecord,
)


def transform_horizontal_datum(
    x: int | float, y: int | float, in_datum: str, out_datum: str
) -> tuple:
    """
    Returns the transformed x, y coordinates and the output datum if the input datum is not the same as the output datum.
    Otherwise returns the original x, y coordinates and the output datum.

    Parameters
    --------
    x: int | float
        The x coordinate to transform

    y: int | float
        The y coordinate to transform

    in_datum: str
        The input datum for the coordinataes

    out_datum: str
        The output datum for the coordinates

    Returns
    --------
    tuple
        The transformed x, y coordinates and the output datum if the input datum is not the same as the output datum.
        Otherwise returns the original x, y coordinates and the output datum.
    """
    if in_datum and in_datum != out_datum:
        nx, ny = datum_transform(x, y, in_datum, out_datum)
        return nx, ny, out_datum
    else:
        return x, y, out_datum


def transform_length_units(
    value: str | int | float, in_unit: str, out_unit: str
) -> tuple:
    """
    Transforms feet to meters or meters to feet.

    Parameters
    --------
    value: str | int | float
        The value to transform

    in_unit: str
        The input unit of the value, should be either "feet" or "meters"

    out_unit: str
        The output unit of the value, should be either "feet" or "meters"

    Returns
    --------
    tuple
        The transformed value and the output unit if the input unit is not the same as the output unit.
        Otherwise returns the original value and the output unit.
    """
    try:
        value = float(value)
    except (ValueError, TypeError):
        return None, out_unit

    if in_unit != out_unit:
        if in_unit.lower() == "feet":
            in_unit = FEET
        if in_unit.lower() == "meters":
            in_unit = METERS

        if in_unit == FEET and out_unit == METERS:
            value = value * 0.3048
            unit = METERS
        elif in_unit == METERS and out_unit == FEET:
            value = value * 3.28084
            unit = FEET
    return value, out_unit


def standardize_datetime(dt, record_id):
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
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S+00:00",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y-%m",
            "%Y",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
            "%m/%d/%Y",
        ]:
            try:
                dt = datetime.strptime(dt.split(".")[0], fmt)
                break
            except ValueError as e:
                try:
                    # Ft Sumner (OSE Roswell) reports Excel date numbers
                    num_days_to_add = int(dt)
                    base_date = date(1900, 1, 1)
                    dt = base_date + timedelta(days=num_days_to_add)
                    break
                except ValueError as e:
                    pass
        else:
            raise ValueError(f"Failed to parse datetime {dt} for {record_id}")

    if fmt == "%Y-%m-%d":
        return dt.strftime("%Y-%m-%d"), ""
    elif fmt == "%Y/%m/%d":
        return dt.strftime("%Y-%m-%d"), ""
    elif fmt == "%Y-%m":
        return dt.strftime("%Y-%m"), ""
    elif fmt == "%Y":
        return dt.strftime("%Y"), ""

    tt = dt.strftime("%H:%M:%S")
    if tt == "00:00:00":
        tt = ""
    return dt.strftime("%Y-%m-%d"), tt


class BaseTransformer:
    _polygon_cache: dict = {}
    check_contained = True

    def __init__(self, converter=None):
        from backend.converter import StandardUnitConverter
        self.converter = converter if converter is not None else StandardUnitConverter()
        _l = make_logger(self.__class__.__name__)
        self.log = _l.log
        self.warn = _l.warn
        self.debug = _l.debug

    def set_config(self, config):
        """
        Sets the config for the transformer. Called in BaseSource.set_config()
        to set the config for both the source and the transformer.

        Parameters
        --------
        config: Config
            The config to set for the transformer
        """
        self.config = config

    def do_transform(
        self, inrecord: dict, *args, **kw
    ) -> ParameterRecord | SiteRecord | SummaryRecord | None:
        transformed_record = self._transform(inrecord, *args, **kw)
        if not transformed_record:
            return None
        if not self._apply_geographic_filter(transformed_record):
            return None
        self._post_transform(transformed_record, *args, **kw)
        self._standardize_datetime(transformed_record)
        klass = self._get_record_klass()
        transformed_record["record_type"] = self._get_record_type()
        klassed_record = klass(transformed_record)
        if isinstance(klassed_record, (SiteRecord, SummaryRecord)):
            klassed_record = self._apply_datum_transform(klassed_record)
            if klassed_record is None:
                return None
            klassed_record = self._apply_elevation_transform(klassed_record)
            klassed_record = self._apply_well_depth_transform(klassed_record)
        elif klassed_record.record_type in (ANALYTES, WATERLEVELS):
            klassed_record = self._apply_unit_conversion(klassed_record)
        return klassed_record

    def _apply_geographic_filter(self, transformed_record: dict) -> bool:
        if "longitude" not in transformed_record or "latitude" not in transformed_record:
            return True
        if not self.contained(transformed_record["longitude"], transformed_record["latitude"]):
            self.warn(f"Skipping site {transformed_record['id']}. It is not within the defined geographic bounds")
            return False
        return True

    def _standardize_datetime(self, transformed_record: dict) -> None:
        dt = transformed_record.get(DT_MEASURED)
        if dt:
            d, t = standardize_datetime(dt, transformed_record["id"])
        else:
            mrd = transformed_record.get("latest_datetime")
            if not mrd:
                return
            d, t = standardize_datetime(mrd, transformed_record["id"])
        transformed_record["date_measured"] = d
        transformed_record["time_measured"] = t

    def _apply_datum_transform(self, klassed_record):
        y = float(klassed_record.latitude)
        x = float(klassed_record.longitude)
        if x == 0 or y == 0:
            self.warn(f"Skipping site {klassed_record.id}. Latitude or Longitude is 0")
            return None
        if not (-180 <= x <= 180) or not (-90 <= y <= 90):
            self.warn(f"Skipping site {klassed_record.id}. Coordinates out of range: lng={x}, lat={y}")
            return None
        input_datum = klassed_record.horizontal_datum
        if input_datum not in ALLOWED_DATUMS:
            self.warn(f"Skipping site {klassed_record.id}. Datum {input_datum} cannot be processed")
            return None
        output_datum = self.config.output_horizontal_datum if self.config else "WGS84"
        lng, lat, datum = transform_horizontal_datum(x, y, input_datum, output_datum)
        if not self.in_nm(lng, lat):
            self.warn(f"Skipping site {klassed_record.id}. Coordinates {x}, {y} with datum {input_datum} are not within 25km of New Mexico")
            return None
        klassed_record.update(latitude=lat, longitude=lng, horizontal_datum=datum)
        return klassed_record

    def _apply_elevation_transform(self, klassed_record):
        units = self.config.output_elevation_units if self.config else ""
        elevation, unit = transform_length_units(klassed_record.elevation, klassed_record.elevation_units, units)
        klassed_record.update(elevation=elevation, elevation_units=unit)
        return klassed_record

    def _apply_well_depth_transform(self, klassed_record):
        units = self.config.output_well_depth_units if self.config else ""
        depth, unit = transform_length_units(klassed_record.well_depth, klassed_record.well_depth_units, units)
        klassed_record.update(well_depth=depth, well_depth_units=unit)
        return klassed_record

    def _apply_unit_conversion(self, klassed_record):
        output_units = (
            self.config.analyte_output_units
            if klassed_record.record_type == ANALYTES
            else self.config.waterlevel_output_units
        )
        source_result = klassed_record.parameter_value
        source_unit = klassed_record.source_parameter_units
        source_name = klassed_record.source_parameter_name
        dt = klassed_record.date_measured
        warning_msg = ""
        conversion_factor = None
        try:
            converted_result, conversion_factor, warning_msg = self.converter.convert(
                float(source_result), source_unit, output_units, source_name, self.config.parameter, dt
            )
            if warning_msg:
                self.warn(f"{warning_msg} for {klassed_record.id}")
        except (TypeError, ValueError):
            self.warn(f"Keeping {source_result} for {klassed_record.id} on {dt} for time series data")
            converted_result = source_result
        if warning_msg == "":
            klassed_record.update(conversion_factor=conversion_factor, parameter_value=converted_result)
            return klassed_record
        return None

    def in_nm(self, lng: float | int | str, lat: float | int | str) -> bool:
        """
        Returns True if the point is in New Mexico, otherwise returns False

        Parameters
        --------
        lng: float | int | str
            The longitude of the point

        lat: float | int | str
            The latitude of the point

        Returns
        --------
        bool
            True if the point is in New Mexico, otherwise False
        """
        point = Point(lng, lat)
        if NM_BOUNDARY_BUFFERED.contains(point):
            return True
        else:
            return False

    def contained(
        self,
        lng: float | int | str,
        lat: float | int | str,
    ) -> bool:
        """
        Returns True if the point is contained within the polygon defined by the bounding_wkt in the config, otherwise returns False

        Parameters
        --------
        lng: float | int | str
            The longitude of the point

        lat: float | int | str
            The latitude of the point

        Returns
        --------
        bool
            True if the point is contained within the polygon defined by the bounding_wkt in the config, otherwise False
        """
        config = self.config
        if config and config.has_bounds() and self.check_contained:
            wkt = config.bounding_wkt()
            if wkt not in BaseTransformer._polygon_cache:
                BaseTransformer._polygon_cache[wkt] = shapely.wkt.loads(wkt)
            poly = BaseTransformer._polygon_cache[wkt]
            return poly.contains(Point(lng, lat))

        return True

    # ==========================================================================
    # Methods That Need to be Implemented For Each SiteTransformer
    # ==========================================================================

    def _transform(self, *args, **kw) -> dict:
        """
        Transforms a record into a standardized format. This method needs to be implemented by each SiteTransformer

        For a site transformer, the output record has the following fields:
        - source
        - id
        - name
        - latitude
        - longitude
        - elevation
        - elevation_units
        - horizontal_datum
        - vertical_datum
        - usgs_site_id (optional)
        - alternate_site_id (optional)
        - aquifer (optional)
        - well_depth (optional)
        - well_depth_units (optional)

        For a parameter transformer, the output record has the following fields:
        - parameter
        - parameter_value
        - parameter_units
        - date_measured
        - time_measured

        If output_summary is True, the output record has the following fields:
        - source
        - id
        - location
        - usgs_site_id
        - alternate_site_id
        - latitude
        - longitude
        - elevation
        - elevation_units
        - well_depth
        - well_depth_units
        - parameter
        - parameter_units

        Parameters
        --------
        If a site transformer:
            record: dict
                The record to transform into the standardized format

        If a parameter transformer:
            record: dict
                The record to transform into the standardized format

            site_record: dict
                The site record associated with the parameter record

        Returns
        --------
        dict
            The record with the standard fields added and populated
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _transform"
        )

    def _post_transform(self, *args, **kw):
        pass

    # ==========================================================================
    # Methods That Are Implemented In Each ParameterTransformer and SiteTransformer (Don't Need To Be Implemented For Each Source)
    # ==========================================================================

    def _get_record_klass(self):
        raise NotImplementedError

    def _get_record_type(self) -> str | None:
        return None


class SiteTransformer(BaseTransformer):
    def _get_record_klass(self) -> type[SiteRecord]:
        """
        Returns the SiteRecord class to use for the transformer for all site records

        Returns
        --------
        SiteRecord
            The record class to use for the transformer
        """
        return SiteRecord


class ParameterTransformer(BaseTransformer):
    source_tag: str

    def _get_parameter_name_and_units(self):
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _get_parameter_name_and_units"
        )

    def _transform(self, record, site_record):
        if self.source_tag is None:
            raise NotImplementedError(
                f"{self.__class__.__name__} source_tag is not set"
            )

        rec = {}

        if self.config.output_summary:
            self._transform_earliest_record(record, site_record.id)
            self._transform_latest_record(record, site_record.id)

            parameter, units = self._get_parameter_name_and_units()
            rec.update(
                {
                    "name": site_record.name,
                    "usgs_site_id": site_record.usgs_site_id,
                    "alternate_site_id": site_record.alternate_site_id,
                    "latitude": site_record.latitude,
                    "longitude": site_record.longitude,
                    "horizontal_datum": site_record.horizontal_datum,
                    "elevation": site_record.elevation,
                    "elevation_units": site_record.elevation_units,
                    "well_depth": site_record.well_depth,
                    "well_depth_units": site_record.well_depth_units,
                    "parameter_name": parameter,
                    "parameter_units": units,
                }
            )
        rec.update(record)

        """
        Some analyte records, like BOR, have a field called "id" that is the record's ID.
        To allow for the record's "id" to be the site's "id", the record's "id" needs to be updated at the end.
        """
        source_id = {
            "source": self.source_tag,
            "id": site_record.id,
        }
        rec.update(source_id)
        return rec

    def _transform_terminal_record(self, record, site_id, position):
        if position == EARLIEST:
            datetime_key = "earliest_datetime"
            date_key = "earliest_date"
            time_key = "earliest_time"
            value_key = "earliest_value"
            unit_key = "earliest_units"
            source_units_key = "earliest_source_units"
            source_name_key = "earliest_source_name"
        elif position == LATEST:
            datetime_key = "latest_datetime"
            date_key = "latest_date"
            time_key = "latest_time"
            value_key = "latest_value"
            unit_key = "latest_units"
            source_units_key = "latest_source_units"
            source_name_key = "latest_source_name"

        dt, tt = standardize_datetime(record[datetime_key], site_id)
        parameter_name, unit = self._get_parameter_name_and_units()
        converted_value, conversion_factor, warning_msg = self.converter.convert(
            record[value_key],
            record[source_units_key],
            unit,
            record[source_name_key],
            parameter_name,
            dt,
        )

        # all failed conversions are skipped and handled in source.read(), so no need to duplicate here
        record[date_key] = dt
        record[time_key] = tt
        record[value_key] = converted_value
        record[unit_key] = unit

    def _transform_earliest_record(self, record, site_id):
        self._transform_terminal_record(record, site_id, EARLIEST)

    def _transform_latest_record(self, record, site_id):
        self._transform_terminal_record(record, site_id, LATEST)


class WaterLevelTransformer(ParameterTransformer):
    def _get_record_klass(self) -> type[ParameterRecord] | type[SummaryRecord]:
        return SummaryRecord if self.config.output_summary else ParameterRecord

    def _get_record_type(self) -> str:
        return WATERLEVELS

    def _get_parameter_name_and_units(self) -> tuple:
        """
        Returns the parameter and units for the water level records

        Returns
        --------
        tuple
            The parameter and units for the water level records
        """
        return DTW, self.config.waterlevel_output_units


class AnalyteTransformer(ParameterTransformer):
    def _get_record_klass(self) -> type[ParameterRecord] | type[SummaryRecord]:
        return SummaryRecord if self.config.output_summary else ParameterRecord

    def _get_record_type(self) -> str:
        return ANALYTES

    def _get_parameter_name_and_units(self) -> tuple:
        """
        Returns the parameter and units for the analyte records

        Returns
        --------
        tuple
            The parameter and units for the analyte records
        """
        return self.config.parameter, self.config.analyte_output_units


# ============= EOF =============================================
