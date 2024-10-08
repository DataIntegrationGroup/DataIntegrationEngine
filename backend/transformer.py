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
    DT_MEASURED,
)
from backend.geo_utils import datum_transform
from backend.record import (
    WaterLevelSummaryRecord,
    WaterLevelRecord,
    SiteRecord,
    AnalyteSummaryRecord,
    SummaryRecord,
    AnalyteRecord,
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


def convert_units(
    input_value: int | float | str, input_units: str, output_units: str
) -> float:
    """
    Converts the following units for any parameter value:

    Concentration:
    - mg/L to ppm
    - ppm to mg/L
    - ton/ac-ft to mg/L
    - ug/L to mg/L

    length:
    - ft to m
    - m to ft

    Parameters
    --------
    input_value: int | float | str
        The value to convert

    input_units: str
        The input unit of the value

    output_units: str
        The output unit of the value

    Returns
    --------
    float
        The converted value
    """
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

    if input_units == "feet":
        input_units = ft
    if input_units == "meters":
        input_units = m

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
            "%Y",
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
    elif fmt == "%Y":
        return dt.strftime("%Y"), ""

    tt = dt.strftime("%H:%M:%S")
    if tt == "00:00:00":
        tt = ""
    return dt.strftime("%Y-%m-%d"), tt


class BaseTransformer:
    """
    Base class for transforming records. Transformers are used in BaseSiteSource and BaseParameterSource to transform records

    ============================================================================
    Methods With Universal Implementations (Already Implemented)
    ============================================================================
    do_transform
        Transforms a record, site or parameter, into a standardized format

    contained
        Checks if a point is contained within a polygon

    ============================================================================
    Methods That Need to be Implemented For Each SiteTransformer
    ============================================================================
    _transform
        Transforms a record into a standardized format

    _post_transform

    ============================================================================
    Methods Implemented In Each ParameterTransformer (Don't Need To Be Implemented For Each Source)
    ============================================================================
    _transform

    _get_parameter

    ============================================================================
    Methods That Are Implemented In Each ParameterTransformer and SiteTransformer (Don't Need To Be Implemented For Each Source)
    ============================================================================
    _get_record_klass
    """

    _cached_polygon = None
    config = None
    check_contained = True

    # ==========================================================================
    # Methods Already Implemented
    # ==========================================================================

    def do_transform(
        self, inrecord: dict, *args, **kw
    ) -> (
        AnalyteRecord
        | WaterLevelRecord
        | SiteRecord
        | AnalyteSummaryRecord
        | WaterLevelSummaryRecord
        | SummaryRecord
    ):
        """
        Transforms a record, site or parameter, into a standardized format.
        Populating the correct fields is performed in _transform, then the
        record is standardized in this method. This includes standardizing the datetime
        for all record types and geographic/well information for site and summary
        records.

        The fields for a site record are:
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

        The fields for a parameter record are:
        - parameter
        - parameter_value
        - parameter_units
        - date_measured
        - time_measured

        Parameters
        --------
        inrecord: dict
            The record to transform

        Returns
        --------
        AnalyteRecord | WaterLevelRecord | SiteRecord | AnalyteSummaryRecord | WaterLevelSummaryRecord | SummaryRecord
            The transformed and standardized record
        """
        # _transform needs to be implemented by each SiteTransformer
        # _transform is already implemented in each ParameterTransformer
        record = self._transform(inrecord, *args, **kw)
        if not record:
            return

        if not self.contained(record["longitude"], record["latitude"]):
            return

        self._post_transform(record, *args, **kw)

        # standardize datetime
        dt = record.get(DT_MEASURED)
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
        # a record klass holds the original record's data as a dictionary, and has methods to update the record's data and get the record's data
        klass = self._get_record_klass()
        record = klass(record)

        # update the record's geographic information and well data if it is a SiteRecord or SummaryRecord
        # transforms the horizontal datum and lon/lat coordinates to WGS84
        # transforms the elevation and well depth units to the output unit specified in the config
        # transforms the well depth and well depth units to the output unit specified in the config
        if isinstance(record, (SiteRecord, SummaryRecord)):
            y = float(record.latitude)
            x = float(record.longitude)
            input_horizontal_datum = record.horizontal_datum

            output_elevation_units = ""
            well_depth_units = ""
            output_horizontal_datum = "WGS84"
            if self.config:
                output_elevation_units = self.config.output_elevation_units
                well_depth_units = self.config.output_well_depth_units
                output_horizontal_datum = self.config.output_horizontal_datum

            lng, lat, datum = transform_horizontal_datum(
                x,
                y,
                input_horizontal_datum,
                output_horizontal_datum,
            )
            record.update(latitude=lat)
            record.update(longitude=lng)
            record.update(horizontal_datum=datum)

            elevation, elevation_unit = transform_length_units(
                record.elevation,
                record.elevation_units,
                output_elevation_units,
            )
            record.update(elevation=elevation)
            record.update(elevation_units=elevation_unit)

            well_depth, well_depth_unit = transform_length_units(
                record.well_depth,
                record.well_depth_units,
                well_depth_units,
            )
            record.update(well_depth=well_depth)
            record.update(well_depth_units=well_depth_unit)

        return record

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
            if not self._cached_polygon:
                poly = shapely.wkt.loads(config.bounding_wkt())
                self._cached_polygon = poly
            else:
                poly = self._cached_polygon

            pt = Point(lng, lat)
            return poly.contains(pt)

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


class SiteTransformer(BaseTransformer):
    def _get_record_klass(self) -> SiteRecord:
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

    def _get_parameter(self):
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _get_parameter"
        )

    def _transform(self, record, site_record):
        if self.source_tag is None:
            raise NotImplementedError(
                f"{self.__class__.__name__} source_tag is not set"
            )

        rec = {
            "source": self.source_tag,
            "id": site_record.id,
        }

        if self.config.output_summary:
            self._transform_most_recents(record)

            parameter, units = self._get_parameter()
            rec.update(
                {
                    "location": site_record.name,
                    "usgs_site_id": site_record.usgs_site_id,
                    "alternate_site_id": site_record.alternate_site_id,
                    "latitude": site_record.latitude,
                    "longitude": site_record.longitude,
                    "elevation": site_record.elevation,
                    "elevation_units": site_record.elevation_units,
                    "well_depth": site_record.well_depth,
                    "well_depth_units": site_record.well_depth_units,
                    "parameter": parameter,
                    "parameter_units": units,
                }
            )
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
    def _get_record_klass(self) -> WaterLevelRecord | WaterLevelSummaryRecord:
        """
        Returns the WaterLevelRecord class to use for the transformer for
        water level records if config.output_summary is False, otherwise
        returns the WaterLevelSummaryRecord class

        Returns
        --------
        WaterLevelRecord | WaterLevelSummaryRecord
            The record class to use for the transformer
        """
        if self.config.output_summary:
            return WaterLevelSummaryRecord
        else:
            return WaterLevelRecord

    def _get_parameter(self) -> tuple:
        """
        Returns the parameter and units for the water level records

        Returns
        --------
        tuple
            The parameter and units for the water level records
        """
        return "DTW BGS", self.config.waterlevel_output_units


class AnalyteTransformer(ParameterTransformer):
    def _get_record_klass(self) -> AnalyteRecord | AnalyteSummaryRecord:
        """
        Returns the AnalyteRecord class to use for the transformer for
        water level records if config.output_summary is False, otherwise
        returns the AnalyteSummaryRecord class

        Returns
        --------
        AnalyteRecord | AnalyteSummaryRecord
            The record class to use for the transformer
        """
        if self.config.output_summary:
            return AnalyteSummaryRecord
        else:
            return AnalyteRecord

    def _get_parameter(self) -> tuple:
        """
        Returns the parameter and units for the analyte records

        Returns
        --------
        tuple
            The parameter and units for the analyte records
        """
        return self.config.analyte, self.config.analyte_output_units


# ============= EOF =============================================
