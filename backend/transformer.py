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
import click
import pprint
from datetime import datetime, date, timedelta

import shapely
from shapely import Point

from backend.constants import (
    MILLIGRAMS_PER_LITER,
    PARTS_PER_MILLION,
    PARTS_PER_BILLION,
    FEET,
    METERS,
    TONS_PER_ACRE_FOOT,
    MICROGRAMS_PER_LITER,
    DT_MEASURED,
    DTW,
)
from backend.geo_utils import datum_transform, ALLOWED_DATUMS
from backend.logging import Loggable
from backend.record import (
    WaterLevelSummaryRecord,
    WaterLevelRecord,
    SiteRecord,
    AnalyteSummaryRecord,
    SummaryRecord,
    AnalyteRecord,
)

logger = Loggable()


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
    input_value: int | float | str,
    input_units: str,
    output_units: str,
    source_parameter_name: str,
    die_parameter_name: str,
    dt: str = None,
) -> tuple[float, float, str]:
    """
    Converts the following units for any parameter value:

    Concentration:
    - mg/L to ppm
    - ppm to mg/L
    - ton/ac-ft to mg/L
    - ug/L to mg/L
    - mg/L CaCO3 to mg/L
    - mg/L N to mg/L (for NO3)

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

    source_parameter_name: str
        The name of the parameter from the source

    die_parameter_name: str
        The name of the parameter as it is called in the DIE

    dt: str
        The date of the record

    Returns
    --------
    tuple[float, float, str]
        converted value, conversion factor, warning message
    """
    warning = ""
    conversion_factor = None

    input_value = float(input_value)
    input_units = input_units.strip().lower()
    output_units = output_units.strip().lower()
    source_parameter_name = source_parameter_name.strip().lower()
    die_parameter_name = die_parameter_name.strip().lower()

    mgl = MILLIGRAMS_PER_LITER.lower()
    ugl = MICROGRAMS_PER_LITER.lower()
    ppm = PARTS_PER_MILLION.lower()
    ppb = PARTS_PER_BILLION.lower()
    tpaf = TONS_PER_ACRE_FOOT.lower()
    ft = FEET.lower()
    m = METERS.lower()

    """
    Each output_unit block needs a check for if input_units == output_units.

    This should go at the end of each block because there are some cases where
    the input_units == output_units, but the conversion factor is not 1 due to
    the source_parameter_name (e.g. nitrate as n).
    """
    if die_parameter_name == "ph":
        conversion_factor = 1
    elif output_units == mgl:
        if input_units in ["mg/l caco3", "mg/l caco3**"]:
            if die_parameter_name == "bicarbonate":
                conversion_factor = 1.22
            elif die_parameter_name == "calcium":
                conversion_factor = 0.4
            elif die_parameter_name == "carbonate":
                conversion_factor = 0.6
        elif input_units == "mg/l as n":
            conversion_factor = 4.427
        elif input_units in ["mg/l asno3", "mg/l as no3"]:
            conversion_factor = 1
        elif input_units == "ug/l as n":
            conversion_factor = 0.004427
        elif input_units == "pci/l":
            conversion_factor = 0.00149
        elif input_units in (ugl, ppb):
            conversion_factor = 0.001
        elif input_units == tpaf:
            conversion_factor = 735.47
        elif input_units == ppm:
            conversion_factor = 1
        elif input_units == output_units:
            if source_parameter_name in ["nitrate as n", "nitrate (as n)"]:
                conversion_factor = 4.427
            else:
                conversion_factor = 1
    elif output_units == ft:
        if input_units in [m, "meters"]:
            conversion_factor = 3.28084
        elif input_units in [ft, "feet"]:
            conversion_factor = 1
    elif output_units == m:
        if input_units in [ft, "feet"]:
            conversion_factor = 0.3048
        elif input_units in [m, "meters"]:
            conversion_factor = 1

    if conversion_factor:
        return input_value * conversion_factor, conversion_factor, warning
    else:
        warning = f"Failed to convert {input_value} {input_units} {source_parameter_name} (source) to {output_units} {die_parameter_name} (die) on {dt}"
        return input_value, conversion_factor, warning


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


class BaseTransformer(Loggable):
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
        - parameter_name
        - parameter_value
        - parameter_units
        - date_measured
        - time_measured
        - source_parameter_name
        - source_parameter_units
        - conversion_factor

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

        # ensure that a site or summary record is contained within the boundaing polygon
        if "longitude" in record and "latitude" in record:
            if not self.contained(record["longitude"], record["latitude"]):
                self.warn(
                    f"Skipping site {record['id']}. It is not within the defined geographic bounds"
                )
                return

        self._post_transform(record, *args, **kw)

        # standardize datetime
        dt = record.get(DT_MEASURED)
        if dt:
            d, t = standardize_datetime(dt, record["id"])
            record["date_measured"] = d
            record["time_measured"] = t
        else:
            mrd = record.get("most_recent_datetime")
            if mrd:
                d, t = standardize_datetime(mrd, record["id"])
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

            if x == 0 or y == 0:
                self.warn(f"Skipping site {record.id}. Latitude or Longitude is 0")
                return None

            input_horizontal_datum = record.horizontal_datum

            if input_horizontal_datum not in ALLOWED_DATUMS:
                self.warn(
                    f"Skipping site {record.id}. Datum {input_horizontal_datum} cannot be processed"
                )
                return None

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

        # update the units to the output unit for analyte records
        # this is done after converting the units to the output unit for the analyte records
        # convert the parameter value to the output unit specified in the config
        elif isinstance(record, (AnalyteRecord, WaterLevelRecord)):
            if isinstance(record, AnalyteRecord):
                output_units = self.config.analyte_output_units
            else:
                output_units = self.config.waterlevel_output_units

            source_result = record.parameter_value
            source_unit = record.source_parameter_units
            dt = record.date_measured
            source_name = record.source_parameter_name
            conversion_factor = None  # conversion factor will remain None if record is kept for time series and cannot be converted, such as non-detects
            warning_msg = ""
            try:
                converted_result, conversion_factor, warning_msg = convert_units(
                    float(source_result),
                    source_unit,
                    output_units,
                    source_name,
                    self.config.parameter,
                    dt,
                )
                if warning_msg != "":
                    msg = f"{warning_msg} for {record.id}"
                    self.warn(msg)
            except TypeError:
                msg = f"Keeping {source_result} for {record.id} on {record.date_measured} for time series data"
                self.warn(msg)
                converted_result = source_result
            except ValueError:
                msg = f"Keeping {source_result} for {record.id} on {record.date_measured} for time series data"
                self.warn(msg)
                converted_result = source_result

            if warning_msg == "":
                record.update(conversion_factor=conversion_factor)
                record.update(parameter_value=converted_result)
            else:
                record = None

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
            self._transform_most_recents(record, site_record.id)

            parameter, units = self._get_parameter_name_and_units()
            rec.update(
                {
                    "location": site_record.name,
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

    def _transform_most_recents(self, record, site_id):
        # convert most_recents
        dt, tt = standardize_datetime(record["most_recent_datetime"], site_id)
        record["most_recent_date"] = dt
        record["most_recent_time"] = tt
        parameter_name, unit = self._get_parameter_name_and_units()

        converted_most_recent_value, conversion_factor, warning_msg = convert_units(
            record["most_recent_value"],
            record["most_recent_source_units"],
            unit,
            record["most_recent_source_name"],
            parameter_name,
            dt,
        )

        # all failed conversions are skipped and handled in source.read(), so no need to duplicate here
        record["most_recent_value"] = converted_most_recent_value
        record["most_recent_units"] = unit


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
