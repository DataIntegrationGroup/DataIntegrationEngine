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
from backend.constants import DTW, FEET, TDS
from backend.transformer import (
    SiteTransformer,
    WaterLevelTransformer,
    AnalyteTransformer,
    standardize_datetime,
)
from backend.connectors.ocotillo.mappings import OCOTILLO_ANALYTE_MAPPING

SOURCE_TAG = "NMBGMR-Ocotillo"


class OcotilloSiteTransformer(SiteTransformer):
    def _transform(self, record):
        props = record["properties"]
        lon, lat = record["geometry"]["coordinates"][:2]
        rec = {
            "source": SOURCE_TAG,
            "id": props["name"],
            "name": props["name"],
            "latitude": lat,
            "longitude": lon,
            # water_wells carries no elevation, datum, or vertical datum.
            # Coordinates are published in WGS84 (GeoJSON default).
            "elevation": None,
            "elevation_units": "",
            "horizontal_datum": "WGS84",
            "vertical_datum": "",
            "usgs_site_id": "",
            "alternate_site_id": props.get("nma_pk_welldata") or "",
            "formation": props.get("nma_formation_zone") or "",
            "well_depth": props.get("well_depth"),
            "well_depth_units": FEET,
        }
        return rec


class _OcotilloSummaryTransformer:
    """Mixin: build a SummaryRecord dict directly from a pre-aggregated Ocotillo
    feature. Ocotillo publishes no raw observations, so the usual summarize path
    (which needs the full record set and both earliest+latest) does not apply.
    Only the latest value (and, for water levels, count/min/max) are available;
    every other summary column is left null."""

    source_tag = SOURCE_TAG

    def _summary_rec(
        self,
        site_record,
        parameter,
        out_units,
        source_name,
        latest_value,
        source_units,
        latest_datetime,
        nrecords=None,
        min_value=None,
        max_value=None,
    ):
        def conv(v):
            if v is None:
                return None
            value, _factor, warning = self.converter.convert(
                v,
                source_units or out_units,
                out_units,
                source_name,
                parameter,
                latest_datetime,
            )
            if warning:
                self.warn(f"{warning} for {site_record.id}")
                return None
            return value

        latest = conv(latest_value)
        if latest is None:
            # No usable latest value -> no summary for this site.
            return None

        if latest_datetime:
            latest_date, latest_time = standardize_datetime(
                latest_datetime, site_record.id
            )
        else:
            latest_date, latest_time = None, None

        return {
            "source": self.source_tag,
            "id": site_record.id,
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
            "parameter_units": out_units,
            "nrecords": nrecords,
            "min": conv(min_value),
            "max": conv(max_value),
            # Ocotillo does not expose mean or the earliest observation.
            "mean": None,
            "earliest_date": None,
            "earliest_time": None,
            "earliest_value": None,
            "earliest_units": None,
            "latest_date": latest_date,
            "latest_time": latest_time,
            "latest_value": latest,
            "latest_units": out_units,
        }


class OcotilloWaterLevelTransformer(_OcotilloSummaryTransformer, WaterLevelTransformer):
    source_tag = SOURCE_TAG

    def _transform(self, record, site_record):
        props = record["properties"]
        _parameter, out_units = self._get_parameter_name_and_units()
        return self._summary_rec(
            site_record,
            parameter=DTW,
            out_units=out_units,
            source_name="depth_to_water_bgs",
            latest_value=props.get("last_water_level"),
            source_units=FEET,
            latest_datetime=props.get("last_water_level_datetime"),
            nrecords=props.get("total_water_levels"),
            min_value=props.get("min_water_level"),
            max_value=props.get("max_water_level"),
        )


class OcotilloAnalyteTransformer(_OcotilloSummaryTransformer, AnalyteTransformer):
    source_tag = SOURCE_TAG

    def _transform(self, record, site_record):
        props = record["properties"]
        parameter, out_units = self._get_parameter_name_and_units()

        if parameter == TDS:
            value = props.get("latest_tds_value")
            source_units = props.get("latest_tds_units")
            latest_datetime = props.get("latest_tds_observation_date")
            source_name = "tds"
        else:
            _collection, column = OCOTILLO_ANALYTE_MAPPING[parameter]
            value = props.get(column)
            source_units = props.get(f"{column}_units")
            latest_datetime = props.get("latest_chemistry_date")
            source_name = column

        return self._summary_rec(
            site_record,
            parameter=parameter,
            out_units=out_units,
            source_name=source_name,
            latest_value=value,
            source_units=source_units,
            latest_datetime=latest_datetime,
            # Chemistry/TDS collections expose only a single latest value.
            nrecords=None,
            min_value=None,
            max_value=None,
        )


# ============= EOF =============================================
