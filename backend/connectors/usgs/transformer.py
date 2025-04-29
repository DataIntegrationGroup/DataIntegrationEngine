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
from backend.record import SiteRecord, WaterLevelRecord
from backend.transformer import BaseTransformer, WaterLevelTransformer, SiteTransformer


class NWISSiteTransformer(SiteTransformer):
    def _transform(self, record):
        elevation = record["alt_va"]
        try:
            elevation = float(elevation)
        except (ValueError, TypeError):
            elevation = None

        lng = record["dec_long_va"]
        lat = record["dec_lat_va"]
        datum = record["coord_datum_cd"]

        # if not self.contained(lng, lat):
        #     return

        agency = record["agency_cd"]
        site_no = record["site_no"]
        site_id = f"{agency}-{site_no}"

        rec = {
            "source": "USGS-NWIS",
            "id": site_id,
            "name": record["station_nm"],
            "latitude": lat,
            "longitude": lng,
            "elevation": elevation,
            "elevation_units": "ft",
            "horizontal_datum": datum,
            "vertical_datum": record["alt_datum_cd"],
            "aquifer": record["nat_aqfr_cd"],
            "well_depth": record["well_depth_va"],
            "well_depth_units": "ft",
        }
        return rec


class NWISWaterLevelTransformer(WaterLevelTransformer):
    source_tag = "USGS-NWIS"

    # def _transform_hook(self, record, config, site_record):
    #     rec = {
    #         "source": "USGS-NWIS",
    #         "id": site_record.id,
    #         "location": site_record.name,
    #         "usgs_site_id": site_record.id,
    #         "latitude": site_record.latitude,
    #         "longitude": site_record.longitude,
    #         "elevation": site_record.elevation,
    #         "elevation_units": site_record.elevation_units,
    #         "well_depth": site_record.well_depth,
    #         "well_depth_units": site_record.well_depth_units,
    #         # "date": record["datetime"],
    #         # "value": record["lev_va"],
    #         # "units": "ft",
    #         # "qualifiers": record["lev_status_cd"],
    #     }
    #
    #     if config.output_summary_waterlevel_stats:
    #         rec.update(record)
    #         # rec["nrecords"] = record["nrecords"]
    #         # rec["min"] = record["min"]
    #         # rec["max"] = record["max"]
    #         # rec["mean"] = record["mean"]
    #         # rec["most_recent_datetime"] = record["most_recent_datetime"]
    #         # rec["date_measured"] = record["most_recent_date"]
    #         # rec["time_measured"] = record["most_recent_time"]
    #     # else:
    #     #     rec["date_measured"] = record["DateMeasured"]
    #     #     rec["time_measured"] = record["TimeMeasured"]
    #     #     rec["depth_to_water_ft_below_ground_surface"] = record["DepthToWaterBGS"]
    #
    #     return rec


# ============= EOF =============================================
