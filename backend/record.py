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


class BaseRecord:
    def to_csv(self):
        raise NotImplementedError

    def __init__(self, payload):
        self._payload = payload

    def to_row(self):

        def get(attr):
            v = self._payload.get(attr)
            if v is None and self.defaults:
                v = self.defaults.get(attr)

            for key, sigfigs in (("elevation", 2),
                                  ("depth_to_water_ft_below_ground_surface", 2),
                                  ("surface_elevation_ft", 2),
                                  ("well_depth_ft_below_ground_surface", 2),
                                  ("well_depth", 2),
                                  ("latitude", 6),
                                  ("longitude", 6),
                                  ("min", 2),
                                  ("max", 2),
                                  ("mean", 2)):
                if v is not None and key == attr:
                    v = round(v, sigfigs)
                    break
            return v

        return [get(k) for k in self.keys]

    def update(self, **kw):
        self._payload.update(kw)

    def __getattr__(self, k):
        return self._payload.get(k)


class WaterLevelRecord(BaseRecord):
    keys = (
        "source",
        "id",
        "location",
        "latitude",
        "longitude",
        "surface_elevation_ft",
        "well_depth_ft_below_ground_surface",
        "depth_to_water_ft_below_ground_surface",
        "date_measured",
        "time_measured",
    )


class WaterLevelSummaryRecord(BaseRecord):
    keys = (
        "source",
        "id",
        "location",
        "usgs_site_id",
        "alternate_site_id",
        "latitude",
        "longitude",
        "surface_elevation_ft",
        "well_depth_ft_below_ground_surface",
        "nrecords",
        "min",
        "max",
        "mean",
        "date_measured",
        "time_measured",
    )




class AnalyteRecord(BaseRecord):
    keys = (
        "source",
        "id",
        "date_measured",
        "time_measured",
        "analyte",
        "result",
        "units",
    )


class SiteRecord(BaseRecord):
    keys = (
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
    )

    defaults = {
        "source": None,
        "id": None,
        "name": "",
        "latitude": None,
        "longitude": None,
        "elevation": None,
        "elevation_units": "feet",
        "horizontal_datum": "",
        "vertical_datum": "",
        "usgs_site_id": "",
        "alternate_site_id": "",
        "formation": "",
        "aquifer": "",
        "well_depth": None,
    }


# ============= EOF =============================================
