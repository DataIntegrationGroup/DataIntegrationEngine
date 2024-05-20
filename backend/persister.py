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
import csv
import os
import shutil

import click
import pandas as pd
import geopandas as gpd

# from frost_sta_client import AuthHandler

from backend.record import SiteRecord


class Loggable:
    def log(self, msg, fg="yellow"):
        click.secho(f"{self.__class__.__name__:30s}{msg}", fg=fg)


class BasePersister(Loggable):
    extension = None

    def __init__(self):
        self.records = []
        self.combined = []
        self.timeseries = []
        # self.keys = record_klass.keys

    def load(self, records):
        self.records.extend(records)

    def dump_timeseries(self, root):
        if self.timeseries:
            if os.path.isdir(root):
                self.log(f"root {root} already exists", fg="red")
                shutil.rmtree(root)

            os.mkdir(root)

            for site, records in self.timeseries:
                path = os.path.join(root, str(site.id).replace(" ", "_"))
                path = self.add_extension(path)
                self.log(f"dumping {site.id} to {os.path.abspath(path)}")
                self._dump_timeseries(path, records)

            self._dump_sites(
                os.path.join(root, self.add_extension("sites")),
                [s[0] for s in self.timeseries],
            )
        else:
            self.log("no timeseries records to dump", fg="red")

    def dump_combined(self, path):
        if self.combined:
            path = self.add_extension(path)

            self.log(f"dumping combined to {os.path.abspath(path)}")
            self._dump_combined(path, self.combined)
        else:
            self.log("no combined records to dump", fg="red")

    def save(self, path):
        if self.records:
            path = self.add_extension(path)
            self.log(f"saving to {path}")
            self._save(path)
        else:
            self.log("no records to save", fg="red")

    def add_extension(self, path):
        if not self.extension:
            raise NotImplementedError

        if not path.endswith(self.extension):
            path = f"{path}.{self.extension}"
        return path

    def _save(self, path):
        raise NotImplementedError

    def _dump_combined(self, path, combined):
        raise NotImplementedError

    def _dump_timeseries(self, root, timeseries):
        raise NotImplementedError

    def _dump_sites(self, path, sites):
        raise NotImplementedError


class CSVPersister(BasePersister):
    extension = "csv"

    def _dump_sites(self, path, sites):
        with open(path, "w") as f:
            writer = csv.writer(f)
            for i, site in enumerate(sites):
                if i == 0:
                    writer.writerow(site.keys)

                writer.writerow(site.to_row())

    def _dump_timeseries(self, path, records):
        with open(path, "w") as f:
            writer = csv.writer(f)
            for i, record in enumerate(records):
                if i == 0:
                    writer.writerow(record.keys)

                writer.writerow(record.to_row())

    def _dump_combined(self, path, combined):
        with open(path, "w") as f:
            writer = csv.writer(f)
            for i, (site, record) in enumerate(combined):
                if i == 0:
                    writer.writerow(site.keys + record.keys)

                writer.writerow(site.to_row() + record.to_row())

    def _save(self, path):
        with open(path, "w") as f:
            writer = csv.writer(f)
            for i, record in enumerate(self.records):
                if i == 0:
                    writer.writerow(record.keys)

                writer.writerow(record.to_row())


class GeoJSONPersister(BasePersister):
    extension = "geojson"

    def _save(self, path):
        df = pd.DataFrame([r.to_row() for r in self.records], columns=self.keys)

        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
        )
        gdf.to_file(path, driver="GeoJSON")


# class ST2Persister(BasePersister):
#     extension = "st2"
#
#     def save(self, path):
#         import frost_sta_client as fsc
#
#         service = fsc.SensorThingsService(
#             "https://st.newmexicowaterdata.org/FROST-Server/v1.0",
#             auth_handler=AuthHandler(os.getenv("ST2_USER"), os.getenv("ST2_PASSWORD")),
#         )
#         for record in self.records:
#             for t in service.things().query().filter(name=record["id"]).list():
#                 print(t)


# ============= EOF =============================================
