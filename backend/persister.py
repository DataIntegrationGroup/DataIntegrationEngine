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
import io
import os
import shutil

import click
import pandas as pd
import geopandas as gpd

from backend.record import SiteRecord

try:
    from google.cloud import storage
except ImportError:
    print("google cloud storage not available")


class Loggable:
    def log(self, msg, fg="yellow"):
        click.secho(f"{self.__class__.__name__:30s}{msg}", fg=fg)


class BasePersister(Loggable):
    extension: str
    output_id: str

    def __init__(self):
        self.records = []
        self.combined = []
        self.timeseries = []
        # self.keys = record_klass.keys

    def load(self, records):
        self.records.extend(records)

    def finalize(self, output_id):
        pass

    def dump_timeseries(self, root):
        if self.timeseries:
            if os.path.isdir(root):
                self.log(f"root {root} already exists", fg="red")
                shutil.rmtree(root)

            self._make_root_directory(root)

            for site, records in self.timeseries:
                path = os.path.join(root, str(site.id).replace(" ", "_"))
                path = self.add_extension(path)
                self.log(f"dumping {site.id} to {os.path.abspath(path)}")
                self._write(path, records)

            self._write(
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
            self._write(path, self.records)
        else:
            self.log("no records to save", fg="red")

    def add_extension(self, path):
        if not self.extension:
            raise NotImplementedError

        if not path.endswith(self.extension):
            path = f"{path}.{self.extension}"
        return path

    def _write(self, path, records):
        raise NotImplementedError

    def _dump_combined(self, path, combined):
        raise NotImplementedError

    def _make_root_directory(self, root):
        os.mkdir(root)


def write_file(path, func):
    with open(path, "w") as f:
        func(csv.writer(f))


def write_memory(func):
    f = io.StringIO()
    func(f, csv.writer(f))


class CloudStoragePersister(BasePersister):
    extension = "csv"
    _content: list

    def __init__(self):
        super(CloudStoragePersister, self).__init__()
        self._content = []

    def finalize(self, output_id):
        """
        zip content and upload to google cloud storage
        :return:
        """
        if not self._content:
            self.log("no content to save", fg="red")
            return

        storage_client = storage.Client()
        bucket = storage_client.bucket("die_cache")
        if len(self._content) > 1:
            import zipfile

            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                for path, cnt in self._content:
                    zf.writestr(path, cnt)
            blob = bucket.blob(f"{output_id}.zip")
            blob.upload_from_string(zip_buffer.getvalue())
        else:
            path, cnt = self._content[0]
            blob = bucket.blob(path)
            blob.upload_from_string(cnt)

    def _make_root_directory(self, root):
        # prevent making root directory, because we are not saving to disk
        pass

    def _write(self, path, records):
        def func(f, writer):
            for i, site in enumerate(records):
                if i == 0:
                    writer.writerow(site.keys)

                writer.writerow(site.to_row())

            self._add_content(path, f.getvalue())

        write_memory(func)

    def _add_content(self, path, cnt):
        self._content.append((path, cnt))

    def _dump_combined(self, path, combined):
        def func(f, writer):
            for i, (site, record) in enumerate(combined):
                if i == 0:
                    writer.writerow(site.keys + record.keys)

                writer.writerow(site.to_row() + record.to_row())

            self._add_content(path, f.getvalue())

        write_memory(func)


class CSVPersister(BasePersister):
    extension = "csv"

    def _write(self, path, records):
        def func(writer):
            for i, site in enumerate(records):
                if i == 0:
                    writer.writerow(site.keys)
                writer.writerow(site.to_row())

        write_file(path, func)

    def _dump_combined(self, path, combined):
        def func(writer):
            for i, (site, record) in enumerate(combined):
                if i == 0:
                    writer.writerow(site.keys + record.keys)

                writer.writerow(site.to_row() + record.to_row())

        write_file(path, func)


class GeoJSONPersister(BasePersister):
    extension = "geojson"

    def _write(self, path, records):
        r0 = records[0]
        df = pd.DataFrame([r.to_row() for r in records], columns=r0.keys)

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
