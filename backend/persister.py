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
from pprint import pprint
import json

from backend import OutputFormat
from backend.logger import Loggable


try:
    from google.cloud import storage
except ImportError:
    print("google cloud storage not available")


def write_memory(func, records, output_format=None):
    f = io.BytesIO()
    func(f, records, output_format)
    return f.getvalue()


def dump_timeseries(path, timeseries: list[list]):
    """
    Dumps timeseries records to a CSV file. The timeseries must be a list of
    lists, where each inner list contains the records for a single site. In the case
    of timeseries separated, the inner list will contain the records for a single site
    and this function will be called multiple times, once for each site.
    """
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        headers_have_not_been_written = True
        for i, records in enumerate(timeseries):
            for record in records:
                if i == 0 and headers_have_not_been_written:
                    writer.writerow(record.keys)
                    headers_have_not_been_written = False
                writer.writerow(record.to_row())


def dump_sites_summary(path, records, output_format: OutputFormat):
    if output_format == OutputFormat.CSV:
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            for i, site in enumerate(records):
                if i == 0:
                    writer.writerow(site.keys)
                writer.writerow(site.to_row())
    else:
        features = [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        getattr(record, "longitude"),
                        getattr(record, "latitude"),
                        getattr(record, "elevation"),
                    ],
                },
                "properties": {
                    k: getattr(record, k)
                    for k in record.keys
                    if k not in ["latitude", "longitude", "elevation"]
                },
            }
            for record in records
        ]
        feature_collection = {"type": "FeatureCollection", "features": features}

        with open(path, "w") as f:
            json.dump(feature_collection, f, indent=4)


class BasePersister(Loggable):
    """
    Class to persist the data to a file or cloud storage.
    If persisting to a file, the output directory is created by config._make_output_path()
    """

    def __init__(self, config=None):
        self.records = []
        self.timeseries = []
        self.sites = []
        self.config = config

        super().__init__()
        # self.keys = record_klass.keys

    def load(self, records: list):
        self.records.extend(records)

    def finalize(self, output_name: str):
        pass

    def dump_sites(self, path: str):
        if self.sites:
            path = os.path.join(path, "sites")
            path = self.add_extension(path, self.config.output_format)
            self.log(f"dumping sites to {os.path.abspath(path)}")
            self._dump_sites_summary(path, self.sites, self.config.output_format)
        else:
            self.log("no sites to dump", fg="red")

    def dump_summary(self, path: str):
        if self.records:
            path = os.path.join(path, "summary")
            path = self.add_extension(path, self.config.output_format)
            self.log(f"dumping summary to {os.path.abspath(path)}")
            self._dump_sites_summary(path, self.records, self.config.output_format)
        else:
            self.log("no records to dump", fg="red")

    def dump_timeseries_unified(self, path: str):
        if self.timeseries:
            path = os.path.join(path, "timeseries_unified")
            path = self.add_extension(path, OutputFormat.CSV)
            self.log(f"dumping unified timeseries to {os.path.abspath(path)}")
            self._dump_timeseries(path, self.timeseries)
        else:
            self.log("no timeseries records to dump", fg="red")

    def dump_timeseries_separated(self, path: str):
        if self.timeseries:
            # make timeseries path inside of config.output_path to which
            # the individual site timeseries will be dumped
            timeseries_path = os.path.join(path, "timeseries")
            self._make_output_directory(timeseries_path)
            for records in self.timeseries:
                site_id = records[0].id
                path = os.path.join(timeseries_path, str(site_id).replace(" ", "_"))
                path = self.add_extension(path, OutputFormat.CSV)
                self.log(f"dumping {site_id} to {os.path.abspath(path)}")

                list_of_records = [records]
                self._dump_timeseries(path, list_of_records)
        else:
            self.log("no timeseries records to dump", fg="red")

    def add_extension(self, path: str, extension: OutputFormat):
        if not extension:
            raise NotImplementedError
        else:
            ext = extension

        if not path.endswith(ext):
            path = f"{path}.{ext}"
        return path

    def _dump_sites_summary(
        self, path: str, records: list, output_format: OutputFormat
    ):
        dump_sites_summary(path, records, output_format)

    def _dump_timeseries(self, path: str, timeseries: list):
        dump_timeseries(path, timeseries)

    def _make_output_directory(self, output_directory: str):
        os.mkdir(output_directory)


class CloudStoragePersister(BasePersister):
    extension = "csv"
    _content: list

    def __init__(self, *args, **kwargs):
        super(CloudStoragePersister, self).__init__(*args, **kwargs)
        self._content = []

    def finalize(self, output_name: str):
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
            blob = bucket.blob(f"{output_name}.zip")
            blob.upload_from_string(zip_buffer.getvalue())
        else:
            path, cnt = self._content[0]

            # this is a hack. need a better way to specify the output path
            dirname = os.path.basename(os.path.dirname(path))
            path = os.path.join(dirname, os.path.basename(path))

            blob = bucket.blob(path)
            blob.upload_from_string(
                cnt,
                content_type=(
                    "application/json"
                    if self.config.output_format == OutputFormat.GEOJSON
                    else "text/csv"
                ),
            )

    def _make_output_directory(self, output_directory: str):
        # prevent making root directory, because we are not saving to disk
        pass

    def _add_content(self, path: str, content: str):
        self._content.append((path, content))

    def _dump_sites_summary(
        self, path: str, records: list, output_format: OutputFormat
    ):
        content = write_memory(dump_sites_summary, records, output_format)
        self._add_content(path, content)

    def _dump_timeseries_unified(self, path: str, timeseries: list):
        content = write_memory(path, dump_timeseries, timeseries)
        self._add_content(path, content)


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
