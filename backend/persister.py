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
import json
import os

from backend import OutputFormat
from backend.logger import make_logger
from backend.persisters.strategies import LocalFileStrategy


def _timeseries_to_bytes(timeseries: list) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    headers_written = False
    for i, records in enumerate(timeseries):
        for record in records:
            if not headers_written:
                writer.writerow(record.keys)
                headers_written = True
            writer.writerow(record.to_row())
    return buf.getvalue().encode("utf-8")


def _records_to_bytes(records: list, output_format: OutputFormat) -> bytes:
    if output_format == OutputFormat.CSV:
        buf = io.StringIO()
        writer = csv.writer(buf)
        for i, site in enumerate(records):
            if i == 0:
                writer.writerow(site.keys)
            writer.writerow(site.to_row())
        return buf.getvalue().encode("utf-8")
    else:
        features = [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        getattr(r, "longitude"),
                        getattr(r, "latitude"),
                        getattr(r, "elevation"),
                    ],
                },
                "properties": {
                    k: getattr(r, k)
                    for k in r.keys
                    if k not in ["latitude", "longitude", "elevation"]
                },
            }
            for r in records
        ]
        fc = {"type": "FeatureCollection", "features": features}
        return json.dumps(fc, indent=4).encode("utf-8")


class BasePersister:
    def __init__(self, config=None, strategy=None):
        self.records = []
        self.timeseries = []
        self.sites = []
        self.config = config
        self._strategy = strategy if strategy is not None else LocalFileStrategy()
        _l = make_logger(self.__class__.__name__)
        self.log = _l.log
        self.warn = _l.warn
        self.debug = _l.debug

    def load(self, records: list):
        self.records.extend(records)

    def finalize(self, output_name: str):
        if hasattr(self._strategy, "finalize"):
            self._strategy.finalize()

    def dump_sites(self, path: str):
        try:
            if self.sites:
                path = os.path.join(path, "sites")
                path = self.add_extension(path, self.config.output_format)
                self.log(f"dumping sites to {os.path.abspath(path)}")
                self._dump_sites_summary(path, self.sites, self.config.output_format)
            else:
                self.log("no sites to dump", fg="red")
        except Exception as e:
            self.warn(f"failed to dump sites: {e}", exc_info=True)
            raise

    def dump_summary(self, path: str):
        try:
            if self.records:
                path = os.path.join(path, "summary")
                path = self.add_extension(path, self.config.output_format)
                self.log(f"dumping summary to {os.path.abspath(path)}")
                self._dump_sites_summary(path, self.records, self.config.output_format)
            else:
                self.log("no records to dump", fg="red")
        except Exception as e:
            self.warn(f"failed to dump summary: {e}", exc_info=True)
            raise

    def dump_timeseries_unified(self, path: str):
        try:
            if self.timeseries:
                path = os.path.join(path, "timeseries_unified")
                path = self.add_extension(path, OutputFormat.CSV.value)
                self.log(f"dumping unified timeseries to {os.path.abspath(path)}")
                self._dump_timeseries(path, self.timeseries)
            else:
                self.log("no timeseries records to dump", fg="red")
        except Exception as e:
            self.warn(f"failed to dump unified timeseries: {e}", exc_info=True)
            raise

    def dump_timeseries_separated(self, path: str):
        try:
            if self.timeseries:
                # make timeseries path inside of config.output_path to which
                # the individual site timeseries will be dumped
                timeseries_path = os.path.join(path, "timeseries")
                self._make_output_directory(timeseries_path)
                for records in self.timeseries:
                    site_id = records[0].id
                    site_path = os.path.join(timeseries_path, str(site_id).replace(" ", "_"))
                    site_path = self.add_extension(site_path, OutputFormat.CSV.value)
                    self.log(f"dumping {site_id} to {os.path.abspath(site_path)}")

                    list_of_records = [records]
                    self._dump_timeseries(site_path, list_of_records)
            else:
                self.log("no timeseries records to dump", fg="red")
        except Exception as e:
            self.warn(f"failed to dump separated timeseries: {e}", exc_info=True)
            raise

    def add_extension(self, path: str, extension: str):
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
        self._strategy.write_bytes(path, _records_to_bytes(records, output_format))

    def _dump_timeseries(self, path: str, timeseries: list):
        self._strategy.write_bytes(path, _timeseries_to_bytes(timeseries))

    def _make_output_directory(self, output_directory: str):
        self._strategy.make_directory(output_directory)


# ============= EOF =============================================
