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

from backend.persister import BasePersister, CSVPersister
from backend.transformer import BaseTransformer


class BaseSource:
    transformer_klass = BaseTransformer

    def __init__(self):
        self.transformer = self.transformer_klass()

    def log(self, msg):
        click.secho(f"{self.__class__.__name__:30s} {msg}", fg="yellow")

    def get_records(self, *args, **kw):
        raise NotImplementedError(
            f"get_records not implemented by {self.__class__.__name__}"
        )


class BaseSiteSource(BaseSource):
    def read(self, config, *args, **kw):
        self.log("Gathering records")
        n = 0
        for record in self.get_records(config):
            record = self.transformer.do_transform(record, config)
            if record:
                n += 1
                yield record

        self.log(f"nrecords={n}")


class BaseWaterLevelsSource(BaseSource):
    def summary(self, parent_record, config):
        self.log(f"Gathering waterlevel summary for record {parent_record.id}")
        rs = list(self.get_records(parent_record, config))
        if rs:
            print(len(rs))
            wls = self._extract_waterlevels(rs)
            mrd = self._extract_most_recent(rs)
            return self.transformer.transform(
                {
                    "nrecords": len(wls),
                    "min": min(wls),
                    "max": max(wls),
                    "mean": sum(wls) / len(wls),
                    "most_recent_date": mrd,
                },
                parent_record,
                config,
            )

    def _extract_waterlevels(self, records):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_waterlevels"
        )

    def _extract_most_recent(self, records):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_most_recent"
        )

    def read(self, parent_record, config):
        self.log(f"Gathering waterlevels for record {parent_record.id}")
        n = 0
        for record in self.get_records(parent_record, config):
            record = self.transformer.transform(record, parent_record, config)
            if record:
                n += 1
                yield record

        self.log(f"nrecords={n}")


class BaseAnalytesSource(BaseSource):
    def read(self, parent_record, config):
        self.log(f"Gathering analytes for record {parent_record.id}")
        n = 0
        for record in self.get_records(parent_record, config):
            record = self.transformer.transform(record, parent_record, config)
            if record:
                n += 1
                yield record

        self.log(f"nrecords={n}")


# ============= EOF =============================================
