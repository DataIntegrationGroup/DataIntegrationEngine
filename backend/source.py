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
        click.secho(f"{self.__class__.__name__:25s} -- {msg}", fg="yellow")

    def get_records(self, *args, **kw):
        raise NotImplementedError(
            f"get_records not implemented by {self.__class__.__name__}"
        )


class BaseSiteSource(BaseSource):
    chunk_size = 1

    def read(self, config, *args, **kw):
        self.log("Gathering site records")
        n = 0
        records = self.get_records(config)
        self.log(f"total records={len(records)}")
        ns = []
        for record in records:
            record = self.transformer.do_transform(record, config)
            if record:
                n += 1
                ns.append(record)

        self.log(f"processed nrecords={n}")
        return ns

    def chunks(self, records, chunk_size=None):
        if chunk_size is None:
            chunk_size = self.chunk_size

        if chunk_size > 1:
            return [
                records[i : i + chunk_size] for i in range(0, len(records), chunk_size)
            ]
        else:
            return records


def make_site_list(parent_record):
    if isinstance(parent_record, list):
        sites = [r.id for r in parent_record]
    else:
        sites = parent_record.id
    return sites


class BaseSummarySource(BaseSource):
    name = ""

    def summary_hook(self, parent_record, config, rs):
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement summary_hook"
        )

    def summary(self, parent_record, config):
        if isinstance(parent_record, list):
            self.log(
                f"Gathering {self.name} summary for multiple records. {len(parent_record)}"
            )
        else:
            self.log(f"Gathering {self.name} summary for record {parent_record.id}")

        rs = self.get_records(parent_record, config)
        if rs:
            if not isinstance(parent_record, list):
                parent_record = [parent_record]
            ret = []
            for pi in parent_record:
                rec = self.summary_hook(pi, config, rs)

                if rec is not None:
                    wls, mrd = rec
                    n = len(wls)
                    self.log(f"Retrieved {self.name}: {n}")
                    trec = self.transformer.do_transform(
                        {
                            "nrecords": n,
                            "min": min(wls),
                            "max": max(wls),
                            "mean": sum(wls) / n,
                            "most_recent_datetime": mrd,
                        },
                        config,
                        pi,
                    )
                    ret.append(trec)

            return ret


class BaseAnalyteSource(BaseSummarySource):
    name = "analyte"

    def summary_hook(self, parent_record, config, rs):

        rss = self._extract_parent_records(rs, parent_record)
        if not rss:
            return

        results = self._extract_analyte_results(rss)
        if not results:
            return

        mrd = self._extract_most_recent(rss)

        return results, mrd

    def _extract_parent_records(self, records, parent_record):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_parent_records"
        )

    def _extract_analyte_results(self, records):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_analyte_results"
        )

    def _extract_most_recent(self, records):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_most_recent"
        )


class BaseWaterLevelSource(BaseSummarySource):
    name = "water levels"

    def summary_hook(self, parent_record, config, rs):
        rrs = self._extract_parent_records(rs, parent_record)
        if not rrs:
            return

        wls = self._extract_waterlevels(rrs)
        if not wls:
            return

        mrd = self._extract_most_recent(rrs)
        if not mrd:
            return

        return wls, mrd

    def _extract_parent_records(self, records, parent_record):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_parent_records"
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


# class BaseAnalytesSource(BaseSource):
#     def read(self, parent_record, config):
#         self.log(f"Gathering analytes for record {parent_record.id}")
#         n = 0
#         for record in self.get_records(parent_record, config):
#             record = self.transformer.transform(record, parent_record, config)
#             if record:
#                 n += 1
#                 yield record
#
#         self.log(f"nrecords={n}")

# ============= EOF =============================================
