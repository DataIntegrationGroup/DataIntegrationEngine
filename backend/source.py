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

from backend.connectors.constants import (
    MILLIGRAMS_PER_LITER,
    FEET,
    METERS,
    PARTS_PER_MILLION,
)
from backend.persister import BasePersister, CSVPersister
from backend.transformer import BaseTransformer, convert_units


class BaseSource:
    transformer_klass = BaseTransformer
    config = None

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

    def read_sites(self, *args, **kw):
        self.log("Gathering site records")
        records = self.get_records()
        self.log(f"total records={len(records)}")
        return self._transform_sites(records)

    def _transform_sites(self, records):
        ns = []
        for record in records:
            record = self.transformer.do_transform(record, self.config)
            if record:
                record.chunk_size = self.chunk_size
                ns.append(record)

        self.log(f"processed nrecords={len(ns)}")
        return ns

    def chunks(self, records, chunk_size=None):
        if chunk_size is None:
            chunk_size = self.chunk_size

        if chunk_size > 1:
            return [
                records[i: i + chunk_size] for i in range(0, len(records), chunk_size)
            ]
        else:
            return records


def make_site_list(parent_record):
    if isinstance(parent_record, list):
        sites = [r.id for r in parent_record]
    else:
        sites = parent_record.id
    return sites


def get_most_recent(records, tag):
    if callable(tag):
        func = tag
    else:
        if '.' in tag:
            def func(x):
                for t in tag.split('.'):
                    x = x[t]
                return x
        else:
            def func(x):
                return x[tag]

    return sorted(records, key=func)[-1]


class BaseSummarySource(BaseSource):
    name = ""

    def _extract_parent_records(self, records, parent_record):
        if parent_record.chunk_size == 1:
            return records

        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_parent_records"
        )

    def _extract_most_recent(self, records):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_most_recent"
        )

    def _clean_records(self, records):
        return records

    def _summary_hook(self, parent_record, rs):
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _summary_hook"
        )

    # def _convert_most_recent(self, records):
    #     return records

    def summarize(self, parent_record):
        if isinstance(parent_record, list):
            self.log(
                f"Gathering {self.name} summary for multiple records. {len(parent_record)}"
            )
        else:
            self.log(f"Gathering {self.name} summary for record {parent_record.id}")

        rs = self.get_records(parent_record)
        if rs:
            if not isinstance(parent_record, list):
                parent_record = [parent_record]
            ret = []
            for pi in parent_record:
                rrs = self._extract_parent_records(rs, pi)
                if not rrs:
                    continue

                cleaned = self._clean_records(rrs)
                if not cleaned:
                    continue

                mr = self._extract_most_recent(cleaned)
                if not mr:
                    continue

                items = self._summary_hook(pi, cleaned)

                if items is not None:
                    n = len(items)
                    self.log(f"Retrieved {self.name}: {n}")
                    trec = self.transformer.do_transform(
                        {
                            "nrecords": n,
                            "min": min(items),
                            "max": max(items),
                            "mean": sum(items) / n,
                            "most_recent_datetime": mr['datetime'],
                            "most_recent_value": mr['value'],
                            "most_recent_units": mr['units']
                        },
                        self.config,
                        pi,
                    )
                    ret.append(trec)

            return ret


class BaseAnalyteSource(BaseSummarySource):
    name = "analyte"

    def _summary_hook(self, parent_record, rs):
        results = self._extract_analyte_results(rs)
        if not results:
            return

        units = self._extract_analyte_units(rs)
        results = [
            convert_units(float(r), u, self.config.analyte_output_units)
            for r, u in zip(results, units)
        ]
        return results

    # def _convert_most_recent(self, records):
    #     print(records)
    #     return [convert_units(float(r['value']),
    #                           r['units'], self.config.analyte_output_units) for r in records]

    def _extract_analyte_units(self, records):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_analyte_units"
        )

    def _extract_analyte_results(self, records):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_analyte_results"
        )


class BaseWaterLevelSource(BaseSummarySource):
    name = "water levels"

    def _summary_hook(self, parent_record, rs):
        rs = self._extract_waterlevels(rs)
        us = self._extract_waterlevel_units(rs)
        return [
            convert_units(float(r), u, self.config.waterlevel_output_units)
            for r, u in zip(rs, us)
        ]

    def _extract_waterlevel_units(self, records):
        return [FEET for _ in records]

    def _extract_waterlevels(self, records):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_waterlevels"
        )

    # def read(self, parent_record, config):
    #     self.log(f"Gathering waterlevels for record {parent_record.id}")
    #     n = 0
    #     for record in self.get_records(parent_record):
    #         record = self.transformer.transform(record, parent_record, config)
    #         if record:
    #             n += 1
    #             yield record
    #
    #     self.log(f"nrecords={n}")

# ============= EOF =============================================
