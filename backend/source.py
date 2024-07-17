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
from json import JSONDecodeError

import click
import httpx
import shapely.wkt
from shapely import MultiPoint

from backend.constants import (
    MILLIGRAMS_PER_LITER,
    FEET,
    METERS,
    PARTS_PER_MILLION,
    DTW,
    DTW_UNITS,
    DT_MEASURED,
    PARAMETER,
    PARAMETER_UNITS,
    PARAMETER_VALUE,
)
from backend.persister import BasePersister, CSVPersister
from backend.transformer import BaseTransformer, convert_units


class BaseSource:
    transformer_klass = BaseTransformer
    config = None

    def __init__(self, config=None):
        self.transformer = self.transformer_klass()
        self.set_config(config)

    @property
    def tag(self):
        return self.__class__.__name__.lower()

    def set_config(self, config):
        self.config = config
        self.transformer.config = config

    # required interface
    def health(self):
        raise NotImplementedError(f"test not implemented by {self.__class__.__name__}")

    def check(self, *args, **kw):
        return True
        # raise NotImplementedError(f"check not implemented by {self.__class__.__name__}")

    def discover(self, *args, **kw):
        return []
        # raise NotImplementedError(f"discover not implemented by {self.__class__.__name__}")

    def read(self, *args, **kw):
        raise NotImplementedError(f"read not implemented by {self.__class__.__name__}")

    # =====================================================================================
    def warn(self, msg):
        self.log(msg, fg="red")

    def log(self, msg, fg="yellow"):
        click.secho(f"{self.__class__.__name__:25s} -- {msg}", fg=fg)

    def get_records(self, *args, **kw):
        raise NotImplementedError(
            f"get_records not implemented by {self.__class__.__name__}"
        )

    def _execute_text_request(self, url, params=None, **kw):
        if "timeout" not in kw:
            kw["timeout"] = 10

        resp = httpx.get(url, params=params, **kw)
        if resp.status_code == 200:
            return resp.text
        else:
            self.warn(f"service url {resp.url}")
            self.warn(f"service responded with status {resp.status_code}")
            self.warn(f"service responded with text {resp.text}")
            return ""

    def _execute_json_request(self, url, params=None, tag=None, **kw):
        print(url)
        resp = httpx.get(url, params=params, **kw)
        if tag is None:
            tag = "data"

        if resp.status_code == 200:
            try:
                obj = resp.json()
                if tag and isinstance(obj, dict):
                    return obj[tag]
                return obj
            except JSONDecodeError:
                self.warn(f"service responded but with no data. \n{resp.text}")
                return []
        else:
            self.warn(f"service responded with status {resp.status_code}")
            self.warn(f"service responded with text {resp.text}")
            return []


class BaseContainerSource(BaseSource):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)

        # locate image
        # make container
        # container writes messages to stdout
        # this class captures the messages from stdout

    def check(self):
        # run the container with the check command
        pass

    def discover(self, *args, **kw):
        # run the container with the discover command
        pass

    def read(self, *args, **kw):
        # run the container with the read command
        pass


class BaseSiteSource(BaseSource):
    chunk_size = 1
    bounding_polygon = None

    @property
    def tag(self):
        return self.__class__.__name__.lower().replace("sitesource", "")

    def generate_bounding_polygon(self):
        records = self.read_sites()
        print(records[0].latitude)
        mpt = MultiPoint([(r.longitude, r.latitude) for r in records])
        print(mpt.convex_hull.buffer(1 / 60.0).wkt)
        # print(mpt.convex_hull.wkt)

    def intersects(self, wkt):
        if self.bounding_polygon:
            wkt = shapely.wkt.loads(wkt)
            return self.bounding_polygon.intersects(wkt)

        return True

    def read(self, *args, **kw):
        self.log("Gathering site records")
        records = self.get_records()
        if records:
            self.log(f"total records={len(records)}")
            return self._transform_sites(records)
        else:
            self.warn("No site records returned")

    def _transform_sites(self, records):
        ns = []
        for record in records:
            record = self.transformer.do_transform(record)
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
        if "." in tag:

            def func(x):
                for t in tag.split("."):
                    x = x[t]
                return x

        else:

            def func(x):
                return x[tag]

    return sorted(records, key=func)[-1]


class BaseParameterSource(BaseSource):
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

    def _extract_parameter_units(self, records):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_parameter_units"
        )

    def _extract_parameter_record(self, record):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_parameter_record"
        )

    def _extract_parameter_results(self, records):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_parameter_results"
        )

    def _validate_record(self, record):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _validate_record"
        )

    def _get_output_units(self):
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _get_output_units"
        )

    def read(self, parent_record, use_summarize):
        if isinstance(parent_record, list):
            self.log(
                f"Gathering {self.name} summary for multiple records. {len(parent_record)}"
            )
        else:
            self.log(
                f"{parent_record.id} ({parent_record.id}): Gathering {self.name} summary"
            )

        rs = self.get_records(parent_record)
        if rs:
            if not isinstance(parent_record, list):
                parent_record = [parent_record]

            ret = []
            for pi in parent_record:
                rrs = self._extract_parent_records(rs, pi)
                if not rrs:
                    self.warn(f"{pi.name}: No parent records found")
                    continue

                cleaned = self._clean_records(rrs)
                if not cleaned:
                    self.warn(f"{pi.name} No clean records found")
                    continue

                items = self._extract_parameter_results(cleaned)
                units = self._extract_parameter_units(cleaned)
                items = [
                    convert_units(float(r), u, self._get_output_units())
                    for r, u in zip(items, units)
                ]

                if items is not None:
                    n = len(items)
                    self.log(f"{pi.name}: Retrieved {self.name}: {n}")
                    if use_summarize:
                        mr = self._extract_most_recent(cleaned)
                        if not mr:
                            continue
                        rec = {
                            "nrecords": n,
                            "min": min(items),
                            "max": max(items),
                            "mean": sum(items) / n,
                            "most_recent_datetime": mr["datetime"],
                            "most_recent_value": mr["value"],
                            "most_recent_units": mr["units"],
                        }
                        trec = self.transformer.do_transform(
                            rec,
                            pi,
                        )
                        ret.append(trec)
                    else:
                        cs = [
                            self.transformer.do_transform(
                                self._extract_parameter(r), pi
                            )
                            for r in cleaned
                        ]
                        cs = sorted(cs, key=self._sort_func)
                        ret.append((pi, cs))

            return ret
        else:
            if isinstance(parent_record, list):
                names = [str(r.id) for r in parent_record]
            else:
                names = [str(parent_record.id)]

            name = ",".join(names)
            self.warn(f"{name}: No records found")

    def _extract_parameter(self, record):
        record = self._extract_parameter_record(record)
        self._validate_record(record)
        return record

    def _sort_func(self, x):
        return x.date_measured


def get_analyte_search_param(parameter, mapping):
    try:
        return mapping[parameter]
    except KeyError:
        raise ValueError(
            f"Invalid parameter name {parameter}. Valid parameters are {list(mapping.keys())}"
        )


class BaseAnalyteSource(BaseParameterSource):
    name = "analyte"

    def _get_output_units(self):
        return self.config.analyte_output_units

    def _validate_record(self, record):
        record[PARAMETER] = self.config.analyte
        for k in (PARAMETER_VALUE, PARAMETER_UNITS, DT_MEASURED):
            if k not in record:
                raise ValueError(f"Invalid record. Missing {k}")


class BaseWaterLevelSource(BaseParameterSource):
    name = "water levels"

    def _get_output_units(self):
        return self.config.waterlevel_output_units

    def _extract_parameter_units(self, records):
        return [FEET for _ in records]

    def _validate_record(self, record):
        for k in (DTW, DTW_UNITS, DT_MEASURED):
            if k not in record:
                raise ValueError(f"Invalid record. Missing {k}")

# ============= EOF =============================================
