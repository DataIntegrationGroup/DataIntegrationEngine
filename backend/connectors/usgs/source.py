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
import httpx

from backend.connectors.usgs.transformer import (
    USGSSiteTransformer,
    USGSWaterLevelTransformer,
)
from backend.source import BaseSource, BaseWaterLevelSource, BaseSiteSource


def parse_rdb(text):
    def line_generator():
        header = None
        for line in text.split("\n"):
            if line.startswith("#"):
                continue
            elif line.startswith("agency_cd"):
                header = [h.strip() for h in line.split("\t")]
                continue
            elif line.startswith("5s"):
                continue
            elif line == "":
                continue

            vals = [v.strip() for v in line.split("\t")]
            if header and any(vals):
                yield dict(zip(header, vals))

    return list(line_generator())


class USGSSiteSource(BaseSiteSource):
    transformer_klass = USGSSiteTransformer

    def get_records(self, config):
        params = {"format": "rdb", "siteOutput": "expanded", "siteType": "GW"}

        if config.has_bounds():
            bbox = config.bounding_points()
            params["bBox"] = ",".join([str(b) for b in bbox])
        else:
            params["stateCd"] = "NM"

        resp = httpx.get(
            "https://waterservices.usgs.gov/nwis/site/", params=params, timeout=10
        )
        records = parse_rdb(resp.text)

        self.log(f"Retrieved {len(records)} records")
        return records


class USGSWaterLevelSource(BaseWaterLevelSource):
    transformer_klass = USGSWaterLevelTransformer

    def get_records(self, parent_record, config):

        if isinstance(parent_record, list):
            sites = ",".join([r.id for r in parent_record])
        else:
            sites = parent_record.id

        params = {
            "format": "rdb",
            "siteType": "GW",
            "sites": sites,
            # "startDT": config.start_date,
            # "endDT": config.end_date,
        }

        resp = httpx.get(
            "https://waterservices.usgs.gov/nwis/gwlevels/", params=params, timeout=10
        )
        records = parse_rdb(resp.text)
        return records

    def _extract_parent_records(self, records, parent_record):
        return [ri for ri in records if ri["site_no"] == parent_record.id]

    def _extract_waterlevels(self, records):
        return [
            float(r["lev_va"])
            for r in records
            if r["lev_va"] is not None and r["lev_va"].strip()
        ]

    def _extract_most_recent(self, records):

        return [(r["lev_dt"], r["lev_tm"]) for r in records if r["lev_dt"] is not None][
            -1
        ]


# ============= EOF =============================================
