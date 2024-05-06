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
from itertools import groupby
import frost_sta_client as fsc
import httpx

from backend.connectors.st2.transformer import ST2SiteTransformer, PVACDSiteTransformer, EBIDSiteTransformer
from backend.source import BaseSource, BaseSiteSource, BaseWaterLevelsSource


class ST2Source(BaseSource):
    pass


class ST2SiteSource(BaseSiteSource):
    url = 'https://st2.newmexicowaterdata.org/FROST-Server/v1.0'


class PVACDSiteSource(ST2SiteSource):
    transformer_klass = PVACDSiteTransformer

    def get_records(self, config, *args, **kw):
        service = fsc.SensorThingsService(self.url)
        for location in service.locations().query().filter("properties/agency eq 'PVACD'").list():
            yield location


class EBIDSiteSource(ST2SiteSource):
    transformer_klass = EBIDSiteTransformer

    def get_records(self, config, *args, **kw):
        service = fsc.SensorThingsService(self.url)
        for location in service.locations().query().filter("properties/agency eq 'EBID'").list():
            yield location
# ============= EOF =============================================
