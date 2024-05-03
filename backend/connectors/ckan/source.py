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

import httpx

from backend.connectors.ckan.transformer import OSERoswellSiteTransformer, OSERoswellWaterLevelTransformer
from backend.source import BaseSource, BaseSiteSource, BaseWaterLevelsSource


class CKANSource:
    base_url = None
    _cached_response = None

    def get_records(self, config):
        yield from self._parse_response(self.get_response(config))

    def get_response(self, config):
        if self.base_url is None:
            raise NotImplementedError('base_url is not set')

        if self._cached_response is None:
            self._cached_response = httpx.get(self.base_url, params=self._get_params(config))

        return self._cached_response

    def _get_params(self, config):
        return {}

    def _parse_response(self, resp):
        raise NotImplementedError('parse_response not implemented')


class NMWDICKANSource(CKANSource):
    base_url = 'https://catalog.newmexicowaterdata.org/api/3/action/datastore_search'


class OSERoswellSource(NMWDICKANSource):
    resource_id = None

    def __init__(self, resource_id):
        self.resource_id = resource_id
        super().__init__()

    def _get_params(self, config):
        return {
            'resource_id': self.resource_id,
        }


class OSERoswellSiteSource(OSERoswellSource, BaseSiteSource):
    transformer_klass = OSERoswellSiteTransformer

    def _parse_response(self, resp):
        records = resp.json()['result']['records']
        # group records by site_no
        records = sorted(records, key=lambda x: x['Site_ID'])
        for site_id, records in groupby(records, key=lambda x: x['Site_ID']):
            yield next(records)


class OSERoswellWaterLevelSource(OSERoswellSource, BaseWaterLevelsSource):
    transformer_klass = OSERoswellWaterLevelTransformer

    def read(self, parent_record, config):
        self.log(f"Gathering records for record {parent_record.id}")
        n = 0
        for record in self._get_waterlevels(parent_record, self.get_response(config)):
            record = self.transformer.transform(record, parent_record, config)
            if record:
                n += 1
                yield record

        self.log(f"nrecords={n}")

    def _get_waterlevels(self, parent_record, resp):
        records = resp.json()['result']['records']
        for record in records:
            if record['Site_ID'] == parent_record.id:
                yield record
# ============= EOF =============================================
