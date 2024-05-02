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
from backend.connectors.ampapi.source import AMPAPISiteSource
from backend.connectors.isc_seven_rivers.source import ISCSevenRiversSiteSource
from backend.connectors.usgs.source import USGSSiteSource
from backend.persister import CSVPersister, GeoJSONPersister


def unify():
    print('unifying')

    persister = CSVPersister()
    persister = GeoJSONPersister()

    # s = AMPAPISiteSource()
    # persister.load(s.read())

    # isc = ISCSevenRiversSiteSource()
    # persister.load(isc.read())

    # nwis = USGSSiteSource()
    # persister.load(nwis.read())

    outpath = 'output'
    persister.save(outpath)


if __name__ == '__main__':
    unify()

# ============= EOF =============================================