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
from backend.config import Config
from backend.connectors.ampapi.source import AMPAPISiteSource, AMPAPIWaterLevelSource
from backend.connectors.isc_seven_rivers.source import (
    ISCSevenRiversSiteSource,
    ISCSevenRiversWaterLevelSource,
)
from backend.connectors.usgs.source import USGSSiteSource
from backend.connectors.wqp.source import WQPSiteSource
from backend.persister import CSVPersister, GeoJSONPersister
from backend.record import SiteRecord, WaterLevelRecord, AnalyteRecord


def perister_factory(config, record_klass):
    persister_klass = CSVPersister
    if config.use_csv:
        persister_klass = CSVPersister
    elif config.use_geojson:
        persister_klass = GeoJSONPersister

    return persister_klass(record_klass)


def unify_wrapper(record_klass, config, func):
    persister = perister_factory(config, record_klass)
    func(config, persister)
    persister.save(config.output_path)


def unify_analytes(config):
    def func(config, persister):
        if config.use_source_wqp:
            wqp = WQPSiteSource()
            persister.load(wqp.read(config))

        # if config.use_source_ampapi:
        #     s = AMPAPISiteSource()
        #     persister.load(s.read(config))
        #
        # if config.use_source_isc_seven_rivers:
        #     isc = ISCSevenRiversSiteSource()
        #     persister.load(isc.read(config))
        #
        # if config.use_source_nwis:
        #     nwis = USGSSiteSource()
        #     persister.load(nwis.read(config))

    unify_wrapper(AnalyteRecord, config, func)


def unify_sites(config):
    print("unifying")

    def func(config, persister):
        if config.use_source_ampapi:
            s = AMPAPISiteSource()
            persister.load(s.read(config))

        if config.use_source_isc_seven_rivers:
            isc = ISCSevenRiversSiteSource()
            persister.load(isc.read(config))

        if config.use_source_nwis:
            nwis = USGSSiteSource()
            persister.load(nwis.read(config))

    unify_wrapper(SiteRecord, config, func)


def unify_waterlevels(config):
    def func(config, persister):
        if config.use_source_ampapi:
            s = AMPAPISiteSource()
            ss = AMPAPIWaterLevelSource()
            for record in s.read(config):
                for wl in ss.read(record, config):
                    persister.records.append(wl)

        if config.use_source_isc_seven_rivers:
            s = ISCSevenRiversSiteSource()
            ss = ISCSevenRiversWaterLevelSource()
            for record in s.read(config):
                for wl in ss.read(record, config):
                    persister.records.append(wl)

        if config.use_source_nwis:
            pass

    unify_wrapper(WaterLevelRecord, config, func)

    # if config.use_source_isc_seven_rivers:
    #     isc = ISCSevenRiversSiteSource()
    #     persister.load(isc.read(config))
    #
    # if config.use_source_nwis:
    #     nwis = USGSSiteSource()
    #     persister.load(nwis.read(config))


if __name__ == "__main__":
    cfg = Config()
    cfg.bbox = "-106.5 32.5, -106.0 33.0"
    unify_waterlevels(cfg)

# ============= EOF =============================================
