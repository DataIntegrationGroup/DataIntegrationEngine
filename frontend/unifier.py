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
from backend.connectors.ckan import (
    HONDO_RESOURCE_ID,
    FORT_SUMNER_RESOURCE_ID,
    ROSWELL_RESOURCE_ID,
)
from backend.connectors.ckan.source import (
    OSERoswellSiteSource,
    OSERoswellWaterLevelSource,
)
from backend.connectors.isc_seven_rivers.source import (
    ISCSevenRiversSiteSource,
    ISCSevenRiversWaterLevelSource,
)
from backend.connectors.st2.source import ST2SiteSource
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
        for source in config.site_sources():
            s = source()
            persister.load(s.read(config))

    unify_wrapper(SiteRecord, config, func)


def unify_waterlevels(config):
    def func(config, persister):
        sources = []

        if config.use_source_ampapi:
            sources.append((AMPAPISiteSource(), AMPAPIWaterLevelSource()))

        if config.use_source_isc_seven_rivers:
            sources.append(
                (ISCSevenRiversSiteSource(), ISCSevenRiversWaterLevelSource())
            )

        if config.use_source_nwis:
            pass

        if config.use_source_ose_roswell:
            sources.append(
                (
                    OSERoswellSiteSource(HONDO_RESOURCE_ID),
                    OSERoswellWaterLevelSource(HONDO_RESOURCE_ID),
                )
            )
            sources.append(
                (
                    OSERoswellSiteSource(FORT_SUMNER_RESOURCE_ID),
                    OSERoswellWaterLevelSource(FORT_SUMNER_RESOURCE_ID),
                )
            )
            sources.append(
                (
                    OSERoswellSiteSource(ROSWELL_RESOURCE_ID),
                    OSERoswellWaterLevelSource(ROSWELL_RESOURCE_ID),
                )
            )

        for s, ss in sources:
            for record in s.read(config):
                for wl in ss.read(record, config):
                    persister.records.append(wl)

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
    cfg.bbox = "-104.0 32.5, -105.0 34.0"
    unify_sites(cfg)
    # unify_waterlevels(cfg)

# ============= EOF =============================================
