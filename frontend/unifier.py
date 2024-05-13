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
from backend.persister import CSVPersister, GeoJSONPersister
from backend.record import (
    SiteRecord,
    WaterLevelRecord,
    AnalyteRecord,
    WaterLevelSummaryRecord,
)


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


def unify_sites(config):
    print("unifying")

    def func(config, persister):
        for source in config.site_sources():
            s = source()
            persister.load(s.read(config))

    unify_wrapper(SiteRecord, config, func)


def unify_waterlevels(config):
    unify_datastream(
        config, config.water_level_sources(), WaterLevelRecord, WaterLevelSummaryRecord
    )


def unify_analytes(config):
    unify_datastream(config, config.analyte_sources(), AnalyteRecord)


def unify_datastream(config, sources, record_klass, summary_record_klass):
    def func(config, persister):
        for sklass, ssklass in sources:
            s = sklass()
            ss = ssklass()
            for i, record in enumerate(s.read(config)):
                # if i > 5:
                #     break

                if config.output_summary_waterlevel_stats:
                    summary_record = ss.summary(record, config)
                    if summary_record:
                        persister.records.append(summary_record)
                else:
                    for wl in ss.read(record, config):
                        persister.records.append(wl)

    klass = record_klass
    if config.output_summary_waterlevel_stats:
        klass = summary_record_klass

    unify_wrapper(klass, config, func)


if __name__ == "__main__":
    cfg = Config()
    cfg.county = "chaves"
    cfg.output_summary_waterlevel_stats = True

    # unify_sites(cfg)
    unify_waterlevels(cfg)

# ============= EOF =============================================
