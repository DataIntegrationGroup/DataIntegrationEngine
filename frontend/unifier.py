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
        for s, ss in sources:
            try:
                sites = s.read(config)
                for i, sites in enumerate(s.chunks(sites, 100)):
                    if config.output_summary_waterlevel_stats:
                        summary_records = ss.summary(sites, config)
                        if summary_records:
                            persister.records.extend(summary_records)
                    else:
                        for wl in ss.read(sites, config):
                            persister.records.append(wl)
            except BaseException:
                import traceback

                exc = traceback.format_exc()
                click.secho(exc, fg="blue")
                click.secho(f"Failed to unify {s}", fg="red")

    klass = record_klass
    if config.output_summary_waterlevel_stats:
        klass = summary_record_klass

    unify_wrapper(klass, config, func)


if __name__ == "__main__":
    cfg = Config()
    cfg.county = "chaves"
    cfg.county = "eddy"
    cfg.output_summary_waterlevel_stats = True
    cfg.has_waterlevels = True

    # cfg.use_source_nwis = False
    cfg.use_source_ampapi = False
    cfg.use_source_isc_seven_rivers = False
    cfg.use_source_st2 = False
    cfg.use_source_ose_roswell = False
    # unify_sites(cfg)
    unify_waterlevels(cfg)

# ============= EOF =============================================
