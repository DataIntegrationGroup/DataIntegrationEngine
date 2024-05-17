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
    WaterLevelSummaryRecord,
    AnalyteSummaryRecord,
)


def perister_factory(config):
    persister_klass = CSVPersister
    if config.use_csv:
        persister_klass = CSVPersister
    elif config.use_geojson:
        persister_klass = GeoJSONPersister

    return persister_klass()


def unify_wrapper(config, func):
    persister = perister_factory(config)
    func(config, persister)
    persister.save(config.output_path)


def unify_sites(config):
    print("unifying")

    def func(config, persister):
        for source in config.site_sources():
            s = source()
            persister.load(s.read(config))

    unify_wrapper(config, func)


def unify_analytes(config):
    def func(config, persister):
        for site_source, ss in config.analyte_sources():
            sites = site_source.read_sites()
            for i, sites in enumerate(site_source.chunks(sites)):
                if config.output_summary_analyte_stats:
                    summary_records = ss.summarize(sites)
                    if summary_records:
                        persister.records.extend(summary_records)
                    # break

    unify_wrapper(config, func)


def unify_waterlevels(config):
    def func(config, persister):
        sources = config.water_level_sources()
        for site_source, ss in sources:
            try:
                sites = site_source.read_sites()
                for i, sites in enumerate(site_source.chunks(sites)):
                    if config.output_summary_waterlevel_stats:
                        summary_records = ss.summarize(sites)
                        if summary_records:
                            persister.records.extend(summary_records)
                    # else:
                    #     for wl in ss.read(sites, config):
                    #         persister.records.append(wl)
            except BaseException:
                import traceback

                exc = traceback.format_exc()
                click.secho(exc, fg="blue")
                click.secho(f"Failed to unify {site_source}", fg="red")

    unify_wrapper(config, func)


def test_analyte_unification():
    cfg = Config()
    cfg.county = "chaves"
    cfg.county = "eddy"

    cfg.analyte = "TDS"
    cfg.output_summary_analyte_stats = True

    # analyte testing
    # cfg.use_source_wqp = False
    cfg.use_source_ampapi = False
    cfg.use_source_isc_seven_rivers = False
    cfg.use_source_bor = False

    unify_analytes(cfg)


def test_waterlevel_unification():
    cfg = Config()
    cfg.county = "chaves"
    cfg.county = "eddy"

    cfg.output_summary_waterlevel_stats = True

    cfg.use_source_nwis = False
    # cfg.use_source_ampapi = False
    cfg.use_source_isc_seven_rivers = False
    cfg.use_source_st2 = False
    cfg.use_source_ose_roswell = False

    # unify_sites(cfg)
    unify_waterlevels(cfg)


if __name__ == "__main__":
    test_waterlevel_unification()
    # test_analyte_unification()

# ============= EOF =============================================
