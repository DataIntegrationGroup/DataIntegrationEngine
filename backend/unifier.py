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
from backend.persister import CSVPersister, GeoJSONPersister, CloudStoragePersister


def unify_sites(config):
    print("Unifying sites")

    # def func(config, persister):
    #     for source in config.site_sources():
    #         s = source()
    #         persister.load(s.read(config))

    # _unify_wrapper(config, func)


def unify_analytes(config):
    print("Unifying analytes")
    config.report()
    config.validate()

    if not config.dry:
        _unify_parameter(config, config.analyte_sources())

    return True


def unify_waterlevels(config):
    print("Unifying waterlevels")

    config.report()
    config.validate()

    if not config.dry:
        _unify_parameter(config, config.water_level_sources())

    return True


def _perister_factory(config):
    persister_klass = CSVPersister
    if config.use_cloud_storage:
        persister_klass = CloudStoragePersister
    elif config.use_csv:
        persister_klass = CSVPersister
    elif config.use_geojson:
        persister_klass = GeoJSONPersister

    return persister_klass()


# def _unify_wrapper(config, func):
#     persister = _perister_factory(config)
#     func(persister)
#     persister.save(config.output_path)


def _site_wrapper(site_source, parameter_source, persister, config):
    try:
        use_summarize = config.output_summary
        site_limit = config.site_limit

        sites = site_source.read_sites()
        if not sites:
            print(f"No sites found for {site_source}")
            return

        for i, sites in enumerate(site_source.chunks(sites)):
            if site_limit and i > site_limit:
                break

            if use_summarize:
                summary_records = parameter_source.load(sites, use_summarize)
                if summary_records:
                    persister.records.extend(summary_records)
            else:
                results = parameter_source.load(sites, use_summarize)
                if results is None:
                    continue

                # combine sites that only have one record
                for site, records in results:
                    if len(records) == 1:
                        persister.combined.append((site, records[0]))
                    else:
                        persister.timeseries.append((site, records))

    except BaseException:
        import traceback

        exc = traceback.format_exc()
        print(exc)
        print(f"Failed to unify {site_source}")


def _unify_parameter(
    config,
    sources,
):
    use_summarize = config.output_summary
    persister = _perister_factory(config)
    for site_source, ss in sources:
        _site_wrapper(site_source, ss, persister, config)
    if use_summarize:
        persister.save(config.output_path)
    else:
        persister.dump_combined(f"{config.output_path}.combined")
        persister.dump_timeseries(f"{config.output_path}_timeseries")


def analyte_unification_test():
    cfg = Config()
    cfg.county = "chaves"
    cfg.county = "eddy"

    cfg.analyte = "TDS"
    cfg.output_summary = True

    # analyte testing
    # cfg.use_source_wqp = False
    cfg.use_source_ampapi = False
    cfg.use_source_isc_seven_rivers = False
    cfg.use_source_bor = False
    cfg.use_source_dwb = False
    cfg.site_limit = 10

    unify_analytes(cfg)


def waterlevel_unification_test():
    cfg = Config()
    cfg.county = "chaves"
    cfg.county = "eddy"
    cfg.bbox = "-104.5 32.5,-104 33"
    cfg.start_date = "2020-01-01"
    cfg.end_date = "2020-5-01"
    cfg.output_summary = True

    cfg.use_source_nwis = False
    # cfg.use_source_ampapi = False
    cfg.use_source_isc_seven_rivers = False
    cfg.use_source_st2 = False
    cfg.use_source_ose_roswell = False
    # cfg.site_limit = 10

    unify_waterlevels(cfg)


if __name__ == "__main__":
    # test_waterlevel_unification()
    # root = logging.getLogger()
    # root.setLevel(logging.DEBUG)
    # shandler = logging.StreamHandler()

    waterlevel_unification_test()
    # analyte_unification_test()

# ============= EOF =============================================
