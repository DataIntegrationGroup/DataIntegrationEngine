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
import shapely

from backend.config import Config, get_source
from backend.persister import CSVPersister, GeoJSONPersister, CloudStoragePersister


def health_check(source):
    source = get_source(source)
    if source:
        return bool(source.health())


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

        if site_source.check():
            print(f"Skipping {site_source}. check failed")

        schemas = site_source.discover()
        if not schemas:
            print(f"No schemas found for {site_source}")

            # in the future make discover required
            # return

        use_summarize = config.output_summary
        site_limit = config.site_limit

        sites = site_source.read()
        if not sites:
            print(f"No sites found for {site_source}")
            return

        for i, sites in enumerate(site_source.chunks(sites)):
            if site_limit and i > site_limit:
                break

            if use_summarize:
                summary_records = parameter_source.read(sites, use_summarize)
                if summary_records:
                    persister.records.extend(summary_records)
            else:
                results = parameter_source.read(sites, use_summarize)
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
    persister.finalize(config.output_name)


def get_sources_in_polygon(polygon):
    # polygon = shapely.wkt.loads(polygon)
    sources = get_sources()
    rets = []
    for source in sources:
        print(source)
        if source.intersects(polygon):
            rets.append(source.tag)
    return rets


def get_county_bounds(county):
    config = Config()
    config.county = county
    bp = config.bounding_wkt()
    return bp


def get_source_bounds(sourcekeys, as_str=False):
    config = Config()
    sourcekeys = sourcekeys.lower().replace("_", "")

    rets = []
    for sourcekey in sourcekeys.split(","):
        for sources in (config.analyte_sources(), config.water_level_sources()):
            for source, _ in sources:
                if source.__class__.__name__.lower().startswith(sourcekey):
                    bp = source.bounding_polygon
                    if bp and bp not in rets:
                        rets.append(bp)

    if rets:
        if len(rets) > 1:
            rets = shapely.GeometryCollection(rets)
        else:
            rets = rets[0]
        if as_str:
            rets = rets.wkt
        return rets


def get_sources(config=None):
    if config is None:
        config = Config()

    sources = []
    if config.analyte:
        allsources = config.analyte_sources()
    else:
        allsources = config.water_level_sources()

    for source, _ in allsources:
        if source.intersects(config.bounding_wkt()):
            sources.append(source)
    return sources


def generate_site_bounds():
    source = get_source("bernco")
    source.generate_bounding_polygon()


def analyte_unification_test():
    cfg = Config()
    cfg.county = "chaves"
    cfg.county = "eddy"

    cfg.analyte = "TDS"
    cfg.output_summary = True

    # analyte testing
    # cfg.use_source_wqp = False
    cfg.use_source_nmbgmr = False
    cfg.use_source_iscsevenrivers = False
    cfg.use_source_bor = False
    cfg.use_source_dwb = False
    cfg.site_limit = 10

    unify_analytes(cfg)


def waterlevel_unification_test():
    cfg = Config()
    cfg.county = "chaves"
    # cfg.county = "eddy"
    # cfg.bbox = "-104.5 32.5,-104 33"
    # cfg.start_date = "2020-01-01"
    # cfg.end_date = "2020-5-01"
    cfg.output_summary = False
    cfg.output_name = "test00112233"
    cfg.output_summary = True

    cfg.use_source_nwis = False
    cfg.use_source_nmbgmr = False
    cfg.use_source_iscsevenrivers = False
    # cfg.use_source_pvacd = False
    cfg.use_source_oseroswell = False
    # cfg.site_limit = 10

    unify_waterlevels(cfg)


def get_datastream(siteid):
    import httpx

    resp = httpx.get(
        f"https://st2.newmexicowaterdata.org/FROST-Server/v1.1/Locations({siteid})?$expand=Things/Datastreams"
    )
    obj = resp.json()
    return obj["Things"][0]["Datastreams"][0]


def get_datastreams():
    s = get_source("pvacd")
    for si in s.read_sites():
        ds = get_datastream(si.id)
        print(si, si.id, ds["@iot.id"])


if __name__ == "__main__":
    # test_waterlevel_unification()
    # root = logging.getLogger()
    # root.setLevel(logging.DEBUG)
    # shandler = logging.StreamHandler()
    # get_sources(Config())
    waterlevel_unification_test()
    # analyte_unification_test()
    # print(health_check("nwis"))
    # generate_site_bounds()

# ============= EOF =============================================
