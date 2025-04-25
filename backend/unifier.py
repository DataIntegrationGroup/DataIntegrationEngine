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

from backend.config import Config, get_source, OutputFormat
from backend.logger import setup_logging
from backend.constants import WATERLEVELS
from backend.persister import BasePersister
from backend.persisters.geoserver import GeoServerPersister
from backend.source import BaseSiteSource


def health_check(source: BaseSiteSource) -> bool | None:
    """
    Determines if data can be returned from the source (if it is healthy)

    Parameters
    -------
    source: BaseSiteSource
        The site source to check, specific to the source being queried

    Returns
    -------
    bool
        True if the source is healthy, else False
    """
    source = get_source(source)
    if source:
        return bool(source.health())
    else:
        return None


def unify_analytes(config):
    print("Unifying analytes\n")
    # config.report() -- report is done in cli.py, no need to do it twice
    config.validate()

    if not config.dry:
        _unify_parameter(config, config.analyte_sources())

    return True


def unify_waterlevels(config):
    print("Unifying waterlevels\n")

    # config.report() -- report is done in cli.py, no need to do it twice
    config.validate()

    if not config.dry:
        _unify_parameter(config, config.water_level_sources())

    return True


def unify_sites(config):
    print("Unifying sites only\n")

    # config.report() -- report is done in cli.py, no need to do it twice
    config.validate()

    if not config.dry:
        _unify_parameter(config, config.all_site_sources())

    return True


# def _perister_factory(config):
#     """
#     Determines the type of persister to use based on the configuration. The
#     persister types are:

#     - CSVPersister
#     - CloudStoragePersister
#     - GeoJSONPersister

#     Parameters
#     -------
#     config: Config
#         The configuration object

#     Returns
#     -------
#     Persister
#         The persister object to use
#     """
#     persister_klass = CSVPersister
#     if config.use_cloud_storage:
#         persister_klass = CloudStoragePersister
#     elif config.output_format == OutputFormat.CSV:
#         persister_klass = CSVPersister
#     elif config.output_format == OutputFormat.GEOJSON:
#         persister_klass = GeoJSONPersister
#     elif config.output_format == OutputFormat.GEOSERVER:
#         persister_klass = GeoServerPersister

#     return persister_klass(config)


# def _unify_wrapper(config, func):
#     persister = _perister_factory(config)
#     func(persister)
#     persister.save(config.output_path)


def _site_wrapper(site_source, parameter_source, persister, config):

    try:
        # TODO: fully develop checks/discoveries below
        # if not site_source.check():
        #     print(f"Skipping {site_source}. check failed")

        # schemas = site_source.discover()
        # if not schemas:
        #     print(f"No schemas found for {site_source}")

        # in the future make discover required
        # return

        use_summarize = config.output_summary
        site_limit = config.site_limit

        sites = site_source.read()

        if not sites:
            return

        sites_with_records_count = 0
        start_ind = 0
        end_ind = 0
        first_flag = True

        if config.sites_only:
            persister.sites.extend(sites)
        else:
            for site_records in site_source.chunks(sites):
                if type(site_records) == list:
                    n = len(site_records)
                    if first_flag:
                        first_flag = False
                    else:
                        start_ind = end_ind + 1

                    end_ind += n

                if use_summarize:
                    summary_records = parameter_source.read(
                        site_records, use_summarize, start_ind, end_ind
                    )
                    if summary_records:
                        persister.records.extend(summary_records)
                        sites_with_records_count += len(summary_records)
                    else:
                        continue
                else:
                    results = parameter_source.read(
                        site_records, use_summarize, start_ind, end_ind
                    )
                    # no records are returned if there is no site record for parameter
                    # or if the record isn't clean (doesn't have the correct fields)
                    # don't count these sites to apply to site_limit
                    if results is None or len(results) == 0:
                        continue
                    else:
                        sites_with_records_count += len(results)

                    for site, records in results:
                        persister.timeseries.append(records)
                        persister.sites.append(site)

                if site_limit:
                    if sites_with_records_count >= site_limit:
                        # remove any extra sites that were gathered. removes 0 if site_limit is not exceeded
                        num_sites_to_remove = sites_with_records_count - site_limit

                        # if sites_with_records_count == sit_limit then num_sites_to_remove = 0
                        # and calling list[:0] will retur an empty list, so subtract
                        # num_sites_to_remove from the length of the list
                        # to remove the last num_sites_to_remove sites
                        if use_summarize:
                            persister.records = persister.records[
                                : len(persister.records) - num_sites_to_remove
                            ]
                        else:
                            persister.timeseries = persister.timeseries[
                                : len(persister.timeseries) - num_sites_to_remove
                            ]
                            persister.sites = persister.sites[
                                : len(persister.sites) - num_sites_to_remove
                            ]
                        break

    except BaseException:
        import traceback

        exc = traceback.format_exc()
        config.warn(exc)
        config.warn(f"Failed to unify {site_source}")


def _unify_parameter(
    config,
    sources,
):

    if config.output_format == OutputFormat.GEOSERVER:
        persister = GeoServerPersister(config)
    else:
        persister = BasePersister(config)

    for site_source, parameter_source in sources:
        _site_wrapper(
            site_source,
            parameter_source,
            persister,
            config,
        )

    if config.output_summary:
        persister.dump_summary(config.output_path)
    elif config.output_timeseries_unified:
        persister.dump_timeseries_unified(config.output_path)
        persister.dump_sites(config.output_path)
    elif config.sites_only:
        persister.dump_sites(config.output_path)
    else:  # config.output_timeseries_separated
        persister.dump_timeseries_separated(config.output_path)
        persister.dump_sites(config.output_path)

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
    if config.parameter == WATERLEVELS:
        allsources = config.water_level_sources()
    else:
        allsources = config.analyte_sources()

    for source, _ in allsources:
        if config.wkt or config.bbox or config.county:
            if source.intersects(config.bounding_wkt()):
                sources.append(source)
        else:
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
    cfg.use_source_wqp = False
    # cfg.use_source_nmbgmr = False
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
    # cfg.output_summary = True
    cfg.output_single_timeseries = True

    cfg.use_source_nwis = False
    cfg.use_source_nmbgmr = False
    cfg.use_source_iscsevenrivers = False
    cfg.use_source_pvacd = False
    # cfg.use_source_oseroswell = False
    cfg.use_source_bernco = False
    cfg.use_source_iscsevenrivers = False
    cfg.use_source_nmose_isc_seven_rivers = False
    cfg.use_source_ebid = False
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


# if __name__ == "__main__":
# test_waterlevel_unification()
# root = logging.getLogger()
# root.setLevel(logging.DEBUG)
# shandler = logging.StreamHandler()
# get_sources(Config())
# setup_logging()
# site_unification_test()
# waterlevel_unification_test()
# analyte_unification_test()
# print(health_check("nwis"))
# generate_site_bounds()

# ============= EOF =============================================
