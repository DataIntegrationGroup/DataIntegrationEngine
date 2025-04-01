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
import sys

import click

from backend.config import Config
from backend.constants import PARAMETER_OPTIONS
from backend.unifier import unify_sites, unify_waterlevels, unify_analytes

from backend.logger import setup_logging


# setup_logging()


@click.group()
def cli():
    pass


ALL_SOURCE_OPTIONS = [
    click.option(
        "--no-bernco",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude Bernalillo County Water Authority data. Default is to include",
    ),
    click.option(
        "--no-bor",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude BoR data. Default is to include",
    ),
    click.option(
        "--no-cabq",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude CABQ data. Default is to include",
    ),
    click.option(
        "--no-ebid",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude EBID data. Default is to include",
    ),
    click.option(
        "--no-nmbgmr-amp",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude NMBGMR AMP data. Default is to include",
    ),
    click.option(
        "--no-nmed-dwb",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude NMED DWB data. Default is to include",
    ),
    click.option(
        "--no-nmose-isc-seven-rivers",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude NMOSE ISC Seven Rivers data. Default is to include",
    ),
    click.option(
        "--no-nmose-roswell",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude NMOSE Roswell data. Default is to include",
    ),
    click.option(
        "--no-nwis",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude NWIS data. Default is to include",
    ),
    click.option(
        "--no-pvacd",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude PVACD data. Default is to include",
    ),
    click.option(
        "--no-wqp",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude WQP data. Default is to include",
    ),
]

SPATIAL_OPTIONS = [
    click.option(
        "--bbox",
        default="",
        help="Bounding box in the form 'x1 y1, x2 y2'",
    ),
    click.option(
        "--county",
        default="",
        help="New Mexico county name",
    ),
]
DEBUG_OPTIONS = [
    click.option(
        "--site-limit",
        type=int,
        default=None,
        help="Max number of sites to return",
    ),
    click.option(
        "--dry",
        is_flag=True,
        default=False,
        help="Dry run. Do not execute unifier. Used by unit tests",
    ),
    click.option(
        "--yes",
        is_flag=True,
        default=False,
        help="Do not ask for confirmation before running",
    ),
]

DT_OPTIONS = [
    click.option(
        "--start-date",
        default="",
        help="Start date in the form 'YYYY', 'YYYY-MM', 'YYYY-MM-DD', 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'",
    ),
    click.option(
        "--end-date",
        default="",
        help="End date in the form 'YYYY', 'YYYY-MM', 'YYYY-MM-DD', 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'",
    ),
]

TIMESERIES_OPTIONS = [
    click.option(
        "--separated_timeseries",
        is_flag=True,
        default=False,
        show_default=True,
        help="Output separate timeseries files for every site",
    ),
    click.option(
        "--unified_timeseries",
        is_flag=True,
        default=False,
        show_default=True,
        help="Output single timeseries file, which includes all sites",
    ),
]

OUTPUT_OPTIONS = [
    click.option(
        "--output",
        type=click.Choice(["summary", "timeseries_unified", "timeseries_separated"]),
        required=True,
        help="Output summary file, single unified timeseries file, or separated timeseries files",
    ),

]
PERSISTER_OPTIONS = [
    click.option(
        "--output-dir",
        default=".",
        help="Output root directory. Default is current directory",
    )
]


def add_options(options):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


@cli.command()
@click.argument(
    "weave",
    type=click.Choice(PARAMETER_OPTIONS, case_sensitive=False),
    required=True,
)
@add_options(OUTPUT_OPTIONS)
@add_options(PERSISTER_OPTIONS)
@add_options(DT_OPTIONS)
@add_options(SPATIAL_OPTIONS)
@add_options(ALL_SOURCE_OPTIONS)
@add_options(DEBUG_OPTIONS)
def weave(
        weave,
        output,
        output_dir,
        start_date,
        end_date,
        bbox,
        county,
        no_bernco,
        no_bor,
        no_cabq,
        no_ebid,
        no_nmbgmr_amp,
        no_nmed_dwb,
        no_nmose_isc_seven_rivers,
        no_nmose_roswell,
        no_nwis,
        no_pvacd,
        no_wqp,
        site_limit,
        dry,
):
    """
    Get parameter timeseries or summary data
    """
    parameter = weave
    # instantiate config and set up parameter
    config = setup_config(f"{parameter}", bbox, county, site_limit, dry)
    config.parameter = parameter

    # # make sure config.output_name is properly set
    # config.update_output_name()
    #
    # # make output_path now so that die.log can be written to it live
    # config.make_output_path()

    # output type
    if output == "summary":
        summary = True
        timeseries_unified = False
        timeseries_separated = False
    elif output == "timeseries_unified":
        summary = False
        timeseries_unified = True
        timeseries_separated = False
    elif output == "timeseries_separated":
        summary = False
        timeseries_unified = False
        timeseries_separated = True
    else:
        click.echo(f"Invalid output type: {output}")
        return

    config.output_summary = summary
    config.output_timeseries_unified = timeseries_unified
    config.output_timeseries_separated = timeseries_separated

    false_agencies = []
    config_agencies = []
    # sources
    if parameter == "waterlevels":
        config_agencies = ["bernco", "cabq", "ebid", "nmbgmr_amp", "nmed_dwb",
                           "nmose_isc_seven_rivers", "nmose_roswell", "nwis", "pvacd", "wqp"]

        false_agencies = ['bor', 'nmed_dwb']

    elif parameter == "carbonate":
        config_agencies = ['nmbgmr_amp', 'wqp']
        false_agencies = ['bor', 'bernco', 'cabq', 'ebid', 'nmed_dwb',
                          'nmose_isc_seven_rivers', 'nmose_roswell', 'nwis', 'pvacd']

    elif parameter in ["arsenic", "uranium"]:
        config_agencies = ['bor', 'nmbgmr_amp', 'nmed_dwb', 'wqp']
        false_agencies = ['bernco', 'cabq', 'ebid', 'nmose_isc_seven_rivers',
                          'nmose_roswell', 'nwis', 'pvacd']


    elif parameter in [
        "bicarbonate",
        "calcium",
        "chloride",
        "fluoride",
        "magnesium",
        "nitrate",
        "ph",
        "potassium",
        "silica",
        "sodium",
        "sulfate",
        "tds",
    ]:
        config_agencies = ['bor', 'nmbgmr_amp', 'nmed_dwb', 'nmose_isc_seven_rivers', 'wqp']
        false_agencies = ['bernco', 'cabq', 'ebid', 'nmose_roswell', 'nwis', 'pvacd']

    if false_agencies:
        for agency in false_agencies:
            setattr(config, f"use_source_{agency}", False)

    lcs = locals()
    if config_agencies:
        for agency in config_agencies:
            setattr(config, f"use_source_{agency}", lcs.get(f'no_{agency}', False))
    # dates
    config.start_date = start_date
    config.end_date = end_date

    config.finalize()
    # setup logging here so that the path can be set to config.output_path
    setup_logging(path=config.output_path)

    if not dry:
        config.report()
        # prompt user to continue
        if not click.confirm("Do you want to continue?", default=True):
            return

    if parameter.lower() == "waterlevels":
        unify_waterlevels(config)
    else:
        unify_analytes(config)


@cli.command()
@add_options(SPATIAL_OPTIONS)
@add_options(PERSISTER_OPTIONS)
@add_options(ALL_SOURCE_OPTIONS)
@add_options(DEBUG_OPTIONS)
def wells(bbox, county,
          output_dir,
          no_bernco,
          no_bor,
          no_cabq,
          no_ebid,
          no_nmbgmr_amp,
          no_nmed_dwb,
          no_nmose_isc_seven_rivers,
          no_nmose_roswell,
          no_nwis,
          no_pvacd,
          no_wqp,
          site_limit,
          dry,
          yes):
    """
    Get locations
    """

    config = setup_config("sites", bbox, county, site_limit, dry)
    config_agencies = ["bernco", "bor", "cabq", "ebid", "nmbgmr_amp", "nmed_dwb",
                       "nmose_isc_seven_rivers", "nmose_roswell", "nwis", "pvacd",
                       "wqp"]
    lcs = locals()
    for agency in config_agencies:
        setattr(config, f"use_source_{agency}", lcs.get(f'no_{agency}', False))

    config.sites_only = True
    config.output_dir = output_dir
    config.finalize()
    # setup logging here so that the path can be set to config.output_path
    setup_logging(path=config.output_path)

    config.report()
    if not yes:
        # prompt user to continue
        if not click.confirm("Do you want to continue?", default=True):
            return

    unify_sites(config)


@cli.command()
@click.argument(
    "sources",
    type=click.Choice(PARAMETER_OPTIONS, case_sensitive=False),
    required=True,
)
@add_options(SPATIAL_OPTIONS)
def sources(sources, bbox, county):
    """
    List available sources
    """
    from backend.unifier import get_sources

    config = Config()
    if county:
        config.county = county
    elif bbox:
        config.bbox = bbox

    parameter = sources
    config.parameter = parameter
    sources = get_sources(config)
    for s in sources:
        click.echo(s)


def setup_config(tag, bbox, county, site_limit, dry):
    config = Config()
    if county:
        click.echo(f"Getting {tag} for county {county}")
        config.county = county
    elif bbox:
        click.echo(f"Getting {tag} for bounding box {bbox}")
        # bbox = -105.396826 36.219290, -106.024162 35.384307
        config.bbox = bbox

    config.site_limit = site_limit
    config.dry = dry

    return config

# ============= EOF =============================================
