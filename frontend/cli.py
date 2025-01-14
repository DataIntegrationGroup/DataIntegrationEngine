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
from backend.constants import ANALYTE_CHOICES
from backend.unifier import unify_sites, unify_waterlevels, unify_analytes

from backend.logging import setup_logging

setup_logging()


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
        "--no-nmbgmr-amp",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude NMBGMR AMP data. Default is to include",
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
    )
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


def add_options(options):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


@cli.command()
@add_options(SPATIAL_OPTIONS)
def wells(bbox, county):
    """
    Get locations
    """

    config = setup_config("sites", bbox, county)
    unify_sites(config)


@cli.command()
@add_options(TIMESERIES_OPTIONS)
@add_options(DT_OPTIONS)
@add_options(SPATIAL_OPTIONS)
@add_options(ALL_SOURCE_OPTIONS)
@add_options(DEBUG_OPTIONS)
def waterlevels(
    separated_timeseries,
    unified_timeseries,
    start_date,
    end_date,
    bbox,
    county,
    no_bernco,
    no_bor, # has no water levels
    no_nmbgmr_amp,
    no_nmed_dwb, # has no water levels
    no_nmose_isc_seven_rivers,
    no_nmose_roswell,
    no_nwis,
    no_pvacd,
    no_wqp, # has no water levels
    site_limit,
    dry,
):
    # output type
    if separated_timeseries or unified_timeseries:
        timeseries = True
    else:
        timeseries = False

    # instantiate config
    config = setup_config("waterlevels", timeseries, bbox, county, site_limit, dry)

    config.output_single_timeseries = unified_timeseries

    # sources
    config.use_source_bernco = no_bernco
    config.use_source_nmbgmr_amp = no_nmbgmr_amp
    config.use_source_nmose_isc_seven_rivers = no_nmose_isc_seven_rivers
    config.use_source_nmose_roswell = no_nmose_roswell
    config.use_source_nwis = no_nwis
    config.use_source_pvacd = no_pvacd

    config.use_source_bor = False
    config.use_source_nmed_dwb = False
    config.use_source_wqp = False
    
    # dates
    config.start_date = start_date
    config.end_date = end_date

    if not dry:
        config.report()
        # prompt user to continue
        if not click.confirm("Do you want to continue?", default=True):
            return

    unify_waterlevels(config)


@cli.command()
@click.argument("analyte", type=click.Choice(ANALYTE_CHOICES))
@add_options(TIMESERIES_OPTIONS)
@add_options(DT_OPTIONS)
@add_options(SPATIAL_OPTIONS)
@add_options(ALL_SOURCE_OPTIONS)
@add_options(DEBUG_OPTIONS)
def analytes(
    analyte,
    separated_timeseries,
    unified_timeseries,
    start_date,
    end_date,
    bbox,
    county,
    no_bernco,  # has no analyte measurements
    no_bor,
    no_nmbgmr_amp,
    no_nmed_dwb,
    no_nmose_isc_seven_rivers,
    no_nmose_roswell,   # has no analyte measurements
    no_nwis,            # has no analyte measurements
    no_pvacd,           # has no analyte measurements
    no_wqp,
    site_limit,
    dry,
):
    # output type
    if separated_timeseries or unified_timeseries:
        timeseries = True
    else:
        timeseries = False

    # instantiate config
    config = setup_config(
        f"analytes ({analyte})", timeseries, bbox, county, site_limit, dry
    )
    config.output_single_timeseries = unified_timeseries
    
    # sources
    config.use_source_bor = no_bor
    config.use_source_nmbgmr_amp = no_nmbgmr_amp
    config.use_source_nmed_dwb = no_nmed_dwb
    config.use_source_nmose_isc_seven_rivers = no_nmose_isc_seven_rivers
    config.use_source_wqp = no_wqp

    config.use_source_bernco = False
    config.use_source_nmose_roswell = False
    config.use_source_nwis = False
    config.use_source_pvacd = False
    

    # analyte
    config.analyte = analyte

    # dates
    config.start_date = start_date
    config.end_date = end_date

    

    if not dry:
        config.report()
        # prompt user to continue
        if not click.confirm("Do you want to continue?", default=True):
            return

    unify_analytes(config)


@cli.command()
@add_options(SPATIAL_OPTIONS)
def sources(bbox, county):
    """
    List available sources
    """
    from backend.unifier import get_sources

    config = Config()
    if county:
        config.county = county
    elif bbox:
        config.bbox = bbox

    sources = get_sources(config)
    for s in sources:
        click.echo(s)


def setup_config(tag, timeseries, bbox, county, site_limit, dry):
    config = Config()
    if county:
        click.echo(f"Getting {tag} for county {county}")
        config.county = county
    elif bbox:
        click.echo(f"Getting {tag} for bounding box {bbox}")
        # bbox = -105.396826 36.219290, -106.024162 35.384307
        config.bbox = bbox

    config.output_summary = not timeseries
    config.site_limit = site_limit
    config.dry = dry

    return config


# ============= EOF =============================================
