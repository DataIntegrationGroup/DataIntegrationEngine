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

from backend import OutputFormat
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
        "--no-nmose-pod",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude NMOSE POD data. Default is to include",
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
    click.option(
        "--wkt",
        default="",
        help="Well known text (WKT) representation of a geometry. For example, 'POLYGON((x1 y1, x2 y2, x3 y3, x1 y1))'",
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
OUTPUT_TYPE_OPTIONS = [
    click.option(
        "--output-type",
        type=click.Choice(["summary", "timeseries_unified", "timeseries_separated"]),
        required=True,
        help="Output summary file, single unified timeseries file, or separated timeseries files",
    ),
]

OUTPUT_DIR_OPTIONS = [
    click.option(
        "--output-dir",
        default=".",
        help="Output root directory. Default is current directory",
    )
]

OUTPUT_FORMATS = sorted([of for of in OutputFormat])
OUTPUT_FORMAT_OPTIONS = [
    click.option(
        "--output-format",
        type=click.Choice(OUTPUT_FORMATS, case_sensitive=False),
        default="csv",
        help=f"Output file format for sites: {OUTPUT_FORMATS}. Default is csv",
    )
]

CONFIG_PATH_OPTIONS = [
    click.option(
        "--config-path",
        type=click.Path(exists=True),
        default=None,
        help="Path to config file. Default is config.yaml",
    ),
]


def add_options(options):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


@cli.command()
@click.argument(
    "parameter",
    type=click.Choice(PARAMETER_OPTIONS, case_sensitive=False),
    required=True,
)
@add_options(CONFIG_PATH_OPTIONS)
@add_options(OUTPUT_TYPE_OPTIONS)
@add_options(OUTPUT_DIR_OPTIONS)
@add_options(DT_OPTIONS)
@add_options(SPATIAL_OPTIONS)
@add_options(ALL_SOURCE_OPTIONS)
@add_options(DEBUG_OPTIONS)
@add_options(OUTPUT_FORMAT_OPTIONS)
def weave(
    parameter,
    config_path,
    output_type,
    output_dir,
    start_date,
    end_date,
    bbox,
    county,
    wkt,
    no_bernco,
    no_bor,
    no_cabq,
    no_ebid,
    no_nmbgmr_amp,
    no_nmed_dwb,
    no_nmose_isc_seven_rivers,
    no_nmose_pod,
    no_nmose_roswell,
    no_nwis,
    no_pvacd,
    no_wqp,
    site_limit,
    dry,
    yes,
    output_format,
):
    """
    Get parameter timeseries or summary data
    """
    # instantiate config and set up parameter
    config = setup_config(
        tag=parameter,
        config_path=config_path,
        bbox=bbox,
        county=county,
        wkt=wkt,
        site_limit=site_limit,
        dry=dry,
        output_format=output_format,
    )

    config.parameter = parameter

    # output type
    if output_type == "summary":
        summary = True
        timeseries_unified = False
        timeseries_separated = False
    elif output_type == "timeseries_unified":
        summary = False
        timeseries_unified = True
        timeseries_separated = False
    elif output_type == "timeseries_separated":
        summary = False
        timeseries_unified = False
        timeseries_separated = True
    else:
        click.echo(f"Invalid output type: {output_type}")
        return

    config.output_summary = summary
    config.output_timeseries_unified = timeseries_unified
    config.output_timeseries_separated = timeseries_separated

    config_agencies, false_agencies = config.get_config_and_false_agencies()

    for agency in false_agencies:
        setattr(config, f"use_source_{agency}", False)

    if config_path is None:
        lcs = locals()
        if config_agencies:
            for agency in config_agencies:
                setattr(config, f"use_source_{agency}", lcs.get(f"no_{agency}", False))
    # dates
    config.start_date = start_date
    config.end_date = end_date

    config.finalize()
    # setup logging here so that the path can be set to config.output_path
    setup_logging(path=config.output_path)

    config.report()
    if not dry:
        if not yes and not config.yes:
            # prompt user to continue
            if not click.confirm("Do you want to continue?", default=True):
                return

        if parameter.lower() == "waterlevels":
            unify_waterlevels(config)
        else:
            unify_analytes(config)
    return config


@cli.command()
@add_options(CONFIG_PATH_OPTIONS)
@add_options(SPATIAL_OPTIONS)
@add_options(OUTPUT_DIR_OPTIONS)
@add_options(ALL_SOURCE_OPTIONS)
@add_options(DEBUG_OPTIONS)
@add_options(OUTPUT_FORMAT_OPTIONS)
def sites(
    config_path,
    bbox,
    county,
    wkt,
    output_dir,
    no_bernco,
    no_bor,
    no_cabq,
    no_ebid,
    no_nmbgmr_amp,
    no_nmed_dwb,
    no_nmose_isc_seven_rivers,
    no_nmose_pod,
    no_nmose_roswell,
    no_nwis,
    no_pvacd,
    no_wqp,
    site_limit,
    dry,
    yes,
    output_format,
):
    """
    Get sites
    """
    config = setup_config(
        "sites", config_path, bbox, county, wkt, site_limit, dry, output_format
    )
    config_agencies = [
        "bernco",
        "bor",
        "cabq",
        "ebid",
        "nmbgmr_amp",
        "nmed_dwb",
        "nmose_isc_seven_rivers",
        "nmose_roswell",
        "nwis",
        "pvacd",
        "wqp",
        "nmose_pod",
    ]

    if config_path is None:
        lcs = locals()
        for agency in config_agencies:
            setattr(config, f"use_source_{agency}", lcs.get(f"no_{agency}", False))
        config.output_dir = output_dir

    config.sites_only = True
    config.finalize()
    # setup logging here so that the path can be set to config.output_path
    setup_logging(path=config.output_path)

    config.report()
    if not yes and not config.yes:
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
def sources(sources, bbox, wkt, county):
    """
    List available sources
    """
    from backend.unifier import get_sources

    config = Config()
    if county:
        config.county = county
    elif bbox:
        config.bbox = bbox
    elif wkt:
        config.wkt = wkt

    parameter = sources
    config.parameter = parameter
    config_agencies, false_agencies = config.get_config_and_false_agencies()

    for agency in false_agencies:
        setattr(config, f"use_source_{agency}", False)

    sources = get_sources(config)
    for s in sources:
        click.echo(s)


def setup_config(
    tag,
    config_path,
    bbox,
    county,
    wkt,
    site_limit,
    dry,
    output_format=OutputFormat.CSV,
):
    config = Config(path=config_path)

    if county:
        click.echo(f"Getting {tag} for county {county}")
        config.county = county
    elif bbox:
        click.echo(f"Getting {tag} for bounding box {bbox}")
        # bbox = -105.396826 36.219290, -106.024162 35.384307
        config.bbox = bbox
    elif wkt:
        click.echo(f"Getting {tag} for WKT {wkt}")
        config.wkt = wkt

    if site_limit:
        config.site_limit = int(site_limit)
    else:
        config.site_limit = None
    config.dry = dry

    config.output_format = output_format.value

    return config


# ============= EOF =============================================
