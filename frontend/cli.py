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
from backend.constants import ANALYTE_CHOICES
from frontend.unifier import unify_sites, unify_waterlevels, unify_analytes


@click.group()
def cli():
    pass


SOURCE_OPTIONS = [
    click.option(
        "--no-amp",
        is_flag=True,
        default=True,
        show_default=True,
        help="Include/Exclude AMP data. Default is to include",
    ),
    click.option(
        "--no-nwis",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude NWIS data. Default is to include",
    ),
    click.option(
        "--no-st2",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude ST2 data. Default is to include",
    ),
    click.option(
        "--no-isc-seven-rivers",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude ISC Seven Rivers data. Default is to include",
    ),
    click.option(
        "--no-bor",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude BOR data. Default is to include",
    ),
    click.option(
        "--no-wqp",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude WQP data. Default is to include",
    ),
    click.option(
        "--no-ckan",
        is_flag=True,
        default=True,
        show_default=True,
        help="Exclude CKAN data. Default is to include",
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
@add_options(SPATIAL_OPTIONS)
@click.option(
    "--timeseries",
    is_flag=True,
    default=False,
    show_default=True,
    help="Include timeseries data",
)
@add_options(SOURCE_OPTIONS)
def waterlevels(
    bbox,
    county,
    timeseries,
    no_amp,
    no_nwis,
    no_st2,
    no_isc_seven_rivers,
    no_bor,
    no_wqp,
    no_ckan,
):
    config = setup_config("waterlevels", bbox, county)

    config.output_summary = not timeseries

    config.use_source_ampapi = no_amp
    config.use_source_nwis = no_nwis
    config.use_source_st2 = no_st2
    config.use_source_isc_seven_rivers = no_isc_seven_rivers
    config.use_source_bor = no_bor
    config.use_source_wqp = no_wqp
    config.use_source_ose_roswell = no_ckan

    unify_waterlevels(config)


@cli.command()
@click.argument("analyte", type=click.Choice(ANALYTE_CHOICES))
@add_options(SPATIAL_OPTIONS)
@add_options(SOURCE_OPTIONS)
def analytes(
    analyte, bbox, county, no_amp, no_nwis, no_st2, no_isc_seven_rivers, no_bor, no_wqp
):
    config = setup_config(f"analytes ({analyte})", bbox, county)
    config.analyte = analyte

    config.use_source_ampapi = no_amp
    config.use_source_nwis = no_nwis
    config.use_source_st2 = no_st2
    config.use_source_isc_seven_rivers = no_isc_seven_rivers
    config.use_source_bor = no_bor
    config.use_source_wqp = no_wqp

    unify_analytes(config)


def setup_config(tag, bbox, county):
    config = Config()
    if county:
        click.echo(f"Getting {tag} for county {county}")
        config.county = county
    elif bbox:
        click.echo(f"Getting {tag} for bounding box {bbox}")

        # bbox = -105.396826 36.219290, -106.024162 35.384307
        config.bbox = bbox

    return config


# ============= EOF =============================================
