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
from frontend.unifier import unify_sites, unify_waterlevels, unify_analytes


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--bbox",
    default="",
    help="Bounding box in the form 'x1 y1, x2 y2'",
)
@click.option(
    "--county",
    default="",
    help="New Mexico county name",
)
def wells(bbox, county):
    """
    Get locations
    """

    config = setup_config("sites", bbox, county)
    unify_sites(config)


@cli.command()
@click.option(
    "--bbox",
    default="",
    help="Bounding box in the form 'x1 y1, x2 y2'",
)
@click.option(
    "--county",
    default="",
    help="New Mexico county name",
)
@click.option(
    "--summarize/--no-summarize",
    is_flag=True,
    default=False,
    show_default=True,
    help="Summarize water levels",
)
def waterlevels(bbox, county, summarize):
    config = setup_config("waterlevels", bbox, county)
    print("summarize", summarize, type(summarize))
    config.output_summary_waterlevel_stats = summarize
    unify_waterlevels(config)


@cli.command()
@click.argument("analyte")
@click.option(
    "--bbox",
    default="",
    help="Bounding box in the form 'x1 y1, x2 y2'",
)
@click.option(
    "--county",
    default="",
    help="New Mexico county name",
)
def analytes(analyte, bbox, county):
    config = setup_config(f"analytes ({analyte})", bbox, county)
    config.analyte = analyte
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
