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
from frontend.unifier import unify_sites, unify_waterlevels


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--bbox",
    default="",
    help="Bounding box in the form 'x1 y1, x2 y2'",
)
def locations(bbox):
    """
    Get locations
    """

    click.echo(f"Getting locations for bounding box {bbox}")

    config = Config()
    # bbox = -105.396826 36.219290, -106.024162 35.384307
    config.bbox = bbox

    unify_sites(config)


@cli.command()
@click.option(
    "--bbox",
    default="",
    help="Bounding box in the form 'x1 y1, x2 y2'",
)
def waterlevels(bbox):
    click.echo(f"Getting waterlevels for bounding box {bbox}")

    config = Config()
    # bbox = -105.396826 36.219290, -106.024162 35.384307
    config.bbox = bbox

    unify_waterlevels(config)


# ============= EOF =============================================
