# ===============================================================================
# Author:  Jake Ross
# Copyright 2025 New Mexico Bureau of Geology & Mineral Resources
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
import os
import dagster as dg

from backend.config import Config
from backend.logger import setup_logging
from backend.unifier import unify_analytes, unify_waterlevels


@dg.asset
def tds():
    """TDS asset"""
    config_path = os.path.join(
        os.path.dirname(__file__), 'config', 'tds_config.dev.yaml'
    )

    config = Config(path=config_path)
    config.parameter = 'tds'
    config.finalize()

    # setup logging here so that the path can be set to config.output_path
    #setup_logging(path=config.output_path)

    # with geoserver.get_connection() as conn:
    unify_analytes(config)


@dg.asset
def summary_waterlevels():
    """Summary water levels asset"""
    config_path = os.path.join(
        os.path.dirname(__file__), 'config', 'summary_waterlevels_config.dev.yaml'
    )

    config = Config(path=config_path)
    config.parameter = 'waterlevels'
    config.finalize()

    # setup logging here so that the path can be set to config.output_path
    #setup_logging(path=config.output_path)

    # with geoserver.get_connection() as conn:
    unify_waterlevels(config)


defs = dg.Definitions(
    assets=[tds, summary_waterlevels],
    schedules=[
        dg.ScheduleDefinition(
            name='tds_schedule',
            # job=tds.to_job(),
            target=dg.AssetSelection.keys("tds"),
            cron_schedule='0 0 * * *',
            execution_timezone='America/Denver',
        )
    ],
    # resources={
    #     'config': dg.ResourceDefinition.hardcoded_resource(Config()),
    # },
)
# ============= EOF =============================================
