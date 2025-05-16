# ===============================================================================
# Author:  Jake Ross
# Copyright 2025 New Mexico Bureau of Geology & Mineral Resources
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================


import dagster as dg

from dagsterio import base_analyte_asset, base_waterlevels_asset
from dagsterio.config.source_constants import ALL_SOURCES
from dagsterio.nmbgmr import tds_request_sensor, nmbgmr_tds, nmbgmr_waterlevels


@dg.asset
def all_tds():
    """TDS asset"""

    base_analyte_asset(
        'tds',
        sources=ALL_SOURCES
    )


@dg.asset
def all_waterlevels():
    """Summary water levels asset"""
    base_waterlevels_asset(
        sources=ALL_SOURCES,
    )

defs = dg.Definitions(
    sensors=[tds_request_sensor],
    assets=[all_tds, all_waterlevels, nmbgmr_tds, nmbgmr_waterlevels],
    schedules=[
        dg.ScheduleDefinition(
            name='all_tds',
            target=dg.AssetSelection.keys("all_tds"),
            cron_schedule='0 11 * * *',
            execution_timezone='America/Denver',
        ),
        dg.ScheduleDefinition(
            name='all_waterlevels',
            target=dg.AssetSelection.keys("all_waterlevels"),
            cron_schedule='0 12 * * *',
            execution_timezone='America/Denver',
        ),

        # dg.ScheduleDefinition(
        #     name='nmbgmr_tds',
        #     target=dg.AssetSelection.keys("nmbgmr_tds"),
        #     cron_schedule='0 3 * * *',
        #     execution_timezone='America/Denver',
        # ),
        # dg.ScheduleDefinition(
        #     name='nmbgmr_waterlevels',
        #     target=dg.AssetSelection.keys("nmbgmr_waterlevels"),
        #     cron_schedule='0 4 * * *',
        #     execution_timezone='America/Denver',
        # ),
    ],
    # resources={
    #     'config': dg.ResourceDefinition.hardcoded_resource(Config()),
    # },
)
# ============= EOF =============================================
