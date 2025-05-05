# ===============================================================================
# Author:  Jake Ross
# Copyright 2025 New Mexico Bureau of Geology & Mineral Resources
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
import os
from os import getenv
from typing import Callable

import dagster as dg

from backend.config import Config
from backend.logger import setup_logging
from backend.unifier import unify_analytes, unify_waterlevels
from dagsterio.config.source_constants import ALL_SOURCES, NMBGMR_SOURCES


@dg.asset
def all_tds():
    """TDS asset"""

    _analyte(
        'tds',
        sources=ALL_SOURCES
    )


@dg.asset
def all_waterlevels():
    """Summary water levels asset"""
    _waterlevels(
        sources=ALL_SOURCES,
    )


@dg.asset
def nmbgmr_waterlevels():
    """NMBGMR water levels asset"""

    _waterlevels(
        sources=NMBGMR_SOURCES,
    )


@dg.asset
def nmbgmr_tds():
    """NMBGMR TDS asset"""
    _analyte(
    'tds',
    sources=NMBGMR_SOURCES,
    )


def _get_geoserver_connection():
    return { 'db':
                 {
                'dbname': getenv('GEOSERVER_DBNAME'),
                'user': getenv('GEOSERVER_USER'),
                'password': getenv('GEOSERVER_PASSWORD'),
                'instance_connection_name': getenv('GEOSERVER_INSTANCE_CONNECTION_NAME'),
                 'cloud_sql': True
                }
             }


def _waterlevels(**payload):
    _unify(unify_waterlevels, 'waterlevels', payload)


def _analyte(param: str, **payload):
    _unify(unify_analytes, param, payload)


def _unify(func: Callable[[Config,], None], parameter: str, payload: dict):
    payload['yes'] = True
    payload['geoserver'] = _get_geoserver_connection()
    payload['output_summary'] = True
    payload['output_format']= 'geoserver'
    config = Config(payload=payload)
    config.parameter = parameter
    config.finalize()

    func(config)



defs = dg.Definitions(
    assets=[all_tds, all_waterlevels, nmbgmr_tds, nmbgmr_waterlevels],
    schedules=[
        dg.ScheduleDefinition(
            name='tds_schedule',
            target=dg.AssetSelection.keys("all_tds"),
            cron_schedule='0 0 * * *',
            execution_timezone='America/Denver',
        ),
        dg.ScheduleDefinition(
            name='waterlevels_schedule',
            target=dg.AssetSelection.keys("all_waterlevels"),
            cron_schedule='0 0 * * *',
            execution_timezone='America/Denver',
        ),
        dg.ScheduleDefinition(
            name='nmbgmr_tds_schedule',
            target=dg.AssetSelection.keys("nmbgmr_tds"),
            cron_schedule='0 0 * * *',
            execution_timezone='America/Denver',
        ),
        dg.ScheduleDefinition(
            name='nmbgmr_waterlevels_schedule',
            target=dg.AssetSelection.keys("nmbgmr_waterlevels"),
            cron_schedule='0 0 * * *',
            execution_timezone='America/Denver',
        ),
    ],
    # resources={
    #     'config': dg.ResourceDefinition.hardcoded_resource(Config()),
    # },
)
# ============= EOF =============================================
