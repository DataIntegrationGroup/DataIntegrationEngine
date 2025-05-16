# ===============================================================================
# Author:  Jake Ross
# Copyright 2025 New Mexico Bureau of Geology & Mineral Resources
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
from os import getenv
from typing import Callable

from backend.config import Config
from backend.unifier import unify_waterlevels, unify_analytes


def base_waterlevels_asset(**payload):
    _unify(unify_waterlevels, 'waterlevels', payload)


def base_analyte_asset(param: str, **payload: object) -> None:
    _unify(unify_analytes, param, payload)


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

def _unify(func: Callable[[Config,], None], parameter: str, payload: dict):
    payload['yes'] = True
    payload['geoserver'] = _get_geoserver_connection()
    payload['output_summary'] = True
    payload['output_format']= 'geoserver'
    config = Config(payload=payload)
    config.parameter = parameter
    config.finalize()

    func(config)
# ============= EOF =============================================
