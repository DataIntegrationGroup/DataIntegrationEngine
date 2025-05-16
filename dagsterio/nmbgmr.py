# ===============================================================================
# Author:  Jake Ross
# Copyright 2025 New Mexico Bureau of Geology & Mineral Resources
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
import json

import dagster as dg
import httpx

from dagsterio import base_waterlevels_asset, base_analyte_asset
from dagsterio.config.source_constants import NMBGMR_SOURCES


@dg.asset
def nmbgmr_waterlevels():
    """NMBGMR water levels asset"""

    base_waterlevels_asset(
        sources=NMBGMR_SOURCES,
    )


@dg.asset
def nmbgmr_tds():
    """NMBGMR TDS asset"""
    base_analyte_asset(
        'tds',
        sources=NMBGMR_SOURCES,
    )


def get_latest_analyte(param: str, state: dict):
    url = 'http://localhost:8009/latest/stats/majorchemistry'
    queryparams = {'analyte': param}
    resp = httpx.get(url, params=queryparams)
    return resp.json().get('count', 0)


request_job = dg.define_asset_job(
    name='nmbgmr_tds_job',
    selection=dg.AssetSelection.assets("nmbgmr_tds"),
)

@dg.sensor(job=request_job, minimum_interval_seconds=3600)
def tds_request_sensor(context: dg.SensorEvaluationContext):
    return analyte_sensor('tds', context)


def analyte_sensor(param, context: dg.SensorEvaluationContext):
    if context.cursor:
        return None

    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}
    runs = []

    latest = get_latest_analyte(param, previous_state)
    if latest:
        key = f'latest_{param}'
        current_state[key] = latest
        if latest > previous_state.get(key, 0):
            runs.append(dg.RunRequest(run_key=param))

    return dg.SensorResult(
        run_requests=runs, cursor=json.dumps(current_state)
    )

# ============= EOF =============================================
