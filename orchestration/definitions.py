from pathlib import Path

import dagster as dg
import yaml

from orchestration.resources.die_config import DIEConfigResource
from orchestration.resources.gcs import GCSResource
from orchestration.assets.waterlevels import (
    build_waterlevels_summary_asset,
    build_waterlevels_timeseries_asset,
)
from orchestration.assets.analytes import build_analyte_summary_asset

_PRODUCTS_PATH = Path(__file__).parent / "config" / "products.yaml"


def _load_products() -> dict:
    return yaml.safe_load(_PRODUCTS_PATH.read_text())


def _build_assets(products_config: dict) -> list:
    assets = []
    for product in products_config["products"]:
        param = product["parameter"]
        output_type = product["output_type"]

        if param == "waterlevels" and output_type == "ogc_summary":
            assets.append(build_waterlevels_summary_asset(product))
        elif param == "waterlevels" and output_type == "ogc_timeseries":
            assets.append(build_waterlevels_timeseries_asset(product))
        elif output_type == "ogc_summary":
            assets.append(build_analyte_summary_asset(product))

    return assets


def _build_schedules(products_config: dict, assets: list) -> list:
    asset_names = {a.key.path[-1] for a in assets}
    schedules = []
    for product in products_config["products"]:
        pid = product["id"]
        if pid not in asset_names:
            continue
        schedules.append(
            dg.ScheduleDefinition(
                name=f"schedule_{pid}",
                target=dg.AssetSelection.keys(pid),
                cron_schedule=product.get("schedule", "0 6 * * *"),
                execution_timezone="America/Denver",
            )
        )
    return schedules


_products_config = _load_products()
_assets = _build_assets(_products_config)
_schedules = _build_schedules(_products_config, _assets)

defs = dg.Definitions(
    assets=_assets,
    schedules=_schedules,
    resources={
        "die_config": DIEConfigResource(),
        "gcs": GCSResource(
            bucket_name=_products_config.get("gcs_bucket", "dataservices-die-products"),
        ),
    },
)
