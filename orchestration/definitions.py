from pathlib import Path

import dagster as dg
import yaml
from dagster_gcp.gcs import GCSPickleIOManager, GCSResource as DagsterGCSResource

from orchestration.resources.die_config import DIEConfigResource
from orchestration.resources.gcs import GCSResource
from orchestration.resources.geoserver import GeoServerResource
from orchestration.assets.products import build_product_assets

_PRODUCTS_PATH = Path(__file__).parent / "config" / "products.yaml"

_SUPPORTED_OUTPUT_TYPES = {"ogc_summary", "ogc_timeseries"}


def _load_products() -> dict:
    return yaml.safe_load(_PRODUCTS_PATH.read_text())


def _build_assets(products_config: dict) -> list:
    assets = []
    for product in products_config["products"]:
        if product.get("output_type") in _SUPPORTED_OUTPUT_TYPES:
            assets.extend(build_product_assets(product))
    return assets


def _build_schedules(products_config: dict) -> list:
    schedules = []
    for product in products_config["products"]:
        if product.get("output_type") not in _SUPPORTED_OUTPUT_TYPES:
            continue
        pid = product["id"]
        schedules.append(
            dg.ScheduleDefinition(
                name=f"schedule_{pid}",
                # Materialize the full per-product graph: the geoserver leaf plus
                # everything upstream of it (combine asset + source assets).
                target=dg.AssetSelection.keys(
                    dg.AssetKey([pid, "geoserver"])
                ).upstream(),
                cron_schedule=product.get("schedule", "0 6 * * *"),
                execution_timezone="America/Denver",
            )
        )
    return schedules


_products_config = _load_products()
_assets = _build_assets(_products_config)
_schedules = _build_schedules(_products_config)

defs = dg.Definitions(
    assets=_assets,
    schedules=_schedules,
    resources={
        "die_config": DIEConfigResource(),
        "gcs": GCSResource(
            bucket_name=_products_config.get("gcs_bucket", "dataservices-die-products"),
        ),
        "geoserver": GeoServerResource(),
        # Persist asset I/O to GCS instead of the serverless run's ephemeral
        # /tmp. Without this, materializing a downstream asset (combine /
        # geoserver) on its own can't load its source inputs from a prior run
        # and fails with FileNotFoundError.
        "io_manager": GCSPickleIOManager(
            gcs=DagsterGCSResource(),
            gcs_bucket=_products_config.get("gcs_bucket", "dataservices-die-products"),
            gcs_prefix="dagster-io",
        ),
    },
)
