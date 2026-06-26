from pathlib import Path

import dagster as dg
import yaml
from dagster_gcp.gcs import GCSPickleIOManager

from orchestration.resources.die_config import DIEConfigResource
from orchestration.resources.gcs import GCSResource, AuthedGCSResource
from orchestration.resources.geoserver import GeoServerResource
from orchestration.assets.products import build_product_assets

_PRODUCTS_PATH = Path(__file__).parent / "config" / "products.yaml"

_SUPPORTED_OUTPUT_TYPES = {
    "ogc_summary",
    "ogc_timeseries",
    "ogc_major_chemistry",
    "ogc_waterlevel_trend",
}


def _load_products() -> dict:
    return yaml.safe_load(_PRODUCTS_PATH.read_text())


def _build_assets(products_config: dict) -> list:
    assets = []
    for product in products_config["products"]:
        if product.get("output_type") in _SUPPORTED_OUTPUT_TYPES:
            assets.extend(build_product_assets(product))
    return assets


def _product_selection(pid: str) -> dg.AssetSelection:
    # The full per-product graph: the geoserver leaf plus everything upstream of
    # it (combine asset + source assets).
    return dg.AssetSelection.keys(dg.AssetKey([pid, "geoserver"])).upstream()


def _products(products_config: dict):
    for product in products_config["products"]:
        if product.get("output_type") in _SUPPORTED_OUTPUT_TYPES:
            yield product


def _build_jobs(products_config: dict) -> dict:
    """One asset job per product, selecting that product's whole graph.
    Returns {product_id: job} so schedules can target the job."""
    jobs = {}
    for product in _products(products_config):
        pid = product["id"]
        jobs[pid] = dg.define_asset_job(
            name=f"{pid}_job",
            selection=_product_selection(pid),
            description=f"Materialize the {pid} data product (sources → combine → geoserver).",
        )
    return jobs


def _build_schedules(products_config: dict, jobs: dict) -> list:
    schedules = []
    for product in _products(products_config):
        pid = product["id"]
        schedules.append(
            dg.ScheduleDefinition(
                name=f"schedule_{pid}",
                job=jobs[pid],
                cron_schedule=product.get("schedule", "0 6 * * *"),
                execution_timezone="America/Denver",
            )
        )
    return schedules


_products_config = _load_products()
_assets = _build_assets(_products_config)
_jobs = _build_jobs(_products_config)
_schedules = _build_schedules(_products_config, _jobs)

defs = dg.Definitions(
    assets=_assets,
    jobs=list(_jobs.values()),
    schedules=_schedules,
    resources={
        # USGS_API_KEY is a Dagster+ secret; EnvVar resolves it at run time and
        # the resource exports it for the NWIS connector. Resolves to None when
        # unset (the API still works, just rate-limited).
        "die_config": DIEConfigResource(
            usgs_api_key=dg.EnvVar("USGS_API_KEY"),
        ),
        "gcs": GCSResource(
            bucket_name=_products_config.get("gcs_bucket", "dataservices-die-products"),
        ),
        "geoserver": GeoServerResource(),
        # Persist asset I/O to GCS instead of the serverless run's ephemeral
        # /tmp. Without this, materializing a downstream asset (combine /
        # geoserver) on its own can't load its source inputs from a prior run
        # and fails with FileNotFoundError.
        "io_manager": GCSPickleIOManager(
            gcs=AuthedGCSResource(),
            gcs_bucket=_products_config.get("gcs_bucket", "dataservices-die-products"),
            gcs_prefix="dagster-io",
        ),
    },
)
