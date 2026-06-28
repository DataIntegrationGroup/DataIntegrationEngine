from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import dagster as dg
import yaml
from dagster_gcp.gcs import GCSPickleIOManager
from google.api_core.exceptions import NotFound

from orchestration.resources.die_config import DIEConfigResource
from orchestration.resources.gcs import GCSResource, AuthedGCSResource
from orchestration.resources.geoserver import GeoServerResource
from orchestration.assets.products import (
    build_product_pipeline_assets,
    build_shared_source_asset,
    product_source_specs,
    shared_source_key,
)

class _TolerantGCSPickleIOManager(GCSPickleIOManager):
    """GCS pickle IO manager that tolerates a missing source input.

    Combine assets load their shared source inputs via ``ins``, and that load
    happens *before* the asset body runs — so a missing pickle can't be caught
    inside the combine. The source assets are materialized by ``sources_job``;
    a product job loads their output from GCS without re-running them. On first
    deploy — or if a newly added shared source has never been materialized —
    the blob is absent and the stock manager would fail the combine hard.

    Returning an empty payload instead degrades that product to an empty
    collection (the geoserver asset already soft-fails on 0 features) rather
    than crashing the run. A source asset always writes a payload when it
    actually runs (it soft-fails to an empty payload, never to *no* output), so
    a missing blob only ever means "never materialized", never "ran and lost
    its data" — which is why swallowing NotFound here can't mask a real error.
    """

    def load_input(self, context: dg.InputContext):
        try:
            return super().load_input(context)
        except (NotFound, FileNotFoundError):
            context.log.warning(
                f"Source input {context.asset_key.to_user_string()!r} not found "
                "in GCS; treating as empty. Run sources_job before the product "
                "jobs (it is scheduled ahead of them, but must run once on a "
                "fresh deploy)."
            )
            return {}


if TYPE_CHECKING:
    # define_asset_job returns this; it isn't a public dagster export.
    from dagster._core.definitions.unresolved_asset_job_definition import (
        UnresolvedAssetJobDefinition,
    )

_PRODUCTS_PATH = Path(__file__).parent / "config" / "products.yaml"

_SUPPORTED_OUTPUT_TYPES = {
    "ogc_summary",
    "ogc_timeseries",
    "ogc_major_chemistry",
    "ogc_waterlevel_trend",
    "ogc_analyte_trend",
    "ogc_mcl_exceedance",
    "ogc_monitoring_recency",
}


def _load_products() -> dict:
    return yaml.safe_load(_PRODUCTS_PATH.read_text())


def _products(products_config: dict) -> Iterator[dict]:
    for product in products_config["products"]:
        if product.get("output_type") in _SUPPORTED_OUTPUT_TYPES:
            yield product


def _build_graph(products_config: dict):
    """Build every asset once. Source assets are shared across products: each
    distinct (parameter, mode, scope, source) tuple becomes one asset, so a
    source two products both need is unified once, not twice.

    Returns ``(source_assets, pipeline_assets, specs_by_pid, all_specs)`` where
    ``specs_by_pid`` maps a product id to the source specs its combine consumes
    and ``all_specs`` is the deduped set of source specs."""
    specs_by_pid: dict[str, list] = {}
    all_specs: dict = {}  # SourceSpec -> SourceSpec (dedup; namedtuple is hashable)
    for product in _products(products_config):
        specs = product_source_specs(product)
        specs_by_pid[product["id"]] = specs
        for spec in specs:
            all_specs.setdefault(spec, spec)

    source_assets = [build_shared_source_asset(spec) for spec in all_specs]

    pipeline_assets: list[dg.AssetsDefinition] = []
    for product in _products(products_config):
        pipeline_assets.extend(
            build_product_pipeline_assets(product, specs_by_pid[product["id"]])
        )

    return source_assets, pipeline_assets, specs_by_pid, all_specs


def _product_selection(pid: str) -> dg.AssetSelection:
    # Only the product's own assets — combine + geoserver. The shared source
    # inputs are NOT selected: the sources job materializes them, and the
    # combine loads them from the GCS IO manager. This is what keeps a product
    # run from re-unifying a source another product already produced.
    return dg.AssetSelection.keys(dg.AssetKey(pid), dg.AssetKey([pid, "geoserver"]))


def _build_product_jobs(
    products_config: dict,
) -> dict[str, "UnresolvedAssetJobDefinition"]:
    """One asset job per product, selecting only that product's combine +
    geoserver assets. Returns {product_id: job} so schedules can target it. The
    shared source inputs are loaded from the GCS IO manager, not re-materialized
    (see :func:`_product_selection`)."""
    jobs = {}
    for product in _products(products_config):
        pid = product["id"]
        jobs[pid] = dg.define_asset_job(
            name=f"{pid}_job",
            selection=_product_selection(pid),
            description=f"Publish the {pid} data product (combine → geoserver) from materialized sources.",
        )
    return jobs


def _build_sources_job(all_specs) -> "UnresolvedAssetJobDefinition":
    """A single job that materializes every shared source asset once. Product
    jobs read these results from the IO manager rather than re-unifying."""
    keys = [shared_source_key(spec) for spec in all_specs]
    return dg.define_asset_job(
        name="sources_job",
        selection=dg.AssetSelection.keys(*keys),
        description="Unify every shared source once; product jobs consume the cached results.",
    )


def _build_schedules(
    products_config: dict,
    product_jobs: dict[str, "UnresolvedAssetJobDefinition"],
    sources_job: "UnresolvedAssetJobDefinition",
) -> list[dg.ScheduleDefinition]:
    # Sources run first (default 05:00), ahead of the product schedules (06:00+),
    # so each product publishes from same-day source data. A product that runs
    # before the sources job simply reads the prior run's cached source IO.
    schedules = [
        dg.ScheduleDefinition(
            name="schedule_sources",
            job=sources_job,
            cron_schedule=products_config.get("sources_schedule", "0 5 * * *"),
            execution_timezone="America/Denver",
        )
    ]
    for product in _products(products_config):
        pid = product["id"]
        schedules.append(
            dg.ScheduleDefinition(
                name=f"schedule_{pid}",
                job=product_jobs[pid],
                cron_schedule=product.get("schedule", "0 6 * * *"),
                execution_timezone="America/Denver",
            )
        )
    return schedules


_products_config = _load_products()
_source_assets, _pipeline_assets, _specs_by_pid, _all_specs = _build_graph(_products_config)
_assets = _source_assets + _pipeline_assets
_product_jobs = _build_product_jobs(_products_config)
_sources_job = _build_sources_job(_all_specs)
_schedules = _build_schedules(_products_config, _product_jobs, _sources_job)

defs = dg.Definitions(
    assets=_assets,
    jobs=[_sources_job, *_product_jobs.values()],
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
        # /tmp. This is what lets a product job load its shared source inputs
        # (materialized by sources_job) without re-running them. The tolerant
        # subclass returns an empty payload when a source's blob is absent, so a
        # combine never hard-fails on a not-yet-materialized source (e.g. on a
        # fresh deploy — run sources_job once before the product jobs).
        "io_manager": _TolerantGCSPickleIOManager(
            gcs=AuthedGCSResource(),
            gcs_bucket=_products_config.get("gcs_bucket", "dataservices-die-products"),
            gcs_prefix="dagster-io",
        ),
    },
)
