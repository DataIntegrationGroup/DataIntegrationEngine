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
    inside the combine. A cohort job materializes a combine's sources in the
    same run, so the blob is normally present; but an **ad-hoc** materialization
    of just a combine (e.g. re-publish from the Assets UI without re-running the
    upstream sources), or a brand-new shared source never yet materialized,
    leaves the blob absent and the stock manager would fail the combine hard.

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
    "ogc_hardness",
    "ogc_water_type",
    "ogc_data_density",
    "ogc_waterlevel_change",
    "ogc_sar",
    "ogc_ion_balance",
    "ogc_wqi",
    "ogc_waterlevel_status",
    "ogc_seasonal_amplitude",
    "ogc_depletion_projection",
}


def _load_products() -> dict:
    return yaml.safe_load(_PRODUCTS_PATH.read_text())


def _products(products_config: dict) -> Iterator[dict]:
    for product in products_config["products"]:
        if product.get("output_type") in _SUPPORTED_OUTPUT_TYPES:
            yield product


def _build_graph(products_config: dict):
    """Build every asset once. Source assets are shared across products: each
    distinct (parameter, scope, source) tuple becomes one asset, fetched once and
    unified for both summary and timeseries, so a source two products both need
    (in either mode) is fetched once, not twice.

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


def _cohort_key(specs) -> tuple[str, str]:
    """A cohort bundles the products that *can* share source assets — same
    parameter group and spatial scope. Materializing a cohort in one run is what
    lets each shared source unify once while every member's full lineage
    (sources → combine → geoserver) stays visible in that run.

    A shared source is now fetched once for *both* summary and timeseries, so a
    summary product and a timeseries product over the same group/scope share
    source assets and must run together — hence mode is not part of the key.
    Both fields are constant within a product (its scope is fixed and its
    parameters are all one group), so any spec is representative."""
    s = specs[0]
    return (s.group, s.scope)


def _cohort_name(key: tuple[str, str]) -> str:
    group, scope = key
    return f"{group}_{scope}"


def _cron_sort_key(cron: str) -> tuple[int, int]:
    # "min hour * * *" -> (hour, minute) for picking a cohort's earliest member.
    parts = cron.split()
    try:
        return (int(parts[1]), int(parts[0]))
    except (IndexError, ValueError):
        return (6, 0)


def _build_cohorts(products_config: dict, specs_by_pid: dict) -> dict:
    """Group products into cohorts keyed by (group, scope). Returns
    ``{cohort_name: {"members": [pid, ...], "cron": str}}``; the cohort cron is
    the earliest member schedule (members run together, so they share one)."""
    cohorts: dict = {}
    for product in _products(products_config):
        pid = product["id"]
        specs = specs_by_pid[pid]
        if not specs:
            continue
        name = _cohort_name(_cohort_key(specs))
        cohort = cohorts.setdefault(name, {"members": [], "cron": None})
        cohort["members"].append(pid)
        cron = product.get("schedule", "0 6 * * *")
        if cohort["cron"] is None or _cron_sort_key(cron) < _cron_sort_key(cohort["cron"]):
            cohort["cron"] = cron
    return cohorts


def _cohort_selection(members: list[str], specs_by_pid: dict) -> dg.AssetSelection:
    """The full graph for a cohort: every member's shared source assets (deduped
    across members — a shared source resolves to one key, so it is selected once
    and materialized once per run), plus every member's combine and geoserver
    asset. The result is a complete sources → combine → geoserver lineage with no
    duplicated source fetch."""
    keys: set = set()
    for pid in members:
        for spec in specs_by_pid[pid]:
            keys.add(shared_source_key(spec))
        keys.add(dg.AssetKey(pid))
        keys.add(dg.AssetKey([pid, "geoserver"]))
    return dg.AssetSelection.keys(*sorted(keys, key=lambda k: k.to_user_string()))


def _build_cohort_jobs(
    cohorts: dict, specs_by_pid: dict
) -> dict[str, "UnresolvedAssetJobDefinition"]:
    """One job per cohort, selecting that cohort's full graph (see
    :func:`_cohort_selection`). Returns ``{cohort_name: job}``."""
    jobs = {}
    for name, cohort in cohorts.items():
        members = cohort["members"]
        jobs[name] = dg.define_asset_job(
            name=f"{name}_job",
            selection=_cohort_selection(members, specs_by_pid),
            description=(
                f"Materialize the {name} cohort in one run — shared sources "
                f"(each unified once) → combines → geoserver for: "
                f"{', '.join(members)}."
            ),
        )
    return jobs


def _build_schedules(
    cohorts: dict, cohort_jobs: dict[str, "UnresolvedAssetJobDefinition"]
) -> list[dg.ScheduleDefinition]:
    return [
        dg.ScheduleDefinition(
            name=f"schedule_{name}",
            job=cohort_jobs[name],
            cron_schedule=cohort["cron"],
            execution_timezone="America/Denver",
        )
        for name, cohort in cohorts.items()
    ]


_products_config = _load_products()
_source_assets, _pipeline_assets, _specs_by_pid, _all_specs = _build_graph(_products_config)
_assets = _source_assets + _pipeline_assets
_cohorts = _build_cohorts(_products_config, _specs_by_pid)
_cohort_jobs = _build_cohort_jobs(_cohorts, _specs_by_pid)
_schedules = _build_schedules(_cohorts, _cohort_jobs)

defs = dg.Definitions(
    assets=_assets,
    jobs=list(_cohort_jobs.values()),
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
        # /tmp. A cohort run materializes a shared source once and every member
        # combine reads it back through this manager (so the source unifies once
        # even though multiple combines consume it). The tolerant subclass
        # returns an empty payload when a source's blob is absent, so an ad-hoc
        # combine-only materialization (or a never-yet-run new source) degrades
        # to an empty collection instead of hard-failing.
        "io_manager": _TolerantGCSPickleIOManager(
            gcs=AuthedGCSResource(),
            gcs_bucket=_products_config.get("gcs_bucket", "dataservices-die-products"),
            gcs_prefix="dagster-io",
        ),
    },
)
