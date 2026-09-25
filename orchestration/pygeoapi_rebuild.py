"""Rebuild pygeoapi once product runs finish, so the image bakes the new data (SPEC §6.6)."""
import json
import os
from datetime import datetime, timezone

import dagster as dg

TRIGGER = "die-pygeoapi-rebuild"
REGION = "us-central1"

_IN_PROGRESS = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
]


def run_trigger() -> None:
    """Start the Cloud Build trigger, using the same credentials as GCS."""
    import google.auth
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2 import service_account

    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    key = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
    if key:
        info = json.loads(key)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        project = info["project_id"]
    else:
        creds, project = google.auth.default(scopes=scopes)
    url = (
        f"https://cloudbuild.googleapis.com/v1/projects/{project}"
        f"/locations/{REGION}/triggers/{TRIGGER}:run"
    )
    AuthorizedSession(creds).post(url, json={}).raise_for_status()


def build_rebuild_sensor(job_names: list[str]) -> dg.SensorDefinition:
    """Sensor that starts one rebuild after a product job succeeds, once no
    product job is queued or running. The cursor is the last check time."""
    jobs = set(job_names)

    # Stopped by default so local and branch deployments never rebuild prod;
    # turn it on in the prod deployment.
    @dg.sensor(
        name="pygeoapi_rebuild",
        minimum_interval_seconds=600,
        default_status=dg.DefaultSensorStatus.STOPPED,
    )
    def pygeoapi_rebuild(context: dg.SensorEvaluationContext):
        now = datetime.now(timezone.utc)
        if context.cursor is None:
            context.update_cursor(now.isoformat())
            return dg.SkipReason("First tick; watching for product runs from now.")

        def product_runs(**filters):
            runs = context.instance.get_runs(filters=dg.RunsFilter(**filters))
            return [r for r in runs if r.job_name in jobs]

        if product_runs(statuses=_IN_PROGRESS):
            return dg.SkipReason("Product runs still in progress.")
        succeeded = product_runs(
            statuses=[dg.DagsterRunStatus.SUCCESS],
            updated_after=datetime.fromisoformat(context.cursor),
        )
        if not succeeded:
            context.update_cursor(now.isoformat())
            return dg.SkipReason("No product runs succeeded since the last check.")

        run_trigger()
        context.update_cursor(now.isoformat())
        context.log.info(f"Started {TRIGGER} after {len(succeeded)} product run(s).")
        return dg.SkipReason(f"Started Cloud Build trigger {TRIGGER}.")

    return pygeoapi_rebuild
