from datetime import datetime, timedelta, timezone
from unittest import mock

import dagster as dg
import pytest

from orchestration import pygeoapi_rebuild
from orchestration.pygeoapi_rebuild import build_rebuild_sensor

SUCCESS = dg.DagsterRunStatus.SUCCESS
STARTED = dg.DagsterRunStatus.STARTED


@pytest.fixture
def trigger():
    with mock.patch.object(pygeoapi_rebuild, "run_trigger") as run:
        yield run


def _evaluate(runs, cursor):
    """Run the sensor against fake (job_name, status) runs."""
    def get_runs(filters):
        return [
            mock.Mock(job_name=job)
            for job, status in runs
            if status in filters.statuses
        ]

    instance = mock.Mock(spec=dg.DagsterInstance)
    instance.get_runs.side_effect = get_runs
    context = dg.build_sensor_context(instance=instance, cursor=cursor)
    result = build_rebuild_sensor(["wl_job", "chem_job"])(context)
    return result, context.cursor


def _earlier():
    return (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()


def test_first_tick_sets_cursor(trigger):
    _, cursor = _evaluate([("wl_job", SUCCESS)], cursor=None)
    assert cursor is not None
    trigger.assert_not_called()


def test_rebuilds_once_product_runs_finish(trigger):
    _evaluate([("wl_job", SUCCESS), ("chem_job", SUCCESS)], cursor=_earlier())
    trigger.assert_called_once()


def test_waits_while_a_product_run_is_in_progress(trigger):
    earlier = _earlier()
    _, cursor = _evaluate([("wl_job", SUCCESS), ("chem_job", STARTED)], cursor=earlier)
    trigger.assert_not_called()
    assert cursor == earlier  # keep the success for the next tick


def test_ignores_other_jobs(trigger):
    _evaluate([("other_job", SUCCESS)], cursor=_earlier())
    trigger.assert_not_called()


def test_no_rebuild_without_new_success(trigger):
    _evaluate([], cursor=_earlier())
    trigger.assert_not_called()
