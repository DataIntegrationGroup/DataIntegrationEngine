"""run_time() gives one timestamp per Dagster run, so the 5/10/20-year trend
layers built in the same run share a window. Needs dagster (run from the
orchestration environment)."""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

pytest.importorskip("dagster")

from orchestration.assets.products import run_time  # noqa: E402


def _context(tags=None, created=None):
    record = None if created is None else SimpleNamespace(create_timestamp=created)
    return SimpleNamespace(
        run=SimpleNamespace(tags=tags or {}),
        run_id="r1",
        instance=SimpleNamespace(get_run_record_by_id=lambda run_id: record),
    )


def test_uses_schedule_tick_time():
    ctx = _context(
        tags={"dagster/scheduled_execution_time": "2026-11-15T06:00:00+00:00"},
        created=datetime(2026, 12, 1, tzinfo=timezone.utc),
    )
    assert run_time(ctx) == datetime(2026, 11, 15, 6, tzinfo=timezone.utc)


def test_falls_back_to_run_creation_time():
    created = datetime(2026, 11, 20, 9, tzinfo=timezone.utc)
    assert run_time(_context(created=created)) == created

