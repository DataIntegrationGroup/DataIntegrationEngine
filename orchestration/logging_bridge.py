"""Forward DIE's stdlib logging into the Dagster compute log.

DIE logs through the root `logging` logger (see backend/logger.py). Dagster only
surfaces messages sent to `context.log`, so during materialization we attach a
handler that relays every root log record to the Dagster logger. Removed on exit
so we never leak handlers across runs.
"""
import logging
from contextlib import contextmanager


class _DagsterForwardHandler(logging.Handler):
    def __init__(self, dagster_logger):
        super().__init__()
        self._dagster_logger = dagster_logger

    def emit(self, record):
        try:
            msg = self.format(record)
            self._dagster_logger.log(record.levelno, msg)
        except Exception:  # never let logging break the asset
            self.handleError(record)


@contextmanager
def forward_die_logs(context, level=logging.INFO):
    """Relay root-logger records to `context.log` for the duration of the block."""
    handler = _DagsterForwardHandler(context.log)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter("%(name)-30s %(message)s"))

    root = logging.getLogger()
    prev_level = root.level
    if root.level > level or root.level == logging.NOTSET:
        root.setLevel(level)
    root.addHandler(handler)
    try:
        yield
    finally:
        root.removeHandler(handler)
        root.setLevel(prev_level)
