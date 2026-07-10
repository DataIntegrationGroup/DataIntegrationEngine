# ===============================================================================
# Copyright 2024 Jake Ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
"""dlt-based HTTP extraction helpers (Phase B spike — see
docs/framework-migration-plan.md).

dlt's ``RESTClient`` wraps a retry-configured ``requests`` session, so it can
replace the hand-rolled retry loops in ``backend/source.py``
(``_execute_text_request`` / ``_execute_json_request``) connector by connector.
This module is the thin shared entry point; connectors build their URL + params
exactly as today and delegate the transport here.

Requires the optional ``dlt`` extra.
"""

from typing import Optional

import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import BasePaginator

from backend.exceptions import PartialOrNoDataError


def fetch_text(url: str, params: Optional[dict] = None, timeout: int = 30) -> str:
    """GET *url* with *params* and return the response body as text.

    For single-shot endpoints (e.g. the WQP TSV downloads) — no pagination.
    ``RESTClient``'s default session retries transient errors (connection
    resets, 5xx, 429) internally, so this replaces the manual backoff loop in
    ``_execute_text_request``. On a final failure it raises
    ``PartialOrNoDataError`` — the same type ``_execute_text_request`` raised —
    so the unifier still skips the source gracefully instead of aborting."""
    client = RESTClient(base_url="")
    try:
        resp = client.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise PartialOrNoDataError(f"Request failed for {url}: {e}")
    return resp.text


def fetch_json_pages(
    url: str,
    params: Optional[dict] = None,
    data_selector: Optional[str] = None,
    paginator: Optional[BasePaginator] = None,
    timeout: int = 30,
):
    """Yield each page of records from a paginated JSON endpoint.

    Unused by the WQP spike (WQP is single-shot TSV) — provided for the
    paginated-JSON connectors (USGS OGC API, ArcGIS offset) where dlt's
    paginators are the actual win. ``data_selector`` picks the records array
    (e.g. ``"features"``); ``paginator`` follows the next link / offset."""
    client = RESTClient(base_url="")
    yield from client.paginate(
        url, params=params, data_selector=data_selector, paginator=paginator
    )
