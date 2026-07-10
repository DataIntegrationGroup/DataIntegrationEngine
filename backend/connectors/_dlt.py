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

from backend.exceptions import PartialOrNoDataError, USGSRateLimitError


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


def fetch_json_records(
    url: str,
    params: Optional[dict] = None,
    json_data: Optional[dict] = None,
    method: str = "GET",
    data_selector: Optional[str] = None,
    paginator: Optional[BasePaginator] = None,
    headers: Optional[dict] = None,
) -> list:
    """Fetch **all** records from a paginated JSON endpoint, following the
    paginator across every page, and return them as one flat list.

    This is where dlt earns its keep: ``paginator`` (e.g. a ``JSONLinkPaginator``
    on the OGC ``rel=next`` link) makes the client *follow* pagination instead of
    the old code refusing a truncated response. ``data_selector`` picks the
    records array (e.g. ``"features"``). ``method="POST"`` with ``json_data``
    carries a CQL body (USGS complex queries).

    Error mapping matches the connectors' expectations so the unifier degrades
    gracefully: a 429 → ``USGSRateLimitError``, any other request failure →
    ``PartialOrNoDataError``."""
    client = RESTClient(base_url="")
    records: list = []
    try:
        for page in client.paginate(
            url,
            method=method,
            params=params,
            json=json_data,
            data_selector=data_selector,
            paginator=paginator,
            headers=headers,
        ):
            records.extend(page)
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        if status == 429:
            raise USGSRateLimitError("Rate limit exceeded")
        raise PartialOrNoDataError(f"Request failed for {url}: {e}")
    except requests.RequestException as e:
        raise PartialOrNoDataError(f"Request failed for {url}: {e}")
    return records
