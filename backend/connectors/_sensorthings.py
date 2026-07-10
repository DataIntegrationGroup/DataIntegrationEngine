# ===============================================================================
# Copyright 2024 Jake Ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
"""Minimal SensorThings (OGC STA / FROST) query helper on dlt.

Replaces ``frost_sta_client`` for the DIE FROST connectors (nmenv/dwb + the st2
fleet). SensorThings is plain OData-over-JSON: ``$expand`` / ``$filter`` /
``$top`` / ``$orderby`` are query params, results come back as a ``value`` array
with an ``@iot.nextLink`` cursor, and expanded entities are nested keys
(``Things``, ``Datastreams``, ``Observations``, ``Locations``,
``ObservedProperty``, ``unitOfMeasurement`` …). frost's typed entities were a
thin convenience over that; here the connectors read the JSON dicts directly.

Retry/backoff + connection pooling now come from dlt's session (frost used bare
``requests.request`` with neither)."""

from typing import Optional

from dlt.sources.helpers.rest_client.paginators import (
    JSONLinkPaginator,
    SinglePagePaginator,
)
from jsonpath_ng.ext import parse

from backend.connectors._dlt import fetch_json_records

# SensorThings exposes the next page as a top-level "@iot.nextLink" URL; the key
# has an "@" and a ".", so it needs bracket-quoting in the JSONPath.
_NEXT_LINK = parse("'@iot.nextLink'")


def sta_query(
    base_url: str,
    path: str,
    *,
    expand: Optional[str] = None,
    filter: Optional[str] = None,  # noqa: A002 - mirrors OData $filter
    top: Optional[int] = None,
    orderby: Optional[str] = None,
) -> list:
    """Return the ``value`` items of a SensorThings collection at
    ``{base_url}/{path}``, following ``@iot.nextLink`` across all pages.

    When *top* is given it is treated as a **limit**: only the first page is
    fetched (``$top`` caps it) and pagination is not followed — matching the
    connectors' ``.top(n)`` usage (health probes, bounded reads)."""
    params: dict = {}
    if expand:
        params["$expand"] = expand
    if filter:
        params["$filter"] = filter
    if orderby:
        params["$orderby"] = orderby

    if top is not None:
        params["$top"] = top
        paginator = SinglePagePaginator()
    else:
        paginator = JSONLinkPaginator(next_url_path=_NEXT_LINK)

    url = f"{base_url.rstrip('/')}/{path}"
    return fetch_json_records(
        url, params=params, data_selector="value", paginator=paginator
    )
