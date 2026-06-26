# ===============================================================================
# Copyright 2024 Jake Ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
import json
from datetime import datetime, timezone
from typing import Optional


def _timestamp_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_feature(record, collection_id: str) -> dict:
    """Build one OGC-compliant Feature from a SummaryRecord or SiteRecord."""
    source = getattr(record, "source", "")
    rid = getattr(record, "id", "")
    feature_id = f"{source}:{rid}" if source and rid else str(rid)

    props = {k: getattr(record, k) for k in record.keys
             if k not in ("latitude", "longitude", "elevation")}

    lat = getattr(record, "latitude", None)
    lon = getattr(record, "longitude", None)
    elev = getattr(record, "elevation", None)

    coords = [lon, lat] if elev is None else [lon, lat, elev]

    return {
        "type": "Feature",
        "id": feature_id,
        "geometry": {"type": "Point", "coordinates": coords},
        "properties": props,
    }


def dump_summary_collection(path: str, records: list, meta: dict) -> dict:
    """
    Write an OGC FeatureCollection of summary/site records to *path*.

    meta keys (all optional):
      id, title, description — collection metadata

    Returns the collection dict (for testing).
    §V: MUST include top-level id, type, numberReturned, timeStamp.
    §V: Each Feature MUST have top-level id.
    """
    collection_id = meta.get("id", "collection")
    features = [_make_feature(r, collection_id) for r in records]

    collection = {
        "type": "FeatureCollection",
        "id": collection_id,
        "title": meta.get("title", collection_id),
        "description": meta.get("description", ""),
        "timeStamp": _timestamp_now(),
        "numberMatched": len(features),
        "numberReturned": len(features),
        "links": [
            {
                "href": meta.get("href", ""),
                "rel": "self",
                "type": "application/geo+json",
            }
        ],
        "features": features,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(collection, f, indent=2, default=str)

    return collection


def dump_major_chemistry_collection(path: str, records: list, meta: dict) -> dict:
    """
    Write an OGC FeatureCollection of wells with major-chemistry analytes as
    properties to *path*. One Feature per well.

    *records* is a flat list of SummaryRecord — one per (well, analyte). They are
    pivoted by well: each analyte contributes ``<analyte>`` (latest value),
    ``<analyte>_units``, and ``<analyte>_date`` properties. Well identity is
    ``(source, id)``; well_depth and geometry come from any of the well's
    records.

    Returns the collection dict (for testing).
    §V: MUST include top-level id, type, numberReturned, timeStamp.
    §V: Each Feature MUST have top-level id.
    """
    collection_id = meta.get("id", "collection")

    wells: dict = {}
    for r in records:
        source = getattr(r, "source", "") or ""
        rid = getattr(r, "id", "") or ""
        key = (source, rid)
        well = wells.get(key)
        if well is None:
            well = {
                "source": source,
                "id": rid,
                "name": getattr(r, "name", None),
                "latitude": getattr(r, "latitude", None),
                "longitude": getattr(r, "longitude", None),
                "elevation": getattr(r, "elevation", None),
                "well_depth": getattr(r, "well_depth", None),
                "well_depth_units": getattr(r, "well_depth_units", None),
                "analytes": {},
            }
            wells[key] = well
        # well_depth can be absent on some analyte records; keep first non-null.
        if well["well_depth"] is None and getattr(r, "well_depth", None) is not None:
            well["well_depth"] = getattr(r, "well_depth", None)
            well["well_depth_units"] = getattr(r, "well_depth_units", None)

        analyte = getattr(r, "parameter_name", None)
        if analyte:
            well["analytes"][analyte] = {
                "value": getattr(r, "latest_value", None),
                "units": getattr(r, "latest_units", None),
                "date": getattr(r, "latest_date", None),
            }

    features = []
    for (source, rid), well in wells.items():
        feature_id = f"{source}:{rid}" if source and rid else str(rid)
        props = {
            "source": source,
            "id": rid,
            "name": well["name"],
            "well_depth": well["well_depth"],
            "well_depth_units": well["well_depth_units"],
        }
        for analyte, vals in well["analytes"].items():
            props[analyte] = vals["value"]
            props[f"{analyte}_units"] = vals["units"]
            props[f"{analyte}_date"] = vals["date"]

        lat, lon, elev = well["latitude"], well["longitude"], well["elevation"]
        coords = [lon, lat] if elev is None else [lon, lat, elev]
        features.append({
            "type": "Feature",
            "id": feature_id,
            "geometry": {"type": "Point", "coordinates": coords},
            "properties": props,
        })

    collection = {
        "type": "FeatureCollection",
        "id": collection_id,
        "title": meta.get("title", collection_id),
        "description": meta.get("description", ""),
        "timeStamp": _timestamp_now(),
        "numberMatched": len(features),
        "numberReturned": len(features),
        "links": [
            {
                "href": meta.get("href", ""),
                "rel": "self",
                "type": "application/geo+json",
            }
        ],
        "features": features,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(collection, f, indent=2, default=str)

    return collection


def dump_timeseries_collection(
    path: str,
    site_records: list,
    timeseries_records: list,
    meta: dict,
    site_lookup: Optional[dict] = None,
) -> dict:
    """
    Write an OGC FeatureCollection of flat timeseries observations to *path*.

    Each feature = one observation (not one well).
    §V: ogc_timeseries features MUST be flat, one per observation.
    §V: MUST have ISO 8601 `datetime` property.
    §V: Each Feature MUST have top-level id.

    site_lookup: {site_id -> SiteRecord} for geometry lookup.
                 Built from site_records if not provided.
    """
    collection_id = meta.get("id", "collection")

    if site_lookup is None:
        site_lookup = {}
        for sr in site_records:
            key = getattr(sr, "id", None)
            if key:
                site_lookup[key] = sr

    features = []
    for obs in timeseries_records:
        site_id = getattr(obs, "id", None)
        site = site_lookup.get(site_id)

        if site:
            lat = getattr(site, "latitude", None)
            lon = getattr(site, "longitude", None)
            elev = getattr(site, "elevation", None)
            coords = [lon, lat] if elev is None else [lon, lat, elev]
            geometry = {"type": "Point", "coordinates": coords}
        else:
            geometry = None

        date = getattr(obs, "date_measured", None)
        time = getattr(obs, "time_measured", None)
        if date and time:
            dt = f"{date}T{time}Z"
        elif date:
            dt = f"{date}T00:00:00Z"
        else:
            dt = None

        source = getattr(obs, "source", "")
        feature_id = f"{source}:{site_id}:{date}" if date else f"{source}:{site_id}"

        props = {k: getattr(obs, k) for k in obs.keys}
        props["datetime"] = dt

        features.append({
            "type": "Feature",
            "id": feature_id,
            "geometry": geometry,
            "properties": props,
        })

    collection = {
        "type": "FeatureCollection",
        "id": collection_id,
        "title": meta.get("title", collection_id),
        "description": meta.get("description", ""),
        "timeStamp": _timestamp_now(),
        "numberMatched": len(features),
        "numberReturned": len(features),
        "links": [
            {
                "href": meta.get("href", ""),
                "rel": "self",
                "type": "application/geo+json",
            }
        ],
        "features": features,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(collection, f, indent=2, default=str)

    return collection
