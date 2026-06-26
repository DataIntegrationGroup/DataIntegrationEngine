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

# Seconds per Julian year (365.25 days) — matches Ocotillo's trend MV, which
# divides the per-second regression slope by 31557600 to get ft/year.
_SECONDS_PER_YEAR = 31557600.0

# Trend classification thresholds, ported verbatim from the Ocotillo
# ogc_depth_to_water_trend_wells materialized view.
_TREND_SLOPE_THRESHOLD = 0.25  # ft/year
_TREND_MIN_RECORDS = 10
_TREND_MIN_RECORDS_WITH_SPAN = 4
_TREND_MIN_SPAN_YEARS = 2.0

# Human-readable description of the trend method, embedded in the product so
# consumers know how the classification was derived.
TREND_METHOD_DESCRIPTION = (
    "Depth-to-water trend per well. A least-squares linear regression "
    "(equivalent to PostgreSQL REGR_SLOPE) is fit to depth-to-water-below-"
    "ground-surface (feet) against observation time; the per-second slope is "
    "scaled by 31,557,600 s/yr (a 365.25-day year) to slope_ft_per_year. A well "
    "is classified only when it has at least 10 measurements, or at least 4 "
    "measurements spanning at least 2 years; otherwise 'not enough data'. When "
    "classified: slope > 0.25 ft/yr is 'increasing' (water level getting "
    "DEEPER, i.e. a declining water table), slope < -0.25 ft/yr is 'decreasing' "
    "(water level getting SHALLOWER), otherwise 'stable'. Ported from the "
    "Ocotillo API ogc_depth_to_water_trend_wells materialized view."
)


def _timestamp_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_epoch_seconds(date, time) -> Optional[float]:
    """Best-effort parse of a DIE date (+optional time) to POSIX seconds (UTC)."""
    if not date:
        return None
    text = f"{date}T{time}" if time else str(date)
    text = text.replace("Z", "")
    for parse in (datetime.fromisoformat,):
        try:
            dt = parse(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except (ValueError, TypeError):
            pass
    try:
        dt = datetime.fromisoformat(str(date)).replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (ValueError, TypeError):
        return None


def _regr_slope(xs: list, ys: list) -> Optional[float]:
    """Least-squares slope of ys on xs (PostgreSQL REGR_SLOPE). None if x has no
    variance (fewer than 2 distinct x values)."""
    n = len(xs)
    if n < 2:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0:
        return None
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return sxy / sxx


def _classify_trend(slope_ft_per_year, record_count, span_years) -> str:
    qualifies = record_count >= _TREND_MIN_RECORDS or (
        record_count >= _TREND_MIN_RECORDS_WITH_SPAN
        and span_years >= _TREND_MIN_SPAN_YEARS
    )
    if not qualifies or slope_ft_per_year is None:
        return "not enough data"
    if slope_ft_per_year > _TREND_SLOPE_THRESHOLD:
        return "increasing"
    if slope_ft_per_year < -_TREND_SLOPE_THRESHOLD:
        return "decreasing"
    return "stable"


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


def dump_waterlevel_trend_collection(
    path: str,
    site_records: list,
    timeseries_records: list,
    meta: dict,
) -> dict:
    """
    Write an OGC FeatureCollection of per-well depth-to-water trends to *path*.
    One Feature per well.

    *site_records* and *timeseries_records* are index-aligned: ``site_records[i]``
    is the well and ``timeseries_records[i]`` is its list of ParameterRecord
    observations (DIE water-level values are already depth-to-water below ground
    surface in feet, so no measuring-point adjustment is applied here).

    Each feature carries: record_count, first/last_observation_datetime,
    span_years, slope_ft_per_year, trend_category, well_depth(+units). The
    collection carries ``trend_method`` describing the calculation. See
    TREND_METHOD_DESCRIPTION.

    §V: MUST include top-level id, type, numberReturned, timeStamp.
    §V: Each Feature MUST have top-level id.
    """
    collection_id = meta.get("id", "collection")

    features = []
    for site, obs_list in zip(site_records, timeseries_records):
        pairs = []
        for obs in obs_list:
            value = getattr(obs, "parameter_value", None)
            epoch = _parse_epoch_seconds(
                getattr(obs, "date_measured", None), getattr(obs, "time_measured", None)
            )
            if value is None or epoch is None:
                continue
            try:
                pairs.append((epoch, float(value)))
            except (TypeError, ValueError):
                continue

        pairs.sort(key=lambda p: p[0])
        record_count = len(pairs)
        xs = [p[0] for p in pairs]
        ys = [p[1] for p in pairs]

        if record_count >= 2:
            span_years = (xs[-1] - xs[0]) / _SECONDS_PER_YEAR
            slope = _regr_slope(xs, ys)
            slope_ft_per_year = None if slope is None else slope * _SECONDS_PER_YEAR
        else:
            span_years = 0.0
            slope_ft_per_year = None

        trend_category = _classify_trend(slope_ft_per_year, record_count, span_years)

        source = getattr(site, "source", "") or ""
        rid = getattr(site, "id", "") or ""
        feature_id = f"{source}:{rid}" if source and rid else str(rid)

        def _iso(epoch):
            if epoch is None:
                return None
            return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        props = {
            "source": source,
            "id": rid,
            "name": getattr(site, "name", None),
            "well_depth": getattr(site, "well_depth", None),
            "well_depth_units": getattr(site, "well_depth_units", None),
            "record_count": record_count,
            "first_observation_datetime": _iso(xs[0]) if record_count else None,
            "last_observation_datetime": _iso(xs[-1]) if record_count else None,
            "span_years": round(span_years, 3),
            "slope_ft_per_year": (
                None if slope_ft_per_year is None else round(slope_ft_per_year, 4)
            ),
            "trend_category": trend_category,
        }

        lat = getattr(site, "latitude", None)
        lon = getattr(site, "longitude", None)
        elev = getattr(site, "elevation", None)
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
        "trend_method": TREND_METHOD_DESCRIPTION,
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
