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

# Seconds per Julian year (365.25 days) — used to express the Theil-Sen slope
# in ft/year.
_SECONDS_PER_YEAR = 31557600.0

# A well is classified only when it has enough data for a meaningful
# Mann-Kendall test: at least 10 measurements, or at least 4 spanning >= 2 years.
_TREND_MIN_RECORDS = 10
_TREND_MIN_RECORDS_WITH_SPAN = 4
_TREND_MIN_SPAN_YEARS = 2.0
_TREND_ALPHA = 0.05  # significance level for the Mann-Kendall test

_MK_COMMON = (
    "Monotonic trend is tested with the non-parametric Mann-Kendall test "
    "(pymannkendall.original_test, alpha=0.05) on the daily series ordered by "
    "time; the rate is the Theil-Sen slope vs time. A well is classified only "
    "when it has at least 10 daily points, or at least 4 daily points spanning "
    "at least 2 years; otherwise 'not enough data'."
)

# Human-readable description of each trend product's method, embedded in the
# collection so consumers know how the classification was derived.
TREND_METHOD_DESCRIPTION = (
    "Depth-to-water trend per well. Observations are downsampled to one point "
    "per calendar day, keeping the daily MINIMUM depth-to-water (shallowest "
    f"reading). {_MK_COMMON} A significant increasing slope is 'increasing' "
    "(water level getting DEEPER, i.e. a declining water table), a significant "
    "decreasing slope is 'decreasing' (water level getting SHALLOWER), else "
    "'stable'. Mirrors the Ocotillo API ogc_depth_to_water_trend_wells "
    "materialized view, using Mann-Kendall + Theil-Sen instead of OLS."
)

ANALYTE_TREND_METHOD_DESCRIPTION = (
    "Analyte concentration trend per well. Observations are downsampled to one "
    f"point per calendar day, keeping the daily MEAN concentration. {_MK_COMMON} "
    "A significant increasing slope is 'increasing' (concentration rising), a "
    "significant decreasing slope is 'decreasing', else 'stable'."
)


def _timestamp_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_utc(epoch) -> Optional[str]:
    """Format POSIX seconds as an ISO-8601 UTC string (None passes through)."""
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _feature_id(source, rid) -> str:
    source = source or ""
    rid = rid or ""
    return f"{source}:{rid}" if source and rid else str(rid)


def _point_geometry(lat, lon, elev=None) -> dict:
    coords = [lon, lat] if elev is None else [lon, lat, elev]
    return {"type": "Point", "coordinates": coords}


def _dump_collection(
    path: str, collection_id: str, features: list, meta: dict, extra: Optional[dict] = None
) -> dict:
    """Build the OGC FeatureCollection envelope around *features*, write it to
    *path*, and return it. *extra* injects collection-level keys (e.g.
    trend_method) after description.

    §V: MUST include top-level id, type, numberReturned, timeStamp.
    """
    collection = {
        "type": "FeatureCollection",
        "id": collection_id,
        "title": meta.get("title", collection_id),
        "description": meta.get("description", ""),
        **(extra or {}),
        "timeStamp": _timestamp_now(),
        "numberMatched": len(features),
        "numberReturned": len(features),
        "links": [
            {"href": meta.get("href", ""), "rel": "self", "type": "application/geo+json"}
        ],
        "features": features,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(collection, f, indent=2, default=str)
    return collection


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


def _daily_series(obs_list: list, reducer: str = "min") -> tuple[int, list]:
    """Reduce a well's observations to one point per calendar day, keyed at the
    day's UTC midnight epoch. *reducer* selects the daily aggregate: "min"
    (shallowest depth-to-water), "max", or "mean" (e.g. analyte concentration).

    *obs_list* is a list of observation payload dicts (parameter_value,
    date_measured, time_measured). Operating on dicts avoids rebuilding
    ParameterRecord objects for what can be millions of observations.

    Downsampling bounds the O(n^2) Mann-Kendall cost for high-frequency wells
    (e.g. continuous loggers) and removes within-day sampling noise. Returns
    (raw_observation_count, [(day_epoch_seconds, value), ...] sorted by day).
    """
    raw_count = 0
    buckets: dict = {}  # date -> (day_epoch, [values])
    for obs in obs_list:
        value = obs.get("parameter_value")
        epoch = _parse_epoch_seconds(
            obs.get("date_measured"), obs.get("time_measured")
        )
        if value is None or epoch is None:
            continue
        try:
            v = float(value)
        except (TypeError, ValueError):
            continue
        raw_count += 1
        day = datetime.fromtimestamp(epoch, tz=timezone.utc).date()
        day_epoch = datetime(
            day.year, day.month, day.day, tzinfo=timezone.utc
        ).timestamp()
        if day not in buckets:
            buckets[day] = (day_epoch, [])
        buckets[day][1].append(v)

    reduce_fn = {
        "min": min,
        "max": max,
        "mean": lambda vs: sum(vs) / len(vs),
    }[reducer]

    pairs = sorted(
        ((day_epoch, reduce_fn(vals)) for day_epoch, vals in buckets.values()),
        key=lambda p: p[0],
    )
    return raw_count, pairs


def _qualifies_for_trend(record_count, span_years) -> bool:
    return record_count >= _TREND_MIN_RECORDS or (
        record_count >= _TREND_MIN_RECORDS_WITH_SPAN
        and span_years >= _TREND_MIN_SPAN_YEARS
    )


def _mann_kendall_trend(years: list, values: list):
    """Run the Mann-Kendall trend test + Theil-Sen slope.

    Returns (trend_category, slope_ft_per_year, p_value, tau). *years* are
    decimal years, *values* depth-to-water (ft), both ordered by time.
    trend_category is one of 'increasing' / 'decreasing' / 'stable'.
    """
    import pymannkendall as mk
    from scipy.stats import theilslopes

    result = mk.original_test(values, alpha=_TREND_ALPHA)
    # Time-aware Theil-Sen slope (ft/year) — robust and correct for the
    # irregular sampling typical of water-level records, unlike MK's index-based
    # slope which assumes unit spacing.
    slope_ft_per_year = float(theilslopes(values, years)[0])

    # mk trend is 'increasing' / 'decreasing' / 'no trend'.
    category = "stable" if result.trend == "no trend" else result.trend
    return category, slope_ft_per_year, float(result.p), float(result.Tau)


def _make_feature(record, collection_id: str) -> dict:
    """Build one OGC-compliant Feature from a SummaryRecord or SiteRecord."""
    props = {k: getattr(record, k) for k in record.keys
             if k not in ("latitude", "longitude", "elevation")}

    return {
        "type": "Feature",
        "id": _feature_id(getattr(record, "source", ""), getattr(record, "id", "")),
        "geometry": _point_geometry(
            getattr(record, "latitude", None),
            getattr(record, "longitude", None),
            getattr(record, "elevation", None),
        ),
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
    return _dump_collection(path, collection_id, features, meta)


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

        features.append({
            "type": "Feature",
            "id": _feature_id(source, rid),
            "geometry": _point_geometry(well["latitude"], well["longitude"], well["elevation"]),
            "properties": props,
        })

    return _dump_collection(path, collection_id, features, meta)


def dump_trend_collection(
    path: str,
    site_records: list,
    timeseries_records: list,
    meta: dict,
    *,
    slope_units: str,
    reducer: str = "min",
    method: str = TREND_METHOD_DESCRIPTION,
    parameter_name: Optional[str] = None,
) -> dict:
    """
    Write an OGC FeatureCollection of per-well Mann-Kendall trends to *path*.
    One Feature per well. Used for both water-level and analyte trends.

    *site_records* and *timeseries_records* are index-aligned **payload dicts**
    (consumed as dicts, not rebuilt into record objects, to keep memory bounded).
    Each well's observations are downsampled to one point per calendar day using
    *reducer* ("min" for depth-to-water, "mean" for analytes), then tested with
    Mann-Kendall + Theil-Sen.

    Each feature carries: parameter_name, record_count (daily points used),
    observation_count (raw readings), first/last_observation_datetime,
    span_years, slope_per_year (Theil-Sen) + slope_units, trend_category
    (Mann-Kendall), mk_p_value, mk_tau, well_depth(+units), and
    source_datastream_link when available. The collection carries
    ``trend_method`` (*method*).

    §V: MUST include top-level id, type, numberReturned, timeStamp.
    §V: Each Feature MUST have top-level id.
    """
    collection_id = meta.get("id", "collection")

    features = []
    for site, obs_list in zip(site_records, timeseries_records):
        observation_count, pairs = _daily_series(obs_list, reducer)
        record_count = len(pairs)
        xs = [p[0] for p in pairs]
        ys = [p[1] for p in pairs]
        span_years = (xs[-1] - xs[0]) / _SECONDS_PER_YEAR if record_count >= 2 else 0.0

        slope_per_year = None
        p_value = None
        tau = None
        if _qualifies_for_trend(record_count, span_years):
            years = [x / _SECONDS_PER_YEAR for x in xs]
            trend_category, slope_per_year, p_value, tau = _mann_kendall_trend(
                years, ys
            )
        else:
            trend_category = "not enough data"

        props = {
            "source": site.get("source") or "",
            "id": site.get("id") or "",
            "name": site.get("name"),
            "parameter_name": parameter_name,
            "well_depth": site.get("well_depth"),
            "well_depth_units": site.get("well_depth_units"),
            "record_count": record_count,
            "observation_count": observation_count,
            "first_observation_datetime": _iso_utc(xs[0]) if record_count else None,
            "last_observation_datetime": _iso_utc(xs[-1]) if record_count else None,
            "span_years": round(span_years, 3),
            "slope_per_year": (
                None if slope_per_year is None else round(slope_per_year, 4)
            ),
            "slope_units": slope_units,
            "trend_category": trend_category,
            "mk_p_value": None if p_value is None else round(p_value, 4),
            "mk_tau": None if tau is None else round(tau, 4),
        }

        # Link to the raw, non-normalized source datastream used for the
        # calculation, when the source provides one (SensorThings/st2).
        source_datastream_link = next(
            (
                o.get("source_datastream_link")
                for o in obs_list
                if o.get("source_datastream_link")
            ),
            None,
        )
        if source_datastream_link:
            props["source_datastream_link"] = source_datastream_link

        features.append({
            "type": "Feature",
            "id": _feature_id(props["source"], props["id"]),
            "geometry": _point_geometry(
                site.get("latitude"),
                site.get("longitude"),
                site.get("elevation"),
            ),
            "properties": props,
        })

    return _dump_collection(
        path, collection_id, features, meta, extra={"trend_method": method}
    )


def dump_mcl_exceedance_collection(
    path: str, records: list, meta: dict, thresholds: dict
) -> dict:
    """
    Write an OGC FeatureCollection flagging drinking-water MCL exceedances, one
    Feature per well.

    *records* is a flat list of SummaryRecord — one per (well, analyte) — pivoted
    by well (like the major-chemistry product). *thresholds* maps analyte ->
    {"mcl": <number, same units as the data (mg/L)>, "type": "primary"|"secondary"}
    and is the source of truth for which analytes have an MCL.

    Per analyte present on a well: ``<analyte>`` (latest value), ``<analyte>_mcl``,
    ``<analyte>_mcl_type``, and ``<analyte>_exceeds`` (value > mcl). Plus
    ``any_exceedance`` (bool), ``exceedance_count`` (int), and
    ``exceeded_analytes`` (list). The collection carries ``mcl_thresholds``.
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
                "source": source, "id": rid, "name": getattr(r, "name", None),
                "latitude": getattr(r, "latitude", None),
                "longitude": getattr(r, "longitude", None),
                "elevation": getattr(r, "elevation", None),
                "well_depth": getattr(r, "well_depth", None),
                "well_depth_units": getattr(r, "well_depth_units", None),
                "values": {},
            }
            wells[key] = well
        analyte = getattr(r, "parameter_name", None)
        if analyte:
            well["values"][analyte] = getattr(r, "latest_value", None)

    features = []
    for (source, rid), well in wells.items():
        props = {
            "source": source, "id": rid, "name": well["name"],
            "well_depth": well["well_depth"],
            "well_depth_units": well["well_depth_units"],
        }
        exceeded = []
        for analyte, value in well["values"].items():
            props[analyte] = value
            limit = thresholds.get(analyte)
            if not limit:
                continue
            mcl = limit.get("mcl")
            props[f"{analyte}_mcl"] = mcl
            props[f"{analyte}_mcl_type"] = limit.get("type")
            exceeds = value is not None and mcl is not None and value > mcl
            props[f"{analyte}_exceeds"] = exceeds
            if exceeds:
                exceeded.append(analyte)

        props["any_exceedance"] = bool(exceeded)
        props["exceedance_count"] = len(exceeded)
        props["exceeded_analytes"] = sorted(exceeded)

        features.append({
            "type": "Feature",
            "id": _feature_id(source, rid),
            "geometry": _point_geometry(
                well["latitude"], well["longitude"], well["elevation"]
            ),
            "properties": props,
        })

    return _dump_collection(
        path, collection_id, features, meta, extra={"mcl_thresholds": thresholds}
    )


def dump_monitoring_recency_collection(
    path: str,
    site_records: list,
    timeseries_records: list,
    meta: dict,
    *,
    run_date: str,
    stale_days: int = 365,
) -> dict:
    """
    Write an OGC FeatureCollection of monitoring recency, one Feature per well.

    *site_records* and *timeseries_records* are index-aligned payload dicts. Per
    well: ``record_count``, ``first_observation_datetime``,
    ``last_observation_datetime``, ``days_since_last`` (relative to *run_date*),
    and ``status`` ("active" if days_since_last <= *stale_days*, else "stale";
    "no data" when the well has no observations). Surfaces dead/lagging
    monitoring points.
    """
    collection_id = meta.get("id", "collection")
    run_epoch = _parse_epoch_seconds(run_date, None)

    features = []
    for site, obs_list in zip(site_records, timeseries_records):
        epochs = [
            e for e in (
                _parse_epoch_seconds(o.get("date_measured"), o.get("time_measured"))
                for o in obs_list
            ) if e is not None
        ]
        record_count = len(epochs)
        if record_count:
            first_e, last_e = min(epochs), max(epochs)
            days_since_last = (
                int((run_epoch - last_e) // 86400) if run_epoch is not None else None
            )
            if days_since_last is None:
                status = "unknown"
            else:
                status = "active" if days_since_last <= stale_days else "stale"
        else:
            first_e = last_e = None
            days_since_last = None
            status = "no data"

        props = {
            "source": site.get("source") or "",
            "id": site.get("id") or "",
            "name": site.get("name"),
            "well_depth": site.get("well_depth"),
            "well_depth_units": site.get("well_depth_units"),
            "record_count": record_count,
            "first_observation_datetime": _iso_utc(first_e),
            "last_observation_datetime": _iso_utc(last_e),
            "days_since_last": days_since_last,
            "status": status,
        }
        features.append({
            "type": "Feature",
            "id": _feature_id(props["source"], props["id"]),
            "geometry": _point_geometry(
                site.get("latitude"), site.get("longitude"), site.get("elevation")
            ),
            "properties": props,
        })

    return _dump_collection(
        path, collection_id, features, meta,
        extra={"stale_threshold_days": stale_days},
    )


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
            geometry = _point_geometry(
                getattr(site, "latitude", None),
                getattr(site, "longitude", None),
                getattr(site, "elevation", None),
            )
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

    return _dump_collection(path, collection_id, features, meta)
