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
import math
from datetime import datetime, timezone
from typing import Optional

from backend.constants import TDS

# Trend statistics live in backend/trend_stats.py (analysis, not serialization).
# Re-exported here so existing importers (and dump_trend_collection's default
# method arg) keep working.
from backend.trend_stats import (
    _SECONDS_PER_YEAR,
    ANALYTE_TREND_METHOD_DESCRIPTION,
    TREND_METHOD_DESCRIPTION,
    daily_series as _daily_series,
    mann_kendall_trend as _mann_kendall_trend,
    parse_epoch_seconds as _parse_epoch_seconds,
    qualifies_for_trend as _qualifies_for_trend,
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


TDS_CLASS_METHOD_DESCRIPTION = (
    "TDS classification (USGS): fresh (<1,000 mg/L), brackish (1,000-10,000 "
    "mg/L), saline (10,000-35,000 mg/L), brine (>=35,000 mg/L); 'insufficient' "
    "when the well has no latest TDS value."
)


def _tds_class(value: Optional[float]) -> str:
    if value is None:
        return "insufficient"
    if value < 1000:
        return "fresh"
    if value < 10000:
        return "brackish"
    if value < 35000:
        return "saline"
    return "brine"


def _make_feature(record, collection_id: str) -> dict:
    """Build one OGC-compliant Feature from a SummaryRecord or SiteRecord."""
    props = {k: getattr(record, k) for k in record.keys
             if k not in ("latitude", "longitude", "elevation")}

    if getattr(record, "parameter_name", None) == TDS:
        props["tds_class"] = _tds_class(_num(getattr(record, "latest_value", None)))

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


def _num(value) -> Optional[float]:
    """Coerce a value to float, or None when missing/unparseable."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pivot_by_well(records: list) -> dict:
    """Pivot a flat list of per-(well, analyte) SummaryRecords into one dict per
    well, keyed ``(source, id)``. Each well carries its geometry/depth plus an
    ``analytes`` map ``{name: {"value", "units", "date"}}``. Same identity and
    well_depth-carry rules as :func:`dump_major_chemistry_collection`."""
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
    return wells


def _well_feature(source, rid, well: dict, props: dict) -> dict:
    """Build a Feature for a pivoted well (geometry from the well dict)."""
    return {
        "type": "Feature",
        "id": _feature_id(source, rid),
        "geometry": _point_geometry(
            well["latitude"], well["longitude"], well["elevation"]
        ),
        "properties": props,
    }


def _site_feature(site: dict, props: dict) -> dict:
    """Build a Feature for a site payload dict (geometry from the site dict)."""
    return {
        "type": "Feature",
        "id": _feature_id(site.get("source") or "", site.get("id") or ""),
        "geometry": _point_geometry(
            site.get("latitude"), site.get("longitude"), site.get("elevation")
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
    extra = None
    if any(getattr(r, "parameter_name", None) == TDS for r in records):
        extra = {"tds_class_method": TDS_CLASS_METHOD_DESCRIPTION}
    return _dump_collection(path, collection_id, features, meta, extra=extra)


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
                None if slope_per_year is None else round(slope_per_year, 2)
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

    Per analyte present on a well: ``<analyte>`` (latest value), ``<analyte>_date``
    (sample/analysis date of that value), ``<analyte>_mcl``, ``<analyte>_mcl_type``,
    and ``<analyte>_exceeds`` (value > mcl). Plus ``any_exceedance`` (bool),
    ``exceedance_count`` (int), and ``exceeded_analytes`` (list). The collection
    carries ``mcl_thresholds``.
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
            well["values"][analyte] = {
                "value": getattr(r, "latest_value", None),
                "date": getattr(r, "latest_date", None),
            }

    features = []
    for (source, rid), well in wells.items():
        props = {
            "source": source, "id": rid, "name": well["name"],
            "well_depth": well["well_depth"],
            "well_depth_units": well["well_depth_units"],
        }
        exceeded = []
        for analyte, entry in well["values"].items():
            value = entry["value"]
            props[analyte] = value
            props[f"{analyte}_date"] = entry["date"]
            limit = thresholds.get(analyte)
            if not limit:
                continue
            mcl = limit.get("mcl")
            props[f"{analyte}_mcl"] = mcl
            props[f"{analyte}_mcl_type"] = limit.get("type")
            # Direct magnitude comparison: the MCL and the value MUST share
            # units AND basis. NOTE (nitrate): the EPA MCL is 10 mg/L measured
            # "as N", but providers/DIE may report nitrate "as NO3" (~4.43x
            # larger). If the data basis is NO3, this flag is wrong unless the
            # mcl in config/mcl.json is the as-NO3 value (~44.3 mg/L). Confirm
            # DIE's normalized nitrate basis before trusting nitrate exceedances.
            num = _num(value)
            exceeds = num is not None and mcl is not None and num > mcl
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


# Water hardness as CaCO3 (mg/L) = 2.497*Ca + 4.118*Mg, with Ca, Mg in mg/L
# (standard USGS factors: each ion's CaCO3 equivalent weight / its own).
_HARDNESS_CA_FACTOR = 2.497
_HARDNESS_MG_FACTOR = 4.118

HARDNESS_METHOD_DESCRIPTION = (
    "Total hardness as CaCO3 (mg/L) = 2.497*calcium + 4.118*magnesium, using each "
    "well's latest calcium and magnesium (both in mg/L). Class (USGS): soft "
    "(<60), moderate (60-120), hard (120-180), very hard (>=180); 'insufficient' "
    "when calcium or magnesium is missing. Calcium and magnesium are each the "
    "latest value reported and may carry different dates."
)


def _hardness_class(hardness: Optional[float]) -> str:
    if hardness is None:
        return "insufficient"
    if hardness < 60:
        return "soft"
    if hardness < 120:
        return "moderate"
    if hardness < 180:
        return "hard"
    return "very hard"


def dump_hardness_collection(path: str, records: list, meta: dict) -> dict:
    """
    Write an OGC FeatureCollection of per-well water hardness, one Feature per
    well. *records* is a flat list of SummaryRecord (one per well+analyte),
    pivoted by well; only calcium and magnesium are used.

    Per well: ``calcium``, ``calcium_date``, ``magnesium``, ``magnesium_date``,
    ``hardness_caco3`` (mg/L as CaCO3, None when either ion is missing),
    ``hardness_units``, and ``hardness_class``. The collection carries
    ``hardness_method``.
    """
    collection_id = meta.get("id", "collection")
    wells = _pivot_by_well(records)

    features = []
    for (source, rid), well in wells.items():
        analytes = well["analytes"]
        ca = _num(analytes.get("calcium", {}).get("value"))
        mg = _num(analytes.get("magnesium", {}).get("value"))
        if ca is None or mg is None:
            hardness = None
        else:
            hardness = round(
                _HARDNESS_CA_FACTOR * ca + _HARDNESS_MG_FACTOR * mg, 1
            )

        props = {
            "source": source,
            "id": rid,
            "name": well["name"],
            "well_depth": well["well_depth"],
            "well_depth_units": well["well_depth_units"],
            "calcium": analytes.get("calcium", {}).get("value"),
            "calcium_date": analytes.get("calcium", {}).get("date"),
            "magnesium": analytes.get("magnesium", {}).get("value"),
            "magnesium_date": analytes.get("magnesium", {}).get("date"),
            "hardness_caco3": hardness,
            "hardness_units": "mg/L as CaCO3",
            "hardness_class": _hardness_class(hardness),
        }
        features.append(_well_feature(source, rid, well, props))

    return _dump_collection(
        path, collection_id, features, meta,
        extra={"hardness_method": HARDNESS_METHOD_DESCRIPTION},
    )


# Equivalent weights (mg per milliequivalent) for the major ions, used to convert
# mg/L concentrations to meq/L before computing milliequivalent percentages.
_EQUIVALENT_WEIGHTS = {
    "calcium": 20.04,
    "magnesium": 12.15,
    "sodium": 22.99,
    "potassium": 39.10,
    "bicarbonate": 61.02,
    "carbonate": 30.00,
    "chloride": 35.45,
    "sulfate": 48.03,
}

WATER_TYPE_METHOD_DESCRIPTION = (
    "Hydrochemical (Piper) water type from the latest major-ion chemistry. Each "
    "ion (mg/L) is converted to meq/L by its equivalent weight; cations are Ca, "
    "Mg, and Na+K, anions are HCO3+CO3, Cl, and SO4. The dominant cation and "
    "anion are those exceeding 50% of their group's meq total, else 'mixed'; "
    "water_type is 'dominantCation-dominantAnion'. 'insufficient' when no cations "
    "or no anions are reported. charge_balance_pct = 100*(cations-anions)/"
    "(cations+anions); magnitudes above ~10% indicate suspect/incomplete analyses."
)


def _meq(analytes: dict, name: str) -> float:
    """meq/L for one ion from the pivoted analyte map (0.0 when absent)."""
    value = _num(analytes.get(name, {}).get("value"))
    if value is None:
        return 0.0
    return value / _EQUIVALENT_WEIGHTS[name]


def _dominant(percentages: dict) -> str:
    """Label of the >50% member, else 'mixed'."""
    label, pct = max(percentages.items(), key=lambda kv: kv[1])
    return label if pct > 50 else "mixed"


def dump_water_type_collection(path: str, records: list, meta: dict) -> dict:
    """
    Write an OGC FeatureCollection of per-well hydrochemical (Piper) water type,
    one Feature per well. *records* is a flat list of SummaryRecord (one per
    well+analyte), pivoted by well; the eight major ions are used.

    Per well: ``water_type``, ``dominant_cation``, ``dominant_anion``, the
    milliequivalent percentages ``ca_pct``/``mg_pct``/``na_k_pct`` (cations) and
    ``hco3_pct``/``cl_pct``/``so4_pct`` (anions), ``cation_meq_total``,
    ``anion_meq_total``, and ``charge_balance_pct``. Wells with no cations or no
    anions get ``water_type='insufficient'`` and null percentages. The collection
    carries ``water_type_method``.
    """
    collection_id = meta.get("id", "collection")
    wells = _pivot_by_well(records)

    features = []
    for (source, rid), well in wells.items():
        a = well["analytes"]
        ca = _meq(a, "calcium")
        mg = _meq(a, "magnesium")
        na_k = _meq(a, "sodium") + _meq(a, "potassium")
        hco3_co3 = _meq(a, "bicarbonate") + _meq(a, "carbonate")
        cl = _meq(a, "chloride")
        so4 = _meq(a, "sulfate")

        cation_total = ca + mg + na_k
        anion_total = hco3_co3 + cl + so4

        props = {
            "source": source,
            "id": rid,
            "name": well["name"],
            "well_depth": well["well_depth"],
            "well_depth_units": well["well_depth_units"],
            "cation_meq_total": round(cation_total, 3),
            "anion_meq_total": round(anion_total, 3),
        }
        if cation_total <= 0 or anion_total <= 0:
            props.update({
                "water_type": "insufficient",
                "dominant_cation": None,
                "dominant_anion": None,
                "ca_pct": None, "mg_pct": None, "na_k_pct": None,
                "hco3_pct": None, "cl_pct": None, "so4_pct": None,
                "charge_balance_pct": None,
            })
        else:
            ca_pct = 100 * ca / cation_total
            mg_pct = 100 * mg / cation_total
            na_k_pct = 100 * na_k / cation_total
            hco3_pct = 100 * hco3_co3 / anion_total
            cl_pct = 100 * cl / anion_total
            so4_pct = 100 * so4 / anion_total
            dom_cation = _dominant(
                {"Ca": ca_pct, "Mg": mg_pct, "Na+K": na_k_pct}
            )
            dom_anion = _dominant({"HCO3": hco3_pct, "Cl": cl_pct, "SO4": so4_pct})
            props.update({
                "water_type": f"{dom_cation}-{dom_anion}",
                "dominant_cation": dom_cation,
                "dominant_anion": dom_anion,
                "ca_pct": round(ca_pct, 1),
                "mg_pct": round(mg_pct, 1),
                "na_k_pct": round(na_k_pct, 1),
                "hco3_pct": round(hco3_pct, 1),
                "cl_pct": round(cl_pct, 1),
                "so4_pct": round(so4_pct, 1),
                "charge_balance_pct": round(
                    100 * (cation_total - anion_total)
                    / (cation_total + anion_total),
                    1,
                ),
            })
        features.append(_well_feature(source, rid, well, props))

    return _dump_collection(
        path, collection_id, features, meta,
        extra={"water_type_method": WATER_TYPE_METHOD_DESCRIPTION},
    )


SAR_METHOD_DESCRIPTION = (
    "Sodium adsorption ratio SAR = Na / sqrt((Ca + Mg) / 2) with each ion's "
    "latest value converted from mg/L to meq/L by its equivalent weight. "
    "Irrigation hazard class (USDA Handbook 60): low (S1, <10), medium (S2, "
    "10-18), high (S3, 18-26), very high (S4, >=26); 'insufficient' when sodium "
    "is missing, or calcium and magnesium are both missing or sum to zero. Ion "
    "values are each the latest reported and may carry different dates."
)


def _sar_class(sar: Optional[float]) -> str:
    if sar is None:
        return "insufficient"
    if sar < 10:
        return "low"
    if sar < 18:
        return "medium"
    if sar < 26:
        return "high"
    return "very high"


def dump_sar_collection(path: str, records: list, meta: dict) -> dict:
    """
    Write an OGC FeatureCollection of per-well sodium adsorption ratio (SAR),
    one Feature per well. *records* is a flat list of SummaryRecord (one per
    well+analyte), pivoted by well; sodium, calcium, and magnesium are used.

    Per well: the three ion values and dates, ``sar`` (dimensionless, None when
    an input is missing), and ``sar_class`` (USDA S1-S4 as low/medium/high/very
    high). The collection carries ``sar_method``.
    """
    collection_id = meta.get("id", "collection")
    wells = _pivot_by_well(records)

    features = []
    for (source, rid), well in wells.items():
        a = well["analytes"]
        na = _num(a.get("sodium", {}).get("value"))
        ca = _num(a.get("calcium", {}).get("value"))
        mg = _num(a.get("magnesium", {}).get("value"))

        sar = None
        if na is not None and not (ca is None and mg is None):
            na_meq = na / _EQUIVALENT_WEIGHTS["sodium"]
            ca_meq = (ca or 0.0) / _EQUIVALENT_WEIGHTS["calcium"]
            mg_meq = (mg or 0.0) / _EQUIVALENT_WEIGHTS["magnesium"]
            denom = math.sqrt((ca_meq + mg_meq) / 2)
            if denom > 0:
                sar = round(na_meq / denom, 2)

        props = {
            "source": source,
            "id": rid,
            "name": well["name"],
            "well_depth": well["well_depth"],
            "well_depth_units": well["well_depth_units"],
            "sodium": na,
            "sodium_date": a.get("sodium", {}).get("date"),
            "calcium": ca,
            "calcium_date": a.get("calcium", {}).get("date"),
            "magnesium": mg,
            "magnesium_date": a.get("magnesium", {}).get("date"),
            "sar": sar,
            "sar_class": _sar_class(sar),
        }
        features.append(_well_feature(source, rid, well, props))

    return _dump_collection(
        path, collection_id, features, meta,
        extra={"sar_method": SAR_METHOD_DESCRIPTION},
    )


ION_BALANCE_METHOD_DESCRIPTION = (
    "Charge balance error from the latest major-ion chemistry. Each ion (mg/L) "
    "is converted to meq/L by its equivalent weight; cations are Ca, Mg, Na, K, "
    "anions are HCO3, CO3, Cl, SO4. charge_balance_pct = 100*(cations-anions)/"
    "(cations+anions). Class: balanced (|CBE| <= 5%), acceptable (<= 10%), "
    "suspect (> 10%); 'insufficient' when no cations or no anions are reported. "
    "Suspect analyses likely have a missing or erroneous major ion; use as a QA "
    "screen on the chemistry-derived products. Ion values are each the latest "
    "reported and may carry different dates."
)


def _ion_balance_class(cbe: Optional[float]) -> str:
    if cbe is None:
        return "insufficient"
    if abs(cbe) <= 5:
        return "balanced"
    if abs(cbe) <= 10:
        return "acceptable"
    return "suspect"


_CATIONS = ("calcium", "magnesium", "sodium", "potassium")
_ANIONS = ("bicarbonate", "carbonate", "chloride", "sulfate")


def dump_ion_balance_collection(path: str, records: list, meta: dict) -> dict:
    """
    Write an OGC FeatureCollection of per-well charge balance error, one Feature
    per well. *records* is a flat list of SummaryRecord (one per well+analyte),
    pivoted by well; the eight major ions are used.

    Per well: ``cation_meq_total``, ``anion_meq_total``, ``n_cations_reported``,
    ``n_anions_reported``, ``charge_balance_pct`` (None when either side is
    empty), and ``balance_class`` (balanced/acceptable/suspect/insufficient).
    The collection carries ``ion_balance_method``.
    """
    collection_id = meta.get("id", "collection")
    wells = _pivot_by_well(records)

    features = []
    for (source, rid), well in wells.items():
        a = well["analytes"]
        cation_total = sum(_meq(a, ion) for ion in _CATIONS)
        anion_total = sum(_meq(a, ion) for ion in _ANIONS)
        n_cations = sum(
            1 for ion in _CATIONS if _num(a.get(ion, {}).get("value")) is not None
        )
        n_anions = sum(
            1 for ion in _ANIONS if _num(a.get(ion, {}).get("value")) is not None
        )

        if cation_total <= 0 or anion_total <= 0:
            cbe = None
        else:
            cbe = round(
                100 * (cation_total - anion_total) / (cation_total + anion_total), 1
            )

        props = {
            "source": source,
            "id": rid,
            "name": well["name"],
            "well_depth": well["well_depth"],
            "well_depth_units": well["well_depth_units"],
            "cation_meq_total": round(cation_total, 3),
            "anion_meq_total": round(anion_total, 3),
            "n_cations_reported": n_cations,
            "n_anions_reported": n_anions,
            "charge_balance_pct": cbe,
            "balance_class": _ion_balance_class(cbe),
        }
        features.append(_well_feature(source, rid, well, props))

    return _dump_collection(
        path, collection_id, features, meta,
        extra={"ion_balance_method": ION_BALANCE_METHOD_DESCRIPTION},
    )


DATA_DENSITY_METHOD_DESCRIPTION = (
    "Per-well measurement coverage. observation_count is the raw count of valid "
    "numeric readings; record_count is the number of distinct calendar days with "
    "a reading; span_years is the time from first to last day. mean_interval_days "
    "is the average gap between distinct days; observations_per_year is "
    "observation_count / span_years."
)


def dump_data_density_collection(
    path: str,
    site_records: list,
    timeseries_records: list,
    meta: dict,
    *,
    parameter_name: Optional[str] = None,
) -> dict:
    """
    Write an OGC FeatureCollection of per-well data density / coverage, one
    Feature per well. *site_records* and *timeseries_records* are index-aligned
    payload dicts (as produced by the source assets).

    Per well: ``observation_count`` (raw valid readings), ``record_count``
    (distinct days), ``first_observation_datetime``, ``last_observation_datetime``,
    ``span_years``, ``mean_interval_days``, and ``observations_per_year``. The
    collection carries ``data_density_method``.
    """
    collection_id = meta.get("id", "collection")

    features = []
    for site, obs_list in zip(site_records, timeseries_records):
        observation_count, pairs = _daily_series(obs_list, "min")
        record_count = len(pairs)
        if record_count:
            first_e, last_e = pairs[0][0], pairs[-1][0]
            span_years = (last_e - first_e) / _SECONDS_PER_YEAR
        else:
            first_e = last_e = None
            span_years = 0.0

        mean_interval_days = (
            round((span_years * 365.25) / (record_count - 1), 1)
            if record_count > 1 else None
        )
        observations_per_year = (
            round(observation_count / span_years, 2) if span_years > 0 else None
        )

        props = {
            "source": site.get("source") or "",
            "id": site.get("id") or "",
            "name": site.get("name"),
            "parameter_name": parameter_name,
            "well_depth": site.get("well_depth"),
            "well_depth_units": site.get("well_depth_units"),
            "observation_count": observation_count,
            "record_count": record_count,
            "first_observation_datetime": _iso_utc(first_e),
            "last_observation_datetime": _iso_utc(last_e),
            "span_years": round(span_years, 3),
            "mean_interval_days": mean_interval_days,
            "observations_per_year": observations_per_year,
        }
        features.append(_site_feature(site, props))

    return _dump_collection(
        path, collection_id, features, meta,
        extra={"data_density_method": DATA_DENSITY_METHOD_DESCRIPTION},
    )


WATERLEVEL_CHANGE_METHOD_TEMPLATE = (
    "Depth-to-water change over the most recent {window} years of each well's "
    "record. Observations are downsampled to one point per calendar day (daily "
    "MINIMUM depth-to-water, the shallowest reading). The end point is the latest "
    "daily value; the start point is the daily value closest to {window} years "
    "before it, accepted only when within half that window of the target "
    "(else status='insufficient'). change_ft = end - start. A POSITIVE change "
    "means depth-to-water INCREASED (water table DECLINED); negative means the "
    "water table ROSE."
)


def dump_waterlevel_change_collection(
    path: str,
    site_records: list,
    timeseries_records: list,
    meta: dict,
    *,
    window_years: float,
    reducer: str = "min",
) -> dict:
    """
    Write an OGC FeatureCollection of per-well depth-to-water change over a
    trailing window, one Feature per well. *site_records* and
    *timeseries_records* are index-aligned payload dicts.

    Per well: ``window_years`` (requested), ``actual_window_years``,
    ``dtw_start``/``dtw_end`` (ft), ``start_date``/``end_date``, ``change_ft``
    (end - start; positive = water table declined), ``direction``
    (declining/rising/stable), ``n_observations_in_window`` (distinct days from
    start to end), ``observation_count`` (raw), and ``status`` (ok/insufficient).
    The collection carries ``change_method``.
    """
    collection_id = meta.get("id", "collection")
    target_span = window_years * _SECONDS_PER_YEAR
    tolerance = 0.5 * target_span

    features = []
    for site, obs_list in zip(site_records, timeseries_records):
        observation_count, pairs = _daily_series(obs_list, reducer)

        status = "insufficient"
        start_e = end_e = None
        dtw_start = dtw_end = change_ft = None
        actual_window_years = direction = None
        n_in_window = 0

        if len(pairs) >= 2:
            end_e, dtw_end = pairs[-1]
            dtw_end = round(dtw_end, 2)
            target = end_e - target_span
            # Closest daily point to the window-start target, excluding the end.
            cand_epoch, cand_val = min(
                pairs[:-1], key=lambda p: abs(p[0] - target)
            )
            if abs(cand_epoch - target) <= tolerance:
                start_e, dtw_start = cand_epoch, round(cand_val, 2)
                change_ft = round(dtw_end - dtw_start, 2)
                actual_window_years = round(
                    (end_e - start_e) / _SECONDS_PER_YEAR, 3
                )
                n_in_window = sum(1 for p in pairs if start_e <= p[0] <= end_e)
                direction = (
                    "declining" if change_ft > 0
                    else "rising" if change_ft < 0 else "stable"
                )
                status = "ok"

        props = {
            "source": site.get("source") or "",
            "id": site.get("id") or "",
            "name": site.get("name"),
            "parameter_name": "waterlevels",
            "well_depth": site.get("well_depth"),
            "well_depth_units": site.get("well_depth_units"),
            "window_years": window_years,
            "actual_window_years": actual_window_years,
            "dtw_start": dtw_start,
            "dtw_end": dtw_end,
            "start_date": _iso_utc(start_e),
            "end_date": _iso_utc(end_e),
            "change_ft": change_ft,
            "change_units": "ft",
            "direction": direction,
            "n_observations_in_window": n_in_window,
            "observation_count": observation_count,
            "status": status,
        }
        features.append(_site_feature(site, props))

    return _dump_collection(
        path, collection_id, features, meta,
        extra={
            "change_method": WATERLEVEL_CHANGE_METHOD_TEMPLATE.format(
                window=window_years
            )
        },
    )


WATERLEVEL_STATUS_METHOD_DESCRIPTION = (
    "Current water level relative to each well's own record. Observations are "
    "downsampled to one point per calendar day (daily MINIMUM depth-to-water). "
    "dtw_percentile is the midrank percentile of the latest daily depth-to-water "
    "within the well's full daily record (100 = deepest ever). Status describes "
    "the WATER LEVEL (inverse of depth): much below normal (dtw_percentile >= "
    "90), below normal (>= 75), normal (25-75), above normal (<= 25), much above "
    "normal (<= 10); 'insufficient' when the well has fewer than 10 daily "
    "points. Check last_observation_datetime — a stale 'current' value reflects "
    "the well's state at that time, not today."
)

_STATUS_MIN_RECORDS = 10


def _waterlevel_status(dtw_percentile: float) -> str:
    if dtw_percentile >= 90:
        return "much below normal"
    if dtw_percentile >= 75:
        return "below normal"
    if dtw_percentile <= 10:
        return "much above normal"
    if dtw_percentile <= 25:
        return "above normal"
    return "normal"


def dump_waterlevel_status_collection(
    path: str,
    site_records: list,
    timeseries_records: list,
    meta: dict,
) -> dict:
    """
    Write an OGC FeatureCollection of per-well current water-level status, one
    Feature per well. *site_records* and *timeseries_records* are index-aligned
    payload dicts.

    Per well: ``latest_dtw`` + ``latest_dtw_date``, the record's ``min_dtw``/
    ``median_dtw``/``max_dtw``, ``dtw_percentile`` (midrank percentile of the
    latest daily value within the well's own record), ``status`` (USGS-style
    much above/above/normal/below/much below normal, describing the water
    level), ``record_count``, ``observation_count``, ``span_years``, and
    first/last observation datetimes. The collection carries ``status_method``.
    """
    collection_id = meta.get("id", "collection")

    features = []
    for site, obs_list in zip(site_records, timeseries_records):
        observation_count, pairs = _daily_series(obs_list, "min")
        record_count = len(pairs)

        latest_e = latest_v = None
        min_v = median_v = max_v = None
        dtw_percentile = None
        span_years = 0.0
        status = "insufficient"

        if record_count:
            latest_e, latest_v = pairs[-1]
            span_years = (latest_e - pairs[0][0]) / _SECONDS_PER_YEAR
            values = sorted(v for _, v in pairs)
            min_v, max_v = values[0], values[-1]
            mid = record_count // 2
            median_v = (
                values[mid] if record_count % 2
                else (values[mid - 1] + values[mid]) / 2
            )
            if record_count >= _STATUS_MIN_RECORDS:
                less = sum(1 for v in values if v < latest_v)
                equal = sum(1 for v in values if v == latest_v)
                dtw_percentile = round(
                    100 * (less + 0.5 * equal) / record_count, 1
                )
                status = _waterlevel_status(dtw_percentile)

        props = {
            "source": site.get("source") or "",
            "id": site.get("id") or "",
            "name": site.get("name"),
            "parameter_name": "waterlevels",
            "well_depth": site.get("well_depth"),
            "well_depth_units": site.get("well_depth_units"),
            "record_count": record_count,
            "observation_count": observation_count,
            "first_observation_datetime": _iso_utc(pairs[0][0]) if record_count else None,
            "last_observation_datetime": _iso_utc(latest_e),
            "span_years": round(span_years, 3),
            "latest_dtw": None if latest_v is None else round(latest_v, 2),
            "latest_dtw_date": _iso_utc(latest_e),
            "min_dtw": None if min_v is None else round(min_v, 2),
            "median_dtw": None if median_v is None else round(median_v, 2),
            "max_dtw": None if max_v is None else round(max_v, 2),
            "dtw_units": "ft",
            "dtw_percentile": dtw_percentile,
            "status": status,
        }
        features.append(_site_feature(site, props))

    return _dump_collection(
        path, collection_id, features, meta,
        extra={"status_method": WATERLEVEL_STATUS_METHOD_DESCRIPTION},
    )


SEASONAL_AMPLITUDE_METHOD_TEMPLATE = (
    "Within-year depth-to-water spread per well. Observations are downsampled "
    "to one point per calendar day (daily MINIMUM depth-to-water) and grouped "
    "by calendar year; a year qualifies when it has at least {min_days} daily "
    "points, and its amplitude is that year's max minus min depth-to-water. "
    "mean/max/min_amplitude_ft summarize the qualifying years. Large amplitudes "
    "indicate pumping-stressed or strongly seasonal wells; 'insufficient' when "
    "no year qualifies."
)


def dump_seasonal_amplitude_collection(
    path: str,
    site_records: list,
    timeseries_records: list,
    meta: dict,
    *,
    min_days_per_year: int = 4,
) -> dict:
    """
    Write an OGC FeatureCollection of per-well seasonal water-level amplitude,
    one Feature per well. *site_records* and *timeseries_records* are
    index-aligned payload dicts.

    Per well: ``mean_amplitude_ft``, ``max_amplitude_ft`` (+ ``max_amplitude_year``),
    ``min_amplitude_ft``, ``n_years_used`` (years with >= *min_days_per_year*
    daily points), ``n_years_with_data``, ``record_count``, ``observation_count``,
    and ``status`` (ok/insufficient). The collection carries
    ``seasonal_amplitude_method``.
    """
    collection_id = meta.get("id", "collection")

    features = []
    for site, obs_list in zip(site_records, timeseries_records):
        observation_count, pairs = _daily_series(obs_list, "min")

        by_year: dict = {}
        for epoch, value in pairs:
            year = datetime.fromtimestamp(epoch, tz=timezone.utc).year
            by_year.setdefault(year, []).append(value)

        amplitudes = {
            year: max(vals) - min(vals)
            for year, vals in by_year.items()
            if len(vals) >= min_days_per_year
        }

        if amplitudes:
            max_year = max(amplitudes, key=lambda y: amplitudes[y])
            mean_amp = sum(amplitudes.values()) / len(amplitudes)
            props_amp = {
                "mean_amplitude_ft": round(mean_amp, 2),
                "max_amplitude_ft": round(amplitudes[max_year], 2),
                "max_amplitude_year": max_year,
                "min_amplitude_ft": round(min(amplitudes.values()), 2),
                "status": "ok",
            }
        else:
            props_amp = {
                "mean_amplitude_ft": None,
                "max_amplitude_ft": None,
                "max_amplitude_year": None,
                "min_amplitude_ft": None,
                "status": "insufficient",
            }

        props = {
            "source": site.get("source") or "",
            "id": site.get("id") or "",
            "name": site.get("name"),
            "parameter_name": "waterlevels",
            "well_depth": site.get("well_depth"),
            "well_depth_units": site.get("well_depth_units"),
            "record_count": len(pairs),
            "observation_count": observation_count,
            "n_years_with_data": len(by_year),
            "n_years_used": len(amplitudes),
            "amplitude_units": "ft",
            **props_amp,
        }
        features.append(_site_feature(site, props))

    return _dump_collection(
        path, collection_id, features, meta,
        extra={
            "seasonal_amplitude_method": SEASONAL_AMPLITUDE_METHOD_TEMPLATE.format(
                min_days=min_days_per_year
            )
        },
    )


DEPLETION_PROJECTION_METHOD_DESCRIPTION = (
    "Screening-level projection of when depth-to-water would reach the well's "
    "total depth if the current trend continued. The per-well trend is the "
    "Theil-Sen slope of the daily-minimum depth-to-water series (same "
    "qualification gate as the trend products); a projection is made only when "
    "the Mann-Kendall test finds a significant INCREASING trend (declining "
    "water table) and the well has a reported depth. years_to_depletion = "
    "(well_depth - latest depth-to-water) / slope, from the latest observation "
    "date. This is a linear extrapolation for screening/prioritization, NOT a "
    "prediction: it ignores aquifer thickness, screen interval, pumping "
    "changes, and recharge. Status: 'projected' (projection made), 'not "
    "declining' (no significant declining trend), 'no well depth', 'dtw exceeds "
    "well depth' (latest reading at/below the well bottom — suspect data or a "
    "dry well), 'not enough data'."
)


def dump_depletion_projection_collection(
    path: str,
    site_records: list,
    timeseries_records: list,
    meta: dict,
) -> dict:
    """
    Write an OGC FeatureCollection of per-well depletion projections, one
    Feature per well. *site_records* and *timeseries_records* are index-aligned
    payload dicts.

    Per well: ``slope_ft_per_year`` + ``trend_category`` (Mann-Kendall /
    Theil-Sen on the daily-min series), ``latest_dtw`` + ``latest_dtw_date``,
    ``well_depth``, ``remaining_ft`` (well_depth - latest_dtw),
    ``years_to_depletion``, ``projected_depletion_year``, and ``status`` (see
    method description). The collection carries ``depletion_method``.
    """
    collection_id = meta.get("id", "collection")

    features = []
    for site, obs_list in zip(site_records, timeseries_records):
        observation_count, pairs = _daily_series(obs_list, "min")
        record_count = len(pairs)
        span_years = (
            (pairs[-1][0] - pairs[0][0]) / _SECONDS_PER_YEAR
            if record_count >= 2 else 0.0
        )

        latest_e = latest_v = None
        slope = trend_category = None
        well_depth = _num(site.get("well_depth"))
        remaining = years_to_depletion = projected_year = None

        if record_count:
            latest_e, latest_v = pairs[-1]

        if not _qualifies_for_trend(record_count, span_years):
            status = "not enough data"
        else:
            years = [p[0] / _SECONDS_PER_YEAR for p in pairs]
            values = [p[1] for p in pairs]
            trend_category, slope, _, _ = _mann_kendall_trend(years, values)
            slope = round(slope, 2)
            if trend_category != "increasing" or slope <= 0:
                status = "not declining"
            elif well_depth is None:
                status = "no well depth"
            else:
                remaining = round(well_depth - latest_v, 2)
                if remaining <= 0:
                    status = "dtw exceeds well depth"
                else:
                    years_to_depletion = round(remaining / slope, 1)
                    latest_year = datetime.fromtimestamp(
                        latest_e, tz=timezone.utc
                    ).year
                    projected_year = int(latest_year + years_to_depletion)
                    status = "projected"

        props = {
            "source": site.get("source") or "",
            "id": site.get("id") or "",
            "name": site.get("name"),
            "parameter_name": "waterlevels",
            "well_depth": well_depth,
            "well_depth_units": site.get("well_depth_units"),
            "record_count": record_count,
            "observation_count": observation_count,
            "span_years": round(span_years, 3),
            "trend_category": trend_category,
            "slope_ft_per_year": slope,
            "latest_dtw": None if latest_v is None else round(latest_v, 2),
            "latest_dtw_date": _iso_utc(latest_e),
            "remaining_ft": remaining,
            "years_to_depletion": years_to_depletion,
            "projected_depletion_year": projected_year,
            "status": status,
        }
        features.append(_site_feature(site, props))

    return _dump_collection(
        path, collection_id, features, meta,
        extra={"depletion_method": DEPLETION_PROJECTION_METHOD_DESCRIPTION},
    )


WQI_METHOD_DESCRIPTION = (
    "CCME Water Quality Index computed from each well's latest value per "
    "analyte against the drinking-water MCL thresholds (same source of truth "
    "as the MCL exceedance product). F1 (scope) = % of tested analytes "
    "exceeding; with one (latest) value per analyte F2 (frequency) equals F1. "
    "F3 (amplitude) is from the normalized sum of excursions (value/MCL - 1). "
    "WQI = 100 - sqrt(F1^2 + F2^2 + F3^2)/1.732, clamped to [0, 100]. Class "
    "(CCME): excellent (>= 95), good (>= 80), fair (>= 65), marginal (>= 45), "
    "poor (< 45); 'insufficient' when no tested analyte has an MCL. Scores from "
    "few analytes are weak evidence — filter on n_analytes_tested."
)


def _wqi_class(wqi: Optional[float]) -> str:
    if wqi is None:
        return "insufficient"
    if wqi >= 95:
        return "excellent"
    if wqi >= 80:
        return "good"
    if wqi >= 65:
        return "fair"
    if wqi >= 45:
        return "marginal"
    return "poor"


def dump_wqi_collection(
    path: str, records: list, meta: dict, thresholds: dict
) -> dict:
    """
    Write an OGC FeatureCollection of per-well CCME water quality index, one
    Feature per well. *records* is a flat list of SummaryRecord (one per
    well+analyte), pivoted by well. *thresholds* maps analyte ->
    {"mcl": <number>, "type": ...} (same file as the MCL exceedance product);
    only analytes with an MCL are scored.

    Per well: ``<analyte>`` + ``<analyte>_date`` for each tested analyte,
    ``wqi`` (0-100), ``wqi_class``, ``n_analytes_tested``, ``n_exceeding``, and
    ``exceeded_analytes``. The collection carries ``wqi_method`` and
    ``mcl_thresholds``.
    """
    collection_id = meta.get("id", "collection")
    wells = _pivot_by_well(records)

    features = []
    for (source, rid), well in wells.items():
        props = {
            "source": source,
            "id": rid,
            "name": well["name"],
            "well_depth": well["well_depth"],
            "well_depth_units": well["well_depth_units"],
        }

        tests = []  # (analyte, value, mcl)
        for analyte, entry in well["analytes"].items():
            value = _num(entry.get("value"))
            limit = thresholds.get(analyte) or {}
            mcl = _num(limit.get("mcl"))
            if value is None or mcl is None or mcl <= 0:
                continue
            props[analyte] = value
            props[f"{analyte}_date"] = entry.get("date")
            tests.append((analyte, value, mcl))

        if not tests:
            wqi = None
            exceeded = []
        else:
            exceeded = sorted(a for a, v, m in tests if v > m)
            f1 = 100 * len(exceeded) / len(tests)
            f2 = f1  # one (latest) value per analyte, so frequency == scope
            excursions = sum(v / m - 1 for a, v, m in tests if v > m)
            nse = excursions / len(tests)
            f3 = nse / (0.01 * nse + 0.01)
            wqi = 100 - math.sqrt(f1 * f1 + f2 * f2 + f3 * f3) / 1.732
            wqi = round(min(100.0, max(0.0, wqi)), 1)

        props.update({
            "wqi": wqi,
            "wqi_class": _wqi_class(wqi),
            "n_analytes_tested": len(tests),
            "n_exceeding": len(exceeded),
            "exceeded_analytes": exceeded,
        })
        features.append(_well_feature(source, rid, well, props))

    return _dump_collection(
        path, collection_id, features, meta,
        extra={"wqi_method": WQI_METHOD_DESCRIPTION, "mcl_thresholds": thresholds},
    )
