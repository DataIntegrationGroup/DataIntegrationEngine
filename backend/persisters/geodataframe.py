# ===============================================================================
# Copyright 2024 Jake Ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
"""GeoPandas-backed persistence (see docs/framework-migration-plan.md, Phase A).

GeoPandas is the serialization engine for the product outputs. Every
``dump_*_collection`` in ``ogc_features`` routes its features through a
``GeoDataFrame`` — see :func:`route_feature_dicts_through_gdf`, called from
``_dump_collection`` — so the OGC GeoJSON is emitted by GeoPandas rather than
hand-assembled, and the same in-memory object can also write GeoPackage /
PostGIS / GeoParquet.

The OGC FeatureCollection *envelope* (top-level id/title/timeStamp/links/
numberReturned and product-level extras) is a product concern that stays in
``ogc_features._dump_collection``; this module only owns the per-feature geometry
+ property serialization and the GeoParquet inter-asset handoff.
"""

import io
import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, shape

from backend.constants import TDS
from backend.persisters.ogc_features import _feature_id, _num, _tds_class

# Property columns are the record's own keys minus the three that become
# geometry. Kept here so the column order in the GeoDataFrame (and therefore the
# GeoJSON `properties` order) matches the legacy feature output exactly.
_GEOMETRY_KEYS = ("latitude", "longitude", "elevation")


def _point(lat, lon, elev):
    """Mirror ogc_features._point_geometry: 2D Point unless an elevation is
    present, and no geometry at all when coordinates are missing (GeoPandas
    emits ``"geometry": null`` for those rows)."""
    if lat is None or lon is None:
        return None
    if elev is None:
        return Point(lon, lat)
    return Point(lon, lat, elev)


def features_to_geodataframe(items) -> gpd.GeoDataFrame:
    """Build a GeoDataFrame from an iterable of ``(feature_id, geometry, props)``
    where *geometry* is a shapely geometry or ``None`` and *props* is a dict.

    The canonical persistence object. Property columns are held as ``object``
    dtype so ``to_json`` preserves exact int/float/None types, and the feature id
    becomes the index (→ GeoJSON feature ``id`` via ``to_json(drop_id=False)``).

    Note: a GeoDataFrame has a *uniform* column set, so a product whose features
    carry ragged property keys (e.g. per-well analyte pivots) gains explicit null
    columns here — a deliberate schema change. Uniform-schema products round-trip
    byte-identically.
    """
    ids: list = []
    geoms: list = []
    rows: list[dict] = []
    for feature_id, geom, props in items:
        ids.append(feature_id)
        geoms.append(geom)
        rows.append(props)
    frame = pd.DataFrame(rows, dtype=object)
    gdf = gpd.GeoDataFrame(frame, geometry=geoms, crs="EPSG:4326")
    gdf.index = ids
    return gdf


def records_to_geodataframe(records: list) -> gpd.GeoDataFrame:
    """Build the canonical persistence GeoDataFrame from Summary/Site records.

    - geometry: per-row Point (2D or 3D) in EPSG:4326, matching the legacy
      coordinate rule (elevation only when present).
    - columns: each record's ``keys`` except latitude/longitude/elevation, in
      order, plus ``tds_class`` for TDS records.
    - index: the OGC feature id ``"source:id"`` so ``to_json(drop_id=False)``
      emits it as the feature-level ``id``.

    Used for the GeoParquet inter-asset handoff (a source asset's summary/sites
    become a GeoDataFrame). Columns are held as ``object`` dtype so pandas does
    not coerce ints to floats or ``None`` to ``NaN`` — the emitted GeoJSON then
    preserves the exact value types the legacy hand-built features carried.
    """
    rows: list[dict] = []
    geoms: list = []
    ids: list[str] = []

    for r in records:
        props = {k: getattr(r, k) for k in r.keys if k not in _GEOMETRY_KEYS}
        if getattr(r, "parameter_name", None) == TDS:
            props["tds_class"] = _tds_class(_num(getattr(r, "latest_value", None)))
        rows.append(props)
        geoms.append(
            _point(
                getattr(r, "latitude", None),
                getattr(r, "longitude", None),
                getattr(r, "elevation", None),
            )
        )
        ids.append(_feature_id(getattr(r, "source", "") or "", getattr(r, "id", "") or ""))

    # object dtype preserves int/float/None exactly through to_json (see docstring).
    frame = pd.DataFrame(rows, dtype=object)
    gdf = gpd.GeoDataFrame(frame, geometry=geoms, crs="EPSG:4326")
    gdf.index = ids
    return gdf


def geodataframe_to_features(gdf: gpd.GeoDataFrame) -> list[dict]:
    """Serialize a GeoDataFrame to a list of GeoJSON Feature dicts via GeoPandas.

    ``drop_id=False`` promotes the index to each feature's top-level ``id``;
    ``na="null"`` emits missing values as JSON null (matching the legacy
    behavior of carrying ``None`` straight through)."""
    if gdf.empty:
        return []
    return json.loads(gdf.to_json(drop_id=False, na="null"))["features"]


def route_feature_dicts_through_gdf(features: list) -> list:
    """Rebuild a list of GeoJSON feature dicts by round-tripping them through a
    GeoDataFrame — the single hook that makes **every** ``dump_*`` in
    ``ogc_features`` GeoDataFrame-backed (called from ``_dump_collection``).

    Geometry dicts are reconstructed with ``shapely.geometry.shape`` (point or
    polygon; ``None`` stays null). Because a GeoDataFrame has a uniform column
    set, products whose features carry ragged property keys gain explicit null
    columns (the chosen schema); uniform products round-trip byte-identically.
    Idempotent: features already emitted from a GeoDataFrame pass through
    unchanged.
    """
    if not features:
        return features
    items = []
    for f in features:
        geom_dict = f.get("geometry")
        geom = shape(geom_dict) if geom_dict else None
        items.append((f.get("id"), geom, f.get("properties", {})))
    gdf = features_to_geodataframe(items)
    return geodataframe_to_features(gdf)


def gdf_to_parquet_bytes(gdf: gpd.GeoDataFrame) -> bytes:
    """Serialize a GeoDataFrame to GeoParquet bytes for the Dagster inter-asset
    handoff (replaces the pickled ``_payload`` dicts).

    The feature-id index is preserved (``index=True``) so it survives the
    round-trip and still becomes the OGC feature id downstream. GeoParquet keeps
    an explicit column schema + CRS in its metadata, so types round-trip far more
    faithfully than pickle-of-dicts — with one caveat: a column that mixes ints
    and ``None`` comes back float (Arrow encodes the null as a floating NaN). No
    summary/site column does that (``nrecords`` is always present), but the
    per-product fan-out should keep it in mind.

    Requires the optional ``parquet`` extra (pyarrow); present in the
    orchestration deploy env via the dagster stack.
    """
    buf = io.BytesIO()
    gdf.to_parquet(buf, index=True)
    return buf.getvalue()


def parquet_bytes_to_gdf(data: bytes) -> gpd.GeoDataFrame:
    """Inverse of :func:`gdf_to_parquet_bytes` — read GeoParquet bytes back into a
    GeoDataFrame (geometry + CRS + feature-id index restored)."""
    return gpd.read_parquet(io.BytesIO(data))


# ---------------------------------------------------------------------------
# Source-asset payload handoff (Parquet) — replaces the pickled {records, sites,
# timeseries} dict crossing the Dagster IO manager. records/sites/observations
# are flat scalar payload dicts (SummaryRecord/SiteRecord/ParameterRecord
# _payloads), so a columnar Parquet table is the natural, typed handoff; the
# combine rebuilds record objects from the dicts exactly as before. Geometry is
# built later, in the dumpers — so these are plain Parquet, not GeoParquet.
# ---------------------------------------------------------------------------

# Marks which site-group each flattened observation belongs to, so the aligned
# list-of-per-site-lists (sites[i] ↔ timeseries[i]) survives the round-trip.
_SITE_IDX = "__site_idx"


def _clean_nans(records: list[dict]) -> list[dict]:
    """pandas reads missing cells back as NaN; restore them to None so the
    rebuilt payload dicts match what was written (record classes expect None)."""
    return [{k: (None if pd.isna(v) else v) for k, v in row.items()} for row in records]


def _frame_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialize *df* to Parquet, stringifying any object column that mixes
    numeric values with non-numeric strings.

    pyarrow infers one logical type per column and raises ArrowInvalid if an
    object column holds both — e.g. a WQP analyte's parameter_value column
    carrying floats *and* the non-detect marker 'ND' (numeric results convert to
    float in _apply_unit_conversion, qualitative ones stay strings). Casting such
    a column to str lets the qualitative markers survive the round-trip; every
    downstream consumer already float()-coerces and skips non-numeric values
    (see backend.trend_stats.daily_series)."""
    for col in df.columns:
        vals = [v for v in df[col] if not _is_null(v)]
        has_num = any(isinstance(v, (int, float)) and not isinstance(v, bool) for v in vals)
        has_str = any(isinstance(v, str) for v in vals)
        if has_num and has_str:
            df[col] = [None if _is_null(v) else str(v) for v in df[col]]
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _is_null(v) -> bool:
    """None or a float NaN. Scalar-only — payload columns are flat scalars."""
    return v is None or (isinstance(v, float) and v != v)


def dicts_to_parquet_bytes(dicts: list[dict]) -> bytes:
    """Serialize a flat list of payload dicts (records or sites) to Parquet.
    object dtype keeps exact types; a uniform column set is fine — the record
    classes tolerate extra null-valued keys on rebuild."""
    return _frame_to_parquet_bytes(pd.DataFrame(dicts, dtype=object))


def parquet_bytes_to_dicts(data: bytes) -> list[dict]:
    """Inverse of :func:`dicts_to_parquet_bytes`."""
    df = pd.read_parquet(io.BytesIO(data))
    return _clean_nans(df.to_dict("records"))


def timeseries_to_parquet_bytes(timeseries: list[list[dict]]) -> bytes:
    """Flatten the aligned list-of-per-site-lists to one Parquet table, tagging
    each observation with its site-group index so the grouping is recoverable."""
    rows: list[dict] = []
    for site_idx, site_ts in enumerate(timeseries):
        for obs in site_ts:
            rows.append({**obs, _SITE_IDX: site_idx})
    return _frame_to_parquet_bytes(pd.DataFrame(rows, dtype=object))


def parquet_bytes_to_timeseries(data: bytes) -> list[list[dict]]:
    """Inverse of :func:`timeseries_to_parquet_bytes` — regroup observations back
    into the aligned list-of-per-site-lists by ``__site_idx``. Group order is the
    site-group order (every persisted site carries ≥1 observation, so groups are
    contiguous 0..N-1)."""
    df = pd.read_parquet(io.BytesIO(data))
    if df.empty:
        return []
    n_groups = int(df[_SITE_IDX].max()) + 1
    groups: list[list[dict]] = [[] for _ in range(n_groups)]
    for row in _clean_nans(df.to_dict("records")):
        idx = int(row.pop(_SITE_IDX))
        groups[idx].append(row)
    return groups


def write_geopackage(gdf: gpd.GeoDataFrame, path: str, layer: str) -> tuple:
    """Write a GeoDataFrame to a GeoPackage layer named *layer* and return its
    2D bounds ``(minx, miny, maxx, maxy)`` in EPSG:4326.

    The single GeoPackage writer for the GeoServer publish path. Sets a default
    CRS when absent, and flattens geometry to 2D — GeoServer's GeoPackage reader
    rejects a 3D CRS ("WGS 84 has 3 dimensions") when computing bounds, so
    elevation stays an attribute only. Bounds are returned so the caller can hand
    them to GeoServer explicitly (avoids a getBounds call that trips the same
    3D-CRS bug). Raises on an empty frame (nothing to publish)."""
    if gdf.empty:
        raise ValueError(f"{layer}: GeoDataFrame has no features; nothing to write")
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    flat = gdf.copy()
    flat["geometry"] = flat.geometry.force_2d()
    minx, miny, maxx, maxy = (float(v) for v in flat.total_bounds)
    flat.to_file(path, driver="GPKG", layer=layer)
    return (minx, miny, maxx, maxy)


def geojson_to_geopackage(geojson_path, layer_name: str, out_dir) -> tuple:
    """Convert a GeoJSON file to a GeoPackage whose layer is *layer_name* (so the
    published GeoServer layer takes that name). Returns ``(gpkg_path, bbox)`` with
    ``bbox`` the 2D EPSG:4326 bounds. Reads the GeoJSON with GeoPandas and writes
    the GPKG via :func:`write_geopackage`. Used by the GeoServer publish asset."""
    gdf = gpd.read_file(geojson_path)
    gpkg_path = Path(out_dir) / f"{layer_name}.gpkg"
    bbox = write_geopackage(gdf, str(gpkg_path), layer_name)
    return gpkg_path, bbox
