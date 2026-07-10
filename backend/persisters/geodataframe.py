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


def write_geopackage(gdf: gpd.GeoDataFrame, path: str, layer: str) -> None:
    """Write the canonical GeoDataFrame straight to a GeoPackage layer.

    The payoff of the migration: the same in-memory object that produces the
    product GeoJSON also writes GPKG (for GeoServer) with one call, replacing the
    GeoJSON→GeoPackage round-trip in the geoserver asset. Geometry is flattened to
    2D for GeoServer's GPKG reader (same reason as
    orchestration/assets/products._geojson_to_geopackage)."""
    flat = gdf.copy()
    flat["geometry"] = flat.geometry.force_2d()
    flat.to_file(path, driver="GPKG", layer=layer)
