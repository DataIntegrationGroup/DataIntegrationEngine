# ===============================================================================
# Copyright 2024 Jake Ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
"""GeoPandas-backed persistence.

First cut of the persistence migration (see docs/framework-migration-plan.md,
Phase A): turn the in-memory record objects into a single canonical
``GeoDataFrame``, then let GeoPandas own serialization to every output format
(GeoJSON / GeoPackage / PostGIS / GeoParquet). This replaces the hand-rolled
byte assembly in ``persister.py`` and the per-feature GeoJSON construction in
``ogc_features.py``.

The OGC FeatureCollection *envelope* (top-level id/title/timeStamp/links/
numberReturned and product-level extras such as ``tds_class_method``) is a
product concern that sits *above* GeoPandas, so it stays in
``ogc_features._dump_collection``; GeoPandas only owns the per-feature geometry +
property serialization. ``dump_summary_collection_gpd`` produces output identical
to the legacy ``dump_summary_collection`` (proven by
tests/test_persisters/test_geodataframe.py) while sourcing the features from a
GeoDataFrame — the pattern the rest of the ``dump_*`` functions follow next.
"""

import io
import json

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from backend.constants import TDS
from backend.persisters.ogc_features import (
    TDS_CLASS_METHOD_DESCRIPTION,
    _dump_collection,
    _feature_id,
    _num,
    _tds_class,
)

# Property columns are the record's own keys minus the three that become
# geometry. Kept here so the column order in the GeoDataFrame (and therefore the
# GeoJSON `properties` order) matches the legacy _make_feature output exactly.
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


def records_to_geodataframe(records: list) -> gpd.GeoDataFrame:
    """Build the canonical persistence GeoDataFrame from Summary/Site records.

    - geometry: per-row Point (2D or 3D) in EPSG:4326, matching the legacy
      coordinate rule (elevation only when present).
    - columns: each record's ``keys`` except latitude/longitude/elevation, in
      order, plus ``tds_class`` for TDS records.
    - index: the OGC feature id ``"source:id"`` so ``to_json(drop_id=False)``
      emits it as the feature-level ``id``.

    Columns are held as ``object`` dtype so pandas does not coerce ints to
    floats or ``None`` to ``NaN`` — the GeoJSON emitted by ``to_json`` then
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


def dump_summary_collection_gpd(path: str, records: list, meta: dict) -> dict:
    """GeoPandas-backed replacement for ogc_features.dump_summary_collection.

    Features are produced from the canonical GeoDataFrame; the OGC envelope is
    still added by ``_dump_collection`` so output is identical to the legacy
    dumper. Returns the collection dict (for testing)."""
    collection_id = meta.get("id", "collection")
    gdf = records_to_geodataframe(records)
    features = geodataframe_to_features(gdf)
    extra = None
    if any(getattr(r, "parameter_name", None) == TDS for r in records):
        extra = {"tds_class_method": TDS_CLASS_METHOD_DESCRIPTION}
    return _dump_collection(path, collection_id, features, meta, extra=extra)


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

    Demonstrates the payoff of the migration: the same in-memory object that
    produces the product GeoJSON also writes GPKG (for GeoServer) with one call,
    replacing the GeoJSON→GeoPackage round-trip in the geoserver asset. Geometry
    is flattened to 2D for GeoServer's GPKG reader (same reason as
    orchestration/assets/products._geojson_to_geopackage)."""
    flat = gdf.copy()
    flat["geometry"] = flat.geometry.force_2d()
    flat.to_file(path, driver="GPKG", layer=layer)
