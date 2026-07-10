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
from shapely.geometry import Point, shape

from backend.constants import TDS
from backend.persisters.ogc_features import (
    HARDNESS_METHOD_DESCRIPTION,
    TDS_CLASS_METHOD_DESCRIPTION,
    WELL_DENSITY_METHOD_DESCRIPTION,
    _HARDNESS_CA_FACTOR,
    _HARDNESS_MG_FACTOR,
    _assign_points_to_regions,
    _dedupe_well_points,
    _dump_collection,
    _feature_id,
    _hardness_class,
    _num,
    _pivot_by_well,
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


def point_geometry(lat, lon, elev):
    """Public alias for the geometry rule (2D unless elevation present, None when
    coordinates missing) so per-product dumpers build geometry the same way."""
    return _point(lat, lon, elev)


def features_to_geodataframe(items) -> gpd.GeoDataFrame:
    """Build a GeoDataFrame from an iterable of ``(feature_id, geometry, props)``
    where *geometry* is a shapely geometry or ``None`` and *props* is a dict.

    The generic path every ``dump_*`` uses: it turns a product's computed
    features into the canonical persistence object. Property columns are held as
    ``object`` dtype so ``to_json`` preserves exact int/float/None types, and the
    feature id becomes the index (→ GeoJSON feature ``id`` via
    ``to_json(drop_id=False)``).

    Note: a GeoDataFrame has a *uniform* column set, so a product whose features
    carry ragged property keys (e.g. per-well analyte pivots) will gain explicit
    null columns here — a deliberate schema change, not byte-parity. Products
    with a uniform feature schema (summary, timeseries, density, …) round-trip
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


def _timeseries_items(site_records, timeseries_records, site_lookup):
    """Yield ``(feature_id, geometry, props)`` per observation — the same id /
    geometry / datetime rules as ogc_features.dump_timeseries_collection."""
    if site_lookup is None:
        site_lookup = {}
        for sr in site_records:
            key = getattr(sr, "id", None)
            if key:
                site_lookup[key] = sr

    for obs in timeseries_records:
        site_id = getattr(obs, "id", None)
        site = site_lookup.get(site_id)
        if site:
            geom = _point(
                getattr(site, "latitude", None),
                getattr(site, "longitude", None),
                getattr(site, "elevation", None),
            )
        else:
            geom = None

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
        yield feature_id, geom, props


def dump_timeseries_collection_gpd(
    path: str,
    site_records: list,
    timeseries_records: list,
    meta: dict,
    site_lookup=None,
) -> dict:
    """GeoPandas-backed replacement for ogc_features.dump_timeseries_collection.

    One flat feature per observation, features sourced from a GeoDataFrame; OGC
    envelope still from ``_dump_collection``. Observation properties are a fixed
    key set (ParameterRecord.keys + datetime), so output is byte-identical to the
    legacy dumper."""
    collection_id = meta.get("id", "collection")
    items = list(_timeseries_items(site_records, timeseries_records, site_lookup))
    gdf = features_to_geodataframe(items)
    features = geodataframe_to_features(gdf)
    return _dump_collection(path, collection_id, features, meta)


def route_feature_dicts_through_gdf(features: list) -> list:
    """Rebuild a list of GeoJSON feature dicts by round-tripping them through a
    GeoDataFrame — the single hook that makes **every** ``dump_*`` in
    ``ogc_features`` GeoDataFrame-backed (called from ``_dump_collection``).

    Geometry dicts are reconstructed with ``shapely.geometry.shape`` (point or
    polygon; ``None`` stays null). Because a GeoDataFrame has a uniform column
    set, products whose features carry ragged property keys gain explicit null
    columns (the chosen schema); uniform products round-trip byte-identically.
    Idempotent: features already emitted from a GeoDataFrame pass through
    unchanged, so item-based dumpers that hit this a second time are unaffected.
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


def dump_collection_from_items(path, collection_id, items, meta, extra=None) -> dict:
    """Serialize precomputed ``(feature_id, geometry, props)`` items to an OGC
    FeatureCollection through a GeoDataFrame. The single serialization path every
    ``dump_*_gpd`` shares: build the GDF, emit GeoJSON features, wrap the OGC
    envelope. *geometry* may be point or polygon (shapely) or None."""
    gdf = features_to_geodataframe(list(items))
    features = geodataframe_to_features(gdf)
    return _dump_collection(path, collection_id, features, meta, extra=extra)


def dump_hardness_collection_gpd(path: str, records: list, meta: dict) -> dict:
    """GeoPandas-backed dump_hardness_collection. One feature per well with a
    fixed property set (calcium/magnesium + hardness), so byte-identical to the
    legacy dumper. Reuses the legacy pivot + hardness helpers so only the
    serialization differs."""
    wells = _pivot_by_well(records)
    items = []
    for (source, rid), well in wells.items():
        analytes = well["analytes"]
        ca = _num(analytes.get("calcium", {}).get("value"))
        mg = _num(analytes.get("magnesium", {}).get("value"))
        hardness = (
            None
            if ca is None or mg is None
            else round(_HARDNESS_CA_FACTOR * ca + _HARDNESS_MG_FACTOR * mg, 1)
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
        geom = _point(well["latitude"], well["longitude"], well["elevation"])
        items.append((_feature_id(source, rid), geom, props))
    return dump_collection_from_items(
        path, meta.get("id", "collection"), items, meta,
        extra={"hardness_method": HARDNESS_METHOD_DESCRIPTION},
    )


def dump_major_chemistry_collection_gpd(path: str, records: list, meta: dict) -> dict:
    """GeoPandas-backed dump_major_chemistry_collection. One feature per well; the
    per-analyte properties are **ragged** in the legacy output, so routing through
    a GeoDataFrame gives every well a *uniform* column set (null for analytes it
    lacks) — the chosen schema, not byte-parity (needed for GPKG/PostGIS)."""
    wells = _pivot_by_well(records)
    items = []
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
        geom = _point(well["latitude"], well["longitude"], well["elevation"])
        items.append((_feature_id(source, rid), geom, props))
    return dump_collection_from_items(path, meta.get("id", "collection"), items, meta)


def dump_well_density_collection_gpd(
    path: str, counties: list, site_records: list, meta: dict
) -> dict:
    """GeoPandas-backed dump_well_density_collection. One feature per county
    **polygon**; uniform props → byte-identical to the legacy dumper. Reuses the
    legacy point-dedupe + region-assignment helpers."""
    points = _dedupe_well_points(site_records)
    counts, unassigned = _assign_points_to_regions(counties, points)
    items = []
    for county, well_count in zip(counties, counts):
        area = county["area_sq_km"]
        props = {
            "county": county["name"],
            "fips": county["fips"],
            "area_sq_km": area,
            "well_count": well_count,
            "wells_per_sq_km": round(well_count / area, 4) if area else None,
        }
        fid = f"county:{county['fips'] or county['name']}"
        items.append((fid, county["geometry"], props))
    return dump_collection_from_items(
        path, meta.get("id", "collection"), items, meta,
        extra={
            "well_density_method": WELL_DENSITY_METHOD_DESCRIPTION,
            "unassigned_well_count": unassigned,
        },
    )


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
