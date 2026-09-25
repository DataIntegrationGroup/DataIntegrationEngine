"""
Patch pygeoapi's Parquet provider in the image (run once at build time).

Each fix replaces source text that must occur exactly once; otherwise the
script exits non-zero and fails the build, so a base-image bump can't drop a
fix silently. When a fix lands upstream, delete it here.

1. Paging: `_response_feature_collection` slices the batch that crosses
   `limit` to `limit + 1` rows, ignoring the rows already taken from earlier
   batches. A filtered page then holds up to limit + 1 + (earlier batches)
   rows, and pages overlap.
2. Date-only `datetime` bounds (e.g. 2020-01-01/2020-12-31) parse to naive
   datetimes, and comparing them with a tz-aware timestamp column raises (500).
   Treat naive bounds as UTC. generate_config.py only sets time_field for
   Parquet files whose datetime column is tz-aware, so this is always safe here.
3. bbox: responses were built with geopandas ``__geo_interface__``, which adds a
   collection bbox — ``[NaN, NaN, NaN, NaN]`` for an empty result, invalid JSON
   that browsers can't parse — and a bbox on every feature. Build them without
   bboxes, matching the OGR (GeoJSON) collections.

Usage:
    python patch_pygeoapi.py [path/to/pygeoapi/provider/parquet.py]
"""
import sys
from pathlib import Path

FIXES = [
    (
        "paging: count rows already read",
        "batches_list.append(batch.slice(0, limit + 1))",
        "batches_list.append(\n"
        "                        batch.slice(0, limit + 1 - (read - batch.num_rows)))",
    ),
    ("datetime: naive begin is UTC", "begin = isoparse(begin)", "begin = _utc(isoparse(begin))"),
    ("datetime: naive end is UTC", "end = isoparse(end)", "end = _utc(isoparse(end))"),
    (
        "datetime: naive instant is UTC",
        "target_time = isoparse(datetime_)",
        "target_time = _utc(isoparse(datetime_))",
    ),
    (
        "bbox: items response",
        "result = gdf.__geo_interface__",
        "result = gdf.to_geo_dict(na='null', show_bbox=False, drop_id=False)",
    ),
    (
        "bbox: single item",
        "return gdf.__geo_interface__['features'][0]",
        "return gdf.to_geo_dict(\n"
        "                na='null', show_bbox=False, drop_id=False)['features'][0]",
    ),
]

UTC_HELPER = '''

def _utc(value):
    """Patched in by DIE (orchestration/pygeoapi/patch_pygeoapi.py)."""
    from datetime import timezone
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
'''


def patch(source: str) -> str:
    for name, old, new in FIXES:
        count = source.count(old)
        if count != 1:
            raise SystemExit(
                f"patch '{name}': expected 1 occurrence of {old!r}, found {count}. "
                "pygeoapi changed; re-check the fix against the new source."
            )
        source = source.replace(old, new)
    return source + UTC_HELPER


def default_path() -> Path:
    import importlib.util

    # find_spec locates the package without importing the provider module.
    spec = importlib.util.find_spec("pygeoapi")
    return Path(spec.origin).parent / "provider" / "parquet.py"


if __name__ == "__main__":
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_path()
    path.write_text(patch(path.read_text()))
    print(f"Patched {path} ({len(FIXES)} fixes)")
