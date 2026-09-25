"""
Patch pygeoapi's Parquet provider at build time. Each target must occur exactly
once, or the build fails. Delete a fix once it lands upstream.

1. Paging: the batch crossing `limit` was sliced to limit + 1 rows, ignoring
   rows already read, so pages were too long and overlapped.
2. Date-only `datetime` bounds were naive and raised against the tz-aware
   column (500). Treat them as UTC; time_field is only set for tz-aware files.
3. bbox: `__geo_interface__` added bboxes, [NaN, NaN, NaN, NaN] (invalid JSON)
   when empty. Omit them.
4. numberMatched: report the filtered row count, not offset + rows returned.

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
    (
        "numberMatched: true total",
        "number_matched = offset + len(rp)",
        "number_matched = scanner.count_rows()",
    ),
]

UTC_HELPER = '''

def _utc(value):
    """Added by DIE's patch_pygeoapi.py: naive datetimes are UTC."""
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
