# ===============================================================================
# Trend statistics for DIE products.
#
# Pure analysis (no serialization / I/O): daily aggregation, the qualification
# gate, and the Mann-Kendall + Theil-Sen trend test. Kept separate from
# backend/persisters/ogc_features.py (which only builds GeoJSON) because this is
# statistics, is independently testable, and pulls heavier deps (scipy,
# pymannkendall) lazily.
# ===============================================================================
from datetime import datetime, timezone
from typing import Optional

# Seconds per Julian year (365.25 days) — used to express the Theil-Sen slope
# per year.
_SECONDS_PER_YEAR = 31557600.0

# A well is classified only when it has enough data for a meaningful
# Mann-Kendall test: at least 10 daily points, or at least 4 spanning >= 2 years.
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


def parse_epoch_seconds(date, time) -> Optional[float]:
    """Best-effort parse of a DIE date (+optional time) to POSIX seconds (UTC)."""
    if not date:
        return None
    text = f"{date}T{time}" if time else str(date)
    text = text.replace("Z", "")
    try:
        dt = datetime.fromisoformat(text)
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


def daily_series(obs_list: list, reducer: str = "min") -> tuple[int, list]:
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
        epoch = parse_epoch_seconds(obs.get("date_measured"), obs.get("time_measured"))
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


def qualifies_for_trend(record_count, span_years) -> bool:
    return record_count >= _TREND_MIN_RECORDS or (
        record_count >= _TREND_MIN_RECORDS_WITH_SPAN
        and span_years >= _TREND_MIN_SPAN_YEARS
    )


def mann_kendall_trend(years: list, values: list):
    """Run the Mann-Kendall trend test + Theil-Sen slope.

    Returns (trend_category, slope_per_year, p_value, tau). *years* are decimal
    years, *values* the measured quantity, both ordered by time. trend_category
    is one of 'increasing' / 'decreasing' / 'stable'.
    """
    import pymannkendall as mk
    from scipy.stats import theilslopes

    result = mk.original_test(values, alpha=_TREND_ALPHA)
    # Time-aware Theil-Sen slope (per year) — robust and correct for the
    # irregular sampling typical of these records, unlike MK's index-based slope
    # which assumes unit spacing.
    slope_per_year = float(theilslopes(values, years)[0])

    # mk trend is 'increasing' / 'decreasing' / 'no trend'.
    category = "stable" if result.trend == "no trend" else result.trend
    return category, slope_per_year, float(result.p), float(result.Tau)
