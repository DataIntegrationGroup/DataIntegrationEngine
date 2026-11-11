# ===============================================================================
# Trend statistics for DIE products.
#
# Pure analysis (no serialization / I/O): daily/monthly aggregation, trailing
# windows, the qualification gate, and the Mann-Kendall + Theil-Sen trend test.
# Kept separate from backend/persisters/ogc_features.py (which only builds
# GeoJSON) because this is statistics, is independently testable, and pulls
# heavier deps (scipy, pymannkendall) lazily.
# ===============================================================================
from datetime import datetime, timezone
from statistics import median
from typing import Optional

# Seconds per Julian year (365.25 days) — used to express the Theil-Sen slope
# per year.
_SECONDS_PER_YEAR = 31557600.0

# A well is classified only when it has enough data for a meaningful
# Mann-Kendall test: at least 10 points, or at least 4 spanning >= 2 years.
# "Points" are the monthly median values, not raw readings.
# For the fixed-window layers the rule applies to the points inside the window.
_TREND_MIN_RECORDS = 10
_TREND_MIN_RECORDS_WITH_SPAN = 4
_TREND_MIN_SPAN_YEARS = 2.0
_TREND_ALPHA = 0.05  # significance level for the Mann-Kendall test

# Value of the trend_period property for the trend over each well's whole record.
FULL_RECORD_PERIOD = "full record"


# Every trend reduces each well to one value per calendar month (UTC) before
# testing: the MEDIAN for depth-to-water, the MEAN for analyte concentrations.
# This thins dense logger data so near-duplicate readings aren't counted as
# independent evidence. It does NOT remove seasonality: original_test assumes
# there is no seasonal pattern.
_TREND_BUCKET = "month"
_WATERLEVEL_TREND_REDUCER = "median"
_ANALYTE_TREND_REDUCER = "mean"

_MK_COMMON = (
    "Monotonic trend is tested with the non-parametric Mann-Kendall test "
    "(pymannkendall.original_test, alpha=0.05) on the monthly series ordered by "
    "time; the rate is the Theil-Sen slope vs time. A well is classified only "
    "when it has at least 10 monthly points, or at least 4 monthly points "
    "spanning at least 2 years; otherwise 'not enough data'."
)


def trend_reducer(analyte: bool = False) -> str:
    """Monthly aggregate for a trend: mean for analytes, median for water levels."""
    return _ANALYTE_TREND_REDUCER if analyte else _WATERLEVEL_TREND_REDUCER


def trend_method_description(analyte: bool = False) -> str:
    """Human-readable description of a trend product's method, embedded in the
    collection so consumers know how the classification was derived. Uses
    trend_reducer(analyte), the same aggregate the computation uses.

    The water-level trend mirrors the Ocotillo API
    ogc_depth_to_water_trend_wells materialized view, using Mann-Kendall +
    Theil-Sen instead of OLS."""
    kept = f"the monthly {trend_reducer(analyte).upper()}"
    if analyte:
        return (
            "Analyte concentration trend per well. Observations are reduced to "
            f"one point per calendar month (UTC), keeping {kept} concentration. "
            f"{_MK_COMMON} A significant increasing slope is 'increasing' "
            "(concentration rising), a significant decreasing slope is "
            "'decreasing', else 'stable'."
        )
    return (
        "Depth-to-water trend per well. Observations are reduced to one point "
        f"per calendar month (UTC), keeping {kept} depth-to-water. {_MK_COMMON} "
        "A significant increasing slope is 'increasing' (water level getting "
        "DEEPER, i.e. a declining water table), a significant decreasing slope "
        "is 'decreasing' (water level getting SHALLOWER), else 'stable'."
    )


TREND_METHOD_DESCRIPTION = trend_method_description()
ANALYTE_TREND_METHOD_DESCRIPTION = trend_method_description(analyte=True)


def trend_period_method(window_years, window_start, window_end) -> str:
    """Sentence appended to a trend collection's method text naming the period
    the trend covers. *window_start*/*window_end* are ISO strings (or None for
    the full-record trend)."""
    if window_years is None:
        return (
            "Period: each well's full record (overall trend); see "
            "first_observation_datetime and last_observation_datetime."
        )
    return (
        f"Period: fixed {window_years:g}-year window from {window_start} to "
        f"{window_end}: the {window_years:g} years of whole calendar months before "
        "the month of the run, the same for every well. Only measurements inside "
        "the window are used, and the minimum-data rule applies to the monthly "
        "points inside the window."
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


_REDUCERS = {
    "min": min,
    "max": max,
    "mean": lambda vs: sum(vs) / len(vs),
    "median": median,
}


def _bucket_start(dt: datetime, period: str) -> datetime:
    if period == "day":
        return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    if period == "month":
        return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)
    raise ValueError(f"unknown aggregation period {period!r}")


def aggregate_series(
    obs_list: list,
    reducer: str = "min",
    period: str = "day",
    start_epoch: Optional[float] = None,
    end_epoch: Optional[float] = None,
) -> tuple[int, list]:
    """Reduce a well's observations to one point per calendar *period* ("day"
    or "month", UTC), keyed at the period's start epoch. *reducer* selects the
    aggregate: "min", "max", "mean", or "median".

    When *start_epoch*/*end_epoch* are given, only observations in the half-open
    range [start_epoch, end_epoch) are kept (applied to the raw readings, before
    aggregating).

    *obs_list* is a list of observation payload dicts (parameter_value,
    date_measured, time_measured). Operating on dicts avoids rebuilding
    ParameterRecord objects for what can be millions of observations.

    Downsampling bounds the O(n^2) Mann-Kendall cost for high-frequency wells
    (e.g. continuous loggers) and removes within-period sampling noise. Returns
    (raw_observation_count, [(period_epoch_seconds, value), ...] sorted by time);
    raw_observation_count counts only the readings that were kept.
    """
    raw_count = 0
    buckets: dict = {}  # period start epoch -> [values]
    for obs in obs_list:
        value = obs.get("parameter_value")
        epoch = parse_epoch_seconds(obs.get("date_measured"), obs.get("time_measured"))
        if value is None or epoch is None:
            continue
        if start_epoch is not None and epoch < start_epoch:
            continue
        if end_epoch is not None and epoch >= end_epoch:
            continue
        try:
            v = float(value)
        except (TypeError, ValueError):
            continue
        raw_count += 1
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        key = _bucket_start(dt, period).timestamp()
        buckets.setdefault(key, []).append(v)

    reduce_fn = _REDUCERS[reducer]
    pairs = sorted((key, reduce_fn(vals)) for key, vals in buckets.items())
    return raw_count, pairs


def daily_series(obs_list: list, reducer: str = "min") -> tuple[int, list]:
    """One point per calendar day (see aggregate_series)."""
    return aggregate_series(obs_list, reducer, "day")


def trend_series(
    obs_list: list,
    analyte: bool = False,
    start_epoch: Optional[float] = None,
    end_epoch: Optional[float] = None,
) -> tuple[int, list]:
    """The series every trend is tested on: one trend_reducer(analyte) value
    per calendar month, optionally limited to [start_epoch, end_epoch) (see
    aggregate_series)."""
    return aggregate_series(
        obs_list, trend_reducer(analyte), _TREND_BUCKET, start_epoch, end_epoch
    )


def trend_window(window_years: float, now: Optional[datetime] = None) -> tuple:
    """Trailing window of whole calendar months before the month of *now*
    (default: current UTC time). It ends at the start of *now*'s month (the
    current month is partial, so it is left out) and starts *window_years*
    years (rounded to whole months) earlier. Aligning to months keeps the first
    and last monthly medians from covering only part of a month.

    Returns (start_epoch, end_epoch), a half-open range [start, end)."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    end = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    months_back = end.year * 12 + (end.month - 1) - round(window_years * 12)
    start = datetime(months_back // 12, months_back % 12 + 1, 1, tzinfo=timezone.utc)
    return start.timestamp(), end.timestamp()


def qualifies_for_trend(record_count, span_years) -> bool:
    return record_count >= _TREND_MIN_RECORDS or (
        record_count >= _TREND_MIN_RECORDS_WITH_SPAN
        and span_years >= _TREND_MIN_SPAN_YEARS
    )


def insufficient_reason(
    record_count, span_years, in_window: bool = False
) -> Optional[str]:
    """Why a well fails the minimum-data rule (counted in monthly values), or
    None when it qualifies. *in_window* says the count is for a fixed window."""
    if qualifies_for_trend(record_count, span_years):
        return None
    where = " in the window" if in_window else ""
    if record_count == 0:
        return f"no measurements{where}"
    rule = (
        f"need at least {_TREND_MIN_RECORDS} monthly values, or at least "
        f"{_TREND_MIN_RECORDS_WITH_SPAN} spanning {_TREND_MIN_SPAN_YEARS:g}+ years"
    )
    if record_count < _TREND_MIN_RECORDS_WITH_SPAN:
        return f"only {record_count} monthly values{where}; {rule}"
    return (
        f"{record_count} monthly values{where} span only {span_years:.1f} years; "
        f"{rule}"
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
