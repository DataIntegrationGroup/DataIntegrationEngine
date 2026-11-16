from datetime import datetime, timezone
from pathlib import Path

import yaml

from backend.trend_stats import (
    daily_series,
    insufficient_reason,
    qualifies_for_trend,
    mann_kendall_trend,
    parse_epoch_seconds,
    trend_method_description,
    trend_series,
    trend_window,
    _SECONDS_PER_YEAR,
)


def _obs(date, value):
    return {"parameter_value": value, "date_measured": date, "time_measured": None}


def _epoch(date):
    return datetime.fromisoformat(date).replace(tzinfo=timezone.utc).timestamp()


class TestParseEpochSeconds:
    def test_date_only_and_none(self):
        assert parse_epoch_seconds("2020-01-01", None) is not None
        assert parse_epoch_seconds(None, None) is None
        assert parse_epoch_seconds("not-a-date", None) is None


class TestDailySeries:
    def test_collapses_same_day_with_reducer(self):
        obs = [_obs("2020-01-01", 5.0), _obs("2020-01-01", 1.0), _obs("2020-01-02", 9.0)]
        raw_min, pairs_min = daily_series(obs, "min")
        raw_mean, pairs_mean = daily_series(obs, "mean")
        assert raw_min == 3
        assert [v for _, v in pairs_min] == [1.0, 9.0]      # daily min
        assert [v for _, v in pairs_mean] == [3.0, 9.0]     # daily mean
        # sorted by day ascending
        assert pairs_min[0][0] < pairs_min[1][0]

    def test_skips_unparseable(self):
        obs = [_obs("2020-01-01", None), _obs(None, 5.0), _obs("2020-01-02", 3.0)]
        raw, pairs = daily_series(obs, "min")
        assert raw == 1
        assert [v for _, v in pairs] == [3.0]


class TestMonthlySeries:
    def test_one_median_per_month_keyed_at_first_of_month(self):
        obs = [
            _obs("2020-01-03", 10.0),
            _obs("2020-01-15", 30.0),
            _obs("2020-01-28", 11.0),   # Jan median = 11
            _obs("2020-02-10", 5.0),
            _obs("2020-02-20", 7.0),    # Feb median = 6 (even count)
        ]
        raw, pairs = trend_series(obs)
        assert raw == 5
        assert pairs == [(_epoch("2020-01-01"), 11.0), (_epoch("2020-02-01"), 6.0)]

    def test_dense_daily_logger_collapses_to_monthly_points(self):
        # 2 years of daily readings (2020 is a leap year: 731 days ends
        # 2021-12-31) -> 24 monthly points, not 731.
        start = _epoch("2020-01-01")
        obs = [
            _obs(datetime.fromtimestamp(start + d * 86400, tz=timezone.utc).date().isoformat(), 50.0)
            for d in range(731)
        ]
        raw, pairs = trend_series(obs)
        assert raw == 731
        assert len(pairs) == 24

    def test_window_cuts_raw_readings_before_aggregating(self):
        obs = [_obs("2019-12-31", 99.0), _obs("2020-01-10", 1.0), _obs("2020-03-01", 2.0)]
        # [start, end): a reading exactly at end_epoch is left out.
        obs.append(_obs("2020-03-01", 3.0))
        raw, pairs = trend_series(
            obs, False, _epoch("2020-01-01"), _epoch("2020-03-01")
        )
        assert raw == 1
        assert [v for _, v in pairs] == [1.0]


class TestMethodText:
    def test_matches_the_aggregate(self):
        assert "monthly MEDIAN depth-to-water" in trend_method_description()
        assert "monthly MEAN concentration" in trend_method_description(analyte=True)


class TestTrendWindow:
    def test_whole_months_before_the_run_month(self):
        # Run 2026-11-15 06:00 -> window [2021-11-01, 2026-11-01) for 5 years.
        now = datetime(2026, 11, 15, 6, 0, tzinfo=timezone.utc)
        for years in (5, 10, 20):
            start, end = trend_window(years, now)
            assert end == _epoch("2026-11-01")
            assert start == _epoch(f"{2026 - years}-11-01")

    def test_same_window_for_any_time_in_the_month(self):
        early = trend_window(5, datetime(2026, 11, 1, tzinfo=timezone.utc))
        late = trend_window(5, datetime(2026, 11, 30, 23, 59, tzinfo=timezone.utc))
        assert early == late

    def test_january_run(self):
        start, end = trend_window(5, datetime(2026, 1, 20, tzinfo=timezone.utc))
        assert (start, end) == (_epoch("2021-01-01"), _epoch("2026-01-01"))


class TestQualifies:
    def test_gate(self):
        assert qualifies_for_trend(10, 0) is True          # >= 10 records
        assert qualifies_for_trend(4, 2.0) is True         # 4 records, 2yr span
        assert qualifies_for_trend(4, 1.0) is False        # span too short
        assert qualifies_for_trend(3, 50.0) is False       # too few records

    def test_reason_matches_gate(self):
        assert insufficient_reason(10, 0) is None
        assert insufficient_reason(4, 2.0) is None
        assert insufficient_reason(0, 0) == "no measurements"
        assert insufficient_reason(2, 5.0).startswith("only 2 monthly values;")
        assert insufficient_reason(5, 1.0).startswith(
            "5 monthly values span only 1.0 years;"
        )

    def test_window_wording_is_attached_to_the_count(self):
        assert insufficient_reason(0, 0, in_window=True) == "no measurements in the window"
        assert insufficient_reason(2, 5.0, in_window=True).startswith(
            "only 2 monthly values in the window; need"
        )
        assert insufficient_reason(5, 1.0, in_window=True).startswith(
            "5 monthly values in the window span only 1.0 years; need"
        )


class TestMannKendall:
    def test_increasing_decreasing_stable(self):
        years = [2010 + i for i in range(12)]
        inc = mann_kendall_trend(years, [1.0 + i for i in range(12)])
        dec = mann_kendall_trend(years, [12.0 - i for i in range(12)])
        flat = mann_kendall_trend(years, [5.0] * 12)
        assert inc[0] == "increasing" and inc[1] > 0
        assert dec[0] == "decreasing" and dec[1] < 0
        assert flat[0] == "stable"

    def test_slope_is_per_year(self):
        # +0.5 per year over 12 years -> Theil-Sen slope ~0.5/yr
        years = [2010 + i for i in range(12)]
        cat, slope, p, tau = mann_kendall_trend(years, [50.0 + 0.5 * i for i in range(12)])
        assert round(slope, 3) == 0.5


def test_products_yaml_has_full_record_and_window_trend_layers():
    path = Path(__file__).resolve().parents[1] / "orchestration" / "config" / "products.yaml"
    products = {p["id"]: p for p in yaml.safe_load(path.read_text())["products"]}
    assert "window_years" not in products["nm_waterlevel_trends"]
    assert "Full Record" in products["nm_waterlevel_trends"]["title"]
    for years in (5, 10, 20):
        p = products[f"nm_waterlevel_trends_{years}yr"]
        assert p["output_type"] == "ogc_waterlevel_trend"
        assert p["window_years"] == years
        assert p["schedule"] == products["nm_waterlevel_trends"]["schedule"]
