from backend.trend_stats import (
    daily_series,
    qualifies_for_trend,
    mann_kendall_trend,
    parse_epoch_seconds,
    _SECONDS_PER_YEAR,
)


def _obs(date, value):
    return {"parameter_value": value, "date_measured": date, "time_measured": None}


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


class TestQualifies:
    def test_gate(self):
        assert qualifies_for_trend(10, 0) is True          # >= 10 records
        assert qualifies_for_trend(4, 2.0) is True         # 4 records, 2yr span
        assert qualifies_for_trend(4, 1.0) is False        # span too short
        assert qualifies_for_trend(3, 50.0) is False       # too few records


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
