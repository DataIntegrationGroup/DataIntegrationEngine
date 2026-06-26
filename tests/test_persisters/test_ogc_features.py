import json
import os
import tempfile

from backend.persisters.ogc_features import (
    dump_summary_collection,
    dump_timeseries_collection,
    dump_major_chemistry_collection,
)
from backend.record import SummaryRecord, SiteRecord, ParameterRecord


def _make_summary_record(source="nmbgmr_amp", rid="RA-1234", lat=35.0, lon=-106.5):
    return SummaryRecord({
        "source": source,
        "id": rid,
        "name": "Test Well",
        "usgs_site_id": "",
        "alternate_site_id": "",
        "latitude": lat,
        "longitude": lon,
        "horizontal_datum": "WGS84",
        "elevation": 1650.0,
        "elevation_units": "ft",
        "well_depth": None,
        "well_depth_units": "ft",
        "parameter_name": "waterlevels",
        "parameter_units": "ft",
        "nrecords": 10,
        "min": 200.0,
        "max": 250.0,
        "mean": 225.0,
        "earliest_date": "1990-01-01",
        "earliest_time": "00:00:00",
        "earliest_value": 200.0,
        "earliest_units": "ft",
        "latest_date": "2024-01-01",
        "latest_time": "00:00:00",
        "latest_value": 220.0,
        "latest_units": "ft",
    })


def _make_site_record(source="nmbgmr_amp", rid="RA-1234", lat=35.0, lon=-106.5):
    return SiteRecord({
        "source": source,
        "id": rid,
        "name": "Test Well",
        "latitude": lat,
        "longitude": lon,
        "elevation": 1650.0,
        "elevation_units": "ft",
        "horizontal_datum": "WGS84",
        "vertical_datum": "",
        "usgs_site_id": "",
        "alternate_site_id": "",
        "formation": "",
        "aquifer": "",
        "well_depth": None,
        "well_depth_units": "ft",
    })


def _make_wl_record(source="nmbgmr_amp", rid="RA-1234", date="2024-01-15", value=212.4):
    return ParameterRecord({
        "source": source,
        "id": rid,
        "parameter_name": "waterlevels",
        "parameter_value": value,
        "parameter_units": "ft",
        "date_measured": date,
        "time_measured": "00:00:00",
        "source_parameter_name": "depth_to_water",
        "source_parameter_units": "ft",
        "conversion_factor": 1.0,
        "record_type": "waterlevels",
    })


class TestDumpSummaryCollection:
    def test_ogc_required_fields(self, tmp_path):
        """§V: OGC FC MUST include top-level id, type, numberReturned, timeStamp."""
        records = [_make_summary_record()]
        out = tmp_path / "summary.geojson"
        result = dump_summary_collection(str(out), records, {"id": "nm_waterlevels"})

        assert result["type"] == "FeatureCollection"
        assert result["id"] == "nm_waterlevels"
        assert "timeStamp" in result
        assert "numberReturned" in result
        assert result["numberReturned"] == 1

    def test_feature_has_top_level_id(self, tmp_path):
        """§V: Each Feature MUST have top-level id (not only in properties)."""
        records = [_make_summary_record(source="nmbgmr_amp", rid="RA-1234")]
        out = tmp_path / "summary.geojson"
        result = dump_summary_collection(str(out), records, {"id": "test"})

        feature = result["features"][0]
        assert "id" in feature
        assert feature["id"] == "nmbgmr_amp:RA-1234"

    def test_writes_valid_geojson_file(self, tmp_path):
        records = [_make_summary_record(), _make_summary_record(rid="RA-5678")]
        out = tmp_path / "summary.geojson"
        dump_summary_collection(str(out), records, {"id": "test"})

        with open(str(out)) as f:
            data = json.load(f)
        assert data["numberReturned"] == 2
        assert len(data["features"]) == 2

    def test_geometry_has_coordinates(self, tmp_path):
        records = [_make_summary_record(lat=35.123, lon=-106.456)]
        out = tmp_path / "summary.geojson"
        result = dump_summary_collection(str(out), records, {"id": "test"})

        geom = result["features"][0]["geometry"]
        assert geom["type"] == "Point"
        assert geom["coordinates"][0] == -106.456
        assert geom["coordinates"][1] == 35.123

    def test_empty_records(self, tmp_path):
        out = tmp_path / "summary.geojson"
        result = dump_summary_collection(str(out), [], {"id": "empty"})
        assert result["numberReturned"] == 0
        assert result["features"] == []


class TestDumpTimeseriesCollection:
    def test_flat_one_feature_per_observation(self, tmp_path):
        """§V: ogc_timeseries MUST be flat (one per observation)."""
        site = _make_site_record()
        obs1 = _make_wl_record(date="2024-01-15", value=212.4)
        obs2 = _make_wl_record(date="2024-04-20", value=218.1)

        out = tmp_path / "ts.geojson"
        result = dump_timeseries_collection(
            str(out), [site], [obs1, obs2], {"id": "nm_wl_ts"}
        )

        assert result["numberReturned"] == 2
        assert len(result["features"]) == 2

    def test_iso8601_datetime_property(self, tmp_path):
        """§V: MUST have ISO 8601 `datetime` property on each feature."""
        site = _make_site_record()
        obs = _make_wl_record(date="2024-01-15")

        out = tmp_path / "ts.geojson"
        result = dump_timeseries_collection(str(out), [site], [obs], {"id": "test"})

        props = result["features"][0]["properties"]
        assert "datetime" in props
        assert props["datetime"].startswith("2024-01-15T")

    def test_feature_has_top_level_id(self, tmp_path):
        """§V: Each Feature MUST have top-level id."""
        site = _make_site_record()
        obs = _make_wl_record(date="2024-01-15")

        out = tmp_path / "ts.geojson"
        result = dump_timeseries_collection(str(out), [site], [obs], {"id": "test"})

        feature = result["features"][0]
        assert "id" in feature
        assert "nmbgmr_amp" in feature["id"]
        assert "RA-1234" in feature["id"]

    def test_ogc_required_fields(self, tmp_path):
        """§V: OGC FC MUST include type, id, numberReturned, timeStamp."""
        out = tmp_path / "ts.geojson"
        result = dump_timeseries_collection(str(out), [], [], {"id": "nm_ts"})

        assert result["type"] == "FeatureCollection"
        assert result["id"] == "nm_ts"
        assert "timeStamp" in result
        assert "numberReturned" in result


def _make_chem_record(source, rid, analyte, value, units="mg/L", date="2024-05-01", well_depth=None):
    return SummaryRecord({
        "source": source,
        "id": rid,
        "name": f"Well {rid}",
        "latitude": 34.0,
        "longitude": -106.0,
        "elevation": None,
        "well_depth": well_depth,
        "well_depth_units": "ft",
        "parameter_name": analyte,
        "latest_value": value,
        "latest_units": units,
        "latest_date": date,
    })


class TestMajorChemistryCollection:
    def test_pivots_analytes_into_one_feature_per_well(self, tmp_path):
        records = [
            _make_chem_record("NMBGMR", "W1", "calcium", 42.0, well_depth=120.0),
            _make_chem_record("NMBGMR", "W1", "chloride", 15.0),
            _make_chem_record("WQP", "W2", "calcium", 55.0),
        ]
        out = tmp_path / "mc.geojson"
        result = dump_major_chemistry_collection(str(out), records, {"id": "nm_major_chemistry"})

        assert result["numberReturned"] == 2  # two distinct wells
        by_id = {f["id"]: f for f in result["features"]}

        w1 = by_id["NMBGMR:W1"]["properties"]
        assert w1["calcium"] == 42.0
        assert w1["calcium_units"] == "mg/L"
        assert w1["calcium_date"] == "2024-05-01"
        assert w1["chloride"] == 15.0
        assert w1["well_depth"] == 120.0  # carried from the record that had it

        w2 = by_id["WQP:W2"]["properties"]
        assert w2["calcium"] == 55.0
        assert "chloride" not in w2  # missing analyte omitted

    def test_geometry_and_required_fields(self, tmp_path):
        out = tmp_path / "mc.geojson"
        result = dump_major_chemistry_collection(
            str(out), [_make_chem_record("NMBGMR", "W1", "sodium", 30.0)], {"id": "nm_major_chemistry"}
        )
        assert result["type"] == "FeatureCollection"
        assert "timeStamp" in result
        feat = result["features"][0]
        assert feat["geometry"]["coordinates"] == [-106.0, 34.0]
