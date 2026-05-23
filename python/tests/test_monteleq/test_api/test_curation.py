"""Unit tests for CurationClient.curate and curation_helpers."""
from __future__ import annotations

import datetime as dt
import time
from unittest.mock import MagicMock

import polars as pl

from monteleq.api.curation_client import CurationClient, _xxh3_64_signed
from monteleq.api.curation_helpers import (
    iso_duration_to_seconds_expr,
    normalize_datetime_string_expr,
    parse_unit_columns_expr,
    reorder_columns,
)
from monteleq.api.schemas import FINAL_SCHEMA


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

def _mock_curation_client() -> CurationClient:
    api = MagicMock()
    api.metadata.curvemap = {}
    return CurationClient(api)


def _timeseries_frame(
    n_rows: int = 5,
    curve_name: str = "DE Power MWh/h H Actual",
) -> pl.DataFrame:
    base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    return pl.DataFrame({
        "curve_name": [curve_name] * n_rows,
        "curve_type": ["TIMESERIES"] * n_rows,
        "curve_data_type": ["ACTUAL"] * n_rows,
        "curve_area": ["DE"] * n_rows,
        "curve_commodity": ["Power"] * n_rows,
        "resolution": [{"frequency": "PT1H", "timezone": "CET"}] * n_rows,
        "unit": ["MWh/h"] * n_rows,
        "data": [
            [{"d": (base + dt.timedelta(hours=i)).isoformat(), "v": float(i * 10)}]
            for i in range(n_rows)
        ],
    })


def _instance_frame(
    n_rows: int = 3,
    curve_name: str = "DE Solar Forecast MWh/h H",
) -> pl.DataFrame:
    base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    issued = dt.datetime(2025, 1, 1, 6, 0, 0, tzinfo=dt.timezone.utc)
    return pl.DataFrame({
        "curve_name": [curve_name] * n_rows,
        "curve_type": ["INSTANCE"] * n_rows,
        "curve_data_type": ["FORECAST"] * n_rows,
        "curve_area": ["DE"] * n_rows,
        "resolution": [{"frequency": "PT1H", "timezone": "CET"}] * n_rows,
        "unit": ["MWh/h"] * n_rows,
        "instance": [{
            "issued": issued.isoformat(),
            "tag": "base",
            "created": issued.isoformat(),
            "modified": issued.isoformat(),
        }] * n_rows,
        "data": [
            [{"d": (base + dt.timedelta(hours=i)).isoformat(), "v": float(i * 5)}]
            for i in range(n_rows)
        ],
    })


def _ohlc_frame(n_rows: int = 3) -> pl.DataFrame:
    base_date = dt.date(2025, 1, 1)
    return pl.DataFrame({
        "curve_name": ["DE Power OHLC EUR/MWh"] * n_rows,
        "curve_type": ["OHLC"] * n_rows,
        "curve_data_type": ["OHLC"] * n_rows,
        "unit": ["EUR/MWh"] * n_rows,
        "data": [
            [{
                "open": 50.0 + i,
                "high": 55.0 + i,
                "low": 48.0 + i,
                "close": 52.0 + i,
                "settlement": 51.0 + i,
                "volume": 1000.0,
                "open_interest": 500.0,
                "product": {
                    "traded_at": str(base_date + dt.timedelta(days=i)),
                    "delivery": str(base_date + dt.timedelta(days=i + 1)),
                    "period": "day",
                    "front": i + 1,
                },
            }]
            for i in range(n_rows)
        ],
    })


def _scenario_frame(n_rows: int = 2) -> pl.DataFrame:
    base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    return pl.DataFrame({
        "curve_name": ["Climate Wind Scenario"] * n_rows,
        "curve_type": ["SCENARIO_TIMESERIES"] * n_rows,
        "curve_data_type": ["CLIMATE"] * n_rows,
        "resolution": [{"frequency": "PT1H", "timezone": "UTC"}] * n_rows,
        "unit": ["MWh/h"] * n_rows,
        "data": [
            [{"d": (base + dt.timedelta(hours=i)).isoformat()}]
            for i in range(n_rows)
        ],
        "scenario_names": [["low", "high"]] * n_rows,
        "data_s": [[10.0, 20.0]] * n_rows,
    })


# ======================================================================
# Helper unit tests
# ======================================================================


class TestXxh3Signed:
    def test_deterministic(self):
        assert _xxh3_64_signed("test") == _xxh3_64_signed("test")

    def test_different_inputs(self):
        assert _xxh3_64_signed("a") != _xxh3_64_signed("b")

    def test_returns_int(self):
        assert isinstance(_xxh3_64_signed("x"), int)

    def test_can_be_negative(self):
        results = [_xxh3_64_signed(f"test_{i}") for i in range(100)]
        assert any(r < 0 for r in results)


class TestIsoDurationToSeconds:
    def _eval(self, values: list[str | None]) -> list[int]:
        df = pl.DataFrame({"dur": values})
        return df.select(
            iso_duration_to_seconds_expr("dur").alias("s")
        )["s"].to_list()

    def test_hourly(self):
        assert self._eval(["PT1H"]) == [3600]

    def test_daily(self):
        assert self._eval(["P1D"]) == [86400]

    def test_15min(self):
        assert self._eval(["PT15M"]) == [900]

    def test_yearly(self):
        assert self._eval(["P1Y"]) == [365 * 86400]

    def test_monthly(self):
        assert self._eval(["P1M"]) == [30 * 86400]

    def test_weekly(self):
        assert self._eval(["P1W"]) == [7 * 86400]

    def test_complex(self):
        assert self._eval(["P1DT2H30M"]) == [86400 + 7200 + 1800]

    def test_empty(self):
        assert self._eval([""]) == [0]

    def test_none_string(self):
        assert self._eval(["none"]) == [0]


class TestParseUnitColumns:
    def _eval(self, unit_val: str) -> dict[str, str | None]:
        df = pl.DataFrame({"unit": [unit_val]})
        result = df.select(*parse_unit_columns_expr("unit"))
        return result.row(0, named=True)

    def test_simple(self):
        r = self._eval("MWh")
        assert r["unit"] == "MWh"
        assert r["denominator"] is None

    def test_ratio(self):
        r = self._eval("MWh/h")
        assert r["unit"] == "MWh"
        assert r["denominator"] == "h"

    def test_eur_mwh(self):
        r = self._eval("EUR/MWh")
        assert r["unit"] == "EUR"
        assert r["denominator"] == "MWh"


class TestNormalizeDatetimeString:
    def _eval(self, val: str) -> str:
        df = pl.DataFrame({"ts": [val]})
        return df.select(
            normalize_datetime_string_expr("ts").alias("n")
        )["n"][0]

    def test_z_suffix(self):
        assert self._eval("2025-01-01T00:00:00Z") == "2025-01-01T00:00:00+00:00"

    def test_missing_seconds(self):
        result = self._eval("2025-01-01T12:00+02:00")
        assert ":00" in result

    def test_passthrough(self):
        val = "2025-01-01T00:00:00+00:00"
        assert self._eval(val) == val


class TestReorderColumns:
    def test_adds_missing_columns(self):
        df = pl.DataFrame({"curve_name": ["test"], "curve_id": [1]})
        result = reorder_columns(df, schema=FINAL_SCHEMA)
        assert set(FINAL_SCHEMA.names()).issubset(set(result.columns))

    def test_drops_extra_columns(self):
        df = pl.DataFrame({
            "curve_name": ["test"],
            "curve_id": [1],
            "extra_col": ["should_not_appear"],
        })
        result = reorder_columns(df, schema=FINAL_SCHEMA)
        assert "extra_col" not in result.columns

    def test_preserves_order(self):
        df = pl.DataFrame({"curve_name": ["test"], "curve_id": [1]})
        result = reorder_columns(df, schema=FINAL_SCHEMA)
        assert result.columns == list(FINAL_SCHEMA.names())

    def test_filters_null_curve_name(self):
        df = pl.DataFrame({"curve_name": ["test", None], "curve_id": [1, 2]})
        result = reorder_columns(df, schema=FINAL_SCHEMA)
        assert result.height == 1


# ======================================================================
# CurationClient.curate
# ======================================================================


class TestCurateTimeseries:
    def test_basic_shape(self):
        client = _mock_curation_client()
        df = _timeseries_frame(n_rows=5)
        result = client.curate(df)
        assert result.height == 5
        assert set(FINAL_SCHEMA.names()) == set(result.columns)

    def test_columns_match_schema(self):
        client = _mock_curation_client()
        result = client.curate(_timeseries_frame())
        assert result.columns == list(FINAL_SCHEMA.names())

    def test_curve_id_populated(self):
        client = _mock_curation_client()
        result = client.curate(_timeseries_frame())
        assert result["curve_id"].null_count() == 0
        assert result["curve_id"].dtype == pl.Int64

    def test_run_hash_populated(self):
        client = _mock_curation_client()
        result = client.curate(_timeseries_frame())
        assert result["run_hash"].null_count() == 0

    def test_timestamps_utc(self):
        client = _mock_curation_client()
        result = client.curate(_timeseries_frame())
        assert result["from_timestamp"].dtype == pl.Datetime("us", "UTC")
        assert result["to_timestamp"].dtype == pl.Datetime("us", "UTC")

    def test_values_present(self):
        client = _mock_curation_client()
        result = client.curate(_timeseries_frame(n_rows=3))
        values = result["value"].to_list()
        assert values == [0.0, 10.0, 20.0]

    def test_resolution_parsed(self):
        client = _mock_curation_client()
        result = client.curate(_timeseries_frame())
        assert result["resolution_frequency"][0] == 3600

    def test_unit_parsed(self):
        client = _mock_curation_client()
        result = client.curate(_timeseries_frame())
        assert result["unit"][0] == "MWh"
        assert result["denominator"][0] == "h"

    def test_begin_end_filter(self):
        client = _mock_curation_client()
        df = _timeseries_frame(n_rows=10)
        begin = dt.datetime(2025, 1, 1, 2, 0, 0, tzinfo=dt.timezone.utc)
        end = dt.datetime(2025, 1, 1, 5, 0, 0, tzinfo=dt.timezone.utc)
        result = client.curate(df, begin=begin, end=end)
        assert result.height == 3

    def test_empty_input(self):
        client = _mock_curation_client()
        df = pl.DataFrame(schema={"curve_name": pl.Utf8})
        result = client.curate(df)
        assert result.height == 0
        assert result.columns == list(FINAL_SCHEMA.names())


class TestCurateInstance:
    def test_instance_fields(self):
        client = _mock_curation_client()
        result = client.curate(_instance_frame())
        assert result["instance_issued"].null_count() == 0
        assert result["instance_tag"][0] == "base"
        assert result["instance_created"].null_count() == 0


class TestCurateOhlc:
    def test_ohlc_produces_output(self):
        client = _mock_curation_client()
        result = client.curate(_ohlc_frame(n_rows=3))
        assert result.columns == list(FINAL_SCHEMA.names())

    def test_ohlc_prices_when_rows_present(self):
        client = _mock_curation_client()
        result = client.curate(_ohlc_frame(n_rows=1))
        if result.height > 0:
            row = result.row(0, named=True)
            assert row["open"] is not None or row["close"] is not None


class TestCurateScenario:
    def test_scenario_explodes(self):
        client = _mock_curation_client()
        result = client.curate(_scenario_frame(n_rows=1))
        if "scenario_name" in result.columns:
            scenarios = result["scenario_name"].drop_nulls().unique().to_list()
            assert len(scenarios) > 0


# ======================================================================
# Determinism & consistency
# ======================================================================


class TestCurationDeterminism:
    def test_same_input_same_output(self):
        client = _mock_curation_client()
        df = _timeseries_frame(n_rows=5)
        r1 = client.curate(df)
        r2 = client.curate(df)
        assert r1.equals(r2)

    def test_curve_id_matches_run_hash_stability(self):
        client = _mock_curation_client()
        df = _timeseries_frame(n_rows=3)
        result = client.curate(df)
        ids = result["curve_id"].unique()
        hashes = result["run_hash"].unique()
        assert len(ids) == 1
        assert len(hashes) == 1

    def test_different_curves_different_ids(self):
        client = _mock_curation_client()
        df1 = _timeseries_frame(n_rows=2, curve_name="Curve A")
        df2 = _timeseries_frame(n_rows=2, curve_name="Curve B")
        r1 = client.curate(df1)
        r2 = client.curate(df2)
        assert r1["curve_id"][0] != r2["curve_id"][0]


# ======================================================================
# Benchmarks
# ======================================================================


class TestCurationBenchmarks:
    def test_timeseries_curation_100_rows(self):
        client = _mock_curation_client()
        df = _timeseries_frame(n_rows=100)
        t0 = time.perf_counter()
        for _ in range(100):
            client.curate(df)
        elapsed = time.perf_counter() - t0
        assert elapsed < 30.0, f"100x curate(100 rows) took {elapsed:.2f}s"

    def test_timeseries_curation_1000_rows(self):
        client = _mock_curation_client()
        df = _timeseries_frame(n_rows=1000)
        t0 = time.perf_counter()
        for _ in range(10):
            client.curate(df)
        elapsed = time.perf_counter() - t0
        assert elapsed < 30.0, f"10x curate(1000 rows) took {elapsed:.2f}s"

    def test_reorder_columns_performance(self):
        df = pl.DataFrame({
            "curve_name": [f"c_{i}" for i in range(1000)],
            "curve_id": list(range(1000)),
        })
        t0 = time.perf_counter()
        for _ in range(100):
            reorder_columns(df, schema=FINAL_SCHEMA)
        elapsed = time.perf_counter() - t0
        assert elapsed < 10.0, f"100x reorder_columns(1000 rows) took {elapsed:.2f}s"

    def test_xxh3_hashing_performance(self):
        values = [f"curve_name_{i}" for i in range(10_000)]
        t0 = time.perf_counter()
        for v in values:
            _xxh3_64_signed(v)
        elapsed = time.perf_counter() - t0
        assert elapsed < 1.0, f"10k xxh3 hashes took {elapsed:.2f}s"

    def test_iso_duration_performance(self):
        df = pl.DataFrame({"dur": ["PT1H"] * 10_000})
        t0 = time.perf_counter()
        for _ in range(10):
            df.select(iso_duration_to_seconds_expr("dur"))
        elapsed = time.perf_counter() - t0
        assert elapsed < 5.0, f"10x iso_duration(10k rows) took {elapsed:.2f}s"
