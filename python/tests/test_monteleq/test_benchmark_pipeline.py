"""
Ingestion pipeline benchmark suite.

Simulates the full ingestion pipeline locally (no API calls, no Databricks)
and benchmarks each stage to identify bottlenecks and validate optimizations.

Pipeline stages benchmarked:
    1. Metadata loading & curve filtering
    2. CurveRequest creation & fan-out (request expansion)
    3. Response curation (timeseries, instance, OHLC)
    4. Hashing (xxh3_64 for curve_id + run_hash)
    5. Data struct assembly (make_data with timezone localization)
    6. Schema reordering (final projection)
    7. Batch concatenation & grouping (table routing)
    8. End-to-end simulated pipeline
"""
from __future__ import annotations

import ctypes
import datetime as dt
import json
import random
import string
import time
from typing import Iterator
from unittest.mock import MagicMock

import polars as pl
import pytest
import xxhash
from energyquantified.metadata import CurveType, DataType

from monteleq.model import Curve, Resolution, Instance
from monteleq.api.request import CurveRequest
from monteleq.api.curation_client import (
    CurationClient,
    _xxh3_64_signed,
    _xxh3_batch,
    _stable_xxh3_hash_expr,
    _canonical_struct_json_expr,
)
from monteleq.api.curation_helpers import (
    iso_duration_to_seconds_expr,
    make_data,
    normalize_ohlc_frame,
    reorder_columns,
    localize_by_distinct_timezones,
    resolve_effective_resolution_timezone,
    data_struct_expr,
    parse_unit_columns_expr,
)
from monteleq.api.schemas import FINAL_SCHEMA


# ======================================================================
# Data generators
# ======================================================================

AREAS = ["DE", "FR", "NO", "SE", "DK", "FI", "NL", "BE", "AT", "CH", "PL", "CZ", "ES", "IT", "GB"]
COMMODITIES = ["Power", "Gas", "Carbon", "Oil"]
SOURCES = ["ENTSO-E", "Montel", "EPEX", "NordPool", "ICE", "EEX"]
FREQUENCIES = ["PT15M", "PT1H", "P1D", "P1W", "P1M", "P1Y"]
TIMEZONES = ["CET", "UTC", "Europe/Berlin", "Europe/Oslo", "Europe/Paris", "Europe/London"]
UNITS = ["MWh/h", "EUR/MWh", "GW", "MW", "GBP/therm", "USD/bbl"]
CATEGORIES_POOL = [
    "Wind", "Solar", "Hydro", "Nuclear", "Gas", "Price", "Consumption",
    "Exchange", "Biomass", "Geothermal", "Carbon", "Oil", "Futures",
]


def _random_curve(
    idx: int,
    curve_type: CurveType = CurveType.TIMESERIES,
    data_type: DataType = DataType.ACTUAL,
) -> Curve:
    area = AREAS[idx % len(AREAS)]
    commodity = COMMODITIES[idx % len(COMMODITIES)]
    cat1 = CATEGORIES_POOL[idx % len(CATEGORIES_POOL)]
    cat2 = CATEGORIES_POOL[(idx + 3) % len(CATEGORIES_POOL)]
    freq = FREQUENCIES[idx % len(FREQUENCIES)]
    tz = TIMEZONES[idx % len(TIMEZONES)]
    unit = UNITS[idx % len(UNITS)]
    source = SOURCES[idx % len(SOURCES)]
    return Curve(
        name=f"{area} {commodity} {cat1} {cat2} {unit} {freq} {data_type.name} {idx}",
        area=area,
        categories=(cat1, cat2),
        resolution=Resolution(frequency=freq, timezone=tz),
        unit=unit,
        source=source,
        data_type=data_type,
        curve_type=curve_type,
        commodity=commodity,
    )


def _generate_curvemap(n_curves: int = 1000) -> dict[str, Curve]:
    curves: dict[str, Curve] = {}
    curve_types = [CurveType.TIMESERIES, CurveType.INSTANCE, CurveType.OHLC, CurveType.INSTANCE_PERIOD]
    data_types = [DataType.ACTUAL, DataType.FORECAST, DataType.NORMAL, DataType.REMIT]
    for i in range(n_curves):
        ct = curve_types[i % len(curve_types)]
        dty = data_types[i % len(data_types)]
        c = _random_curve(i, curve_type=ct, data_type=dty)
        curves[c.name] = c
    return curves


def _timeseries_response_frame(
    n_curves: int = 10,
    n_points_per_curve: int = 100,
    timezone: str = "CET",
) -> pl.DataFrame:
    base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    rows: list[dict] = []
    for ci in range(n_curves):
        curve_name = f"DE Power MWh/h H Actual {ci}"
        data_points = [
            {"d": (base + dt.timedelta(hours=pi)).isoformat(), "v": float(pi * 10 + ci)}
            for pi in range(n_points_per_curve)
        ]
        rows.append({
            "curve_name": curve_name,
            "curve_type": "TIMESERIES",
            "curve_data_type": "ACTUAL",
            "curve_area": "DE",
            "curve_commodity": "Power",
            "curve_source": "ENTSO-E",
            "resolution": {"frequency": "PT1H", "timezone": timezone},
            "unit": "MWh/h",
            "data": data_points,
        })
    return pl.DataFrame(rows)


def _instance_response_frame(
    n_curves: int = 5,
    n_instances_per_curve: int = 3,
    n_points_per_instance: int = 48,
) -> pl.DataFrame:
    base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    rows: list[dict] = []
    for ci in range(n_curves):
        curve_name = f"DE Solar Forecast MWh/h H {ci}"
        for ii in range(n_instances_per_curve):
            issued = base + dt.timedelta(hours=6 * ii)
            data_points = [
                {"d": (base + dt.timedelta(hours=pi)).isoformat(), "v": float(pi + ci + ii)}
                for pi in range(n_points_per_instance)
            ]
            rows.append({
                "curve_name": curve_name,
                "curve_type": "INSTANCE",
                "curve_data_type": "FORECAST",
                "curve_area": "DE",
                "resolution": {"frequency": "PT1H", "timezone": "CET"},
                "unit": "MWh/h",
                "instance": {
                    "issued": issued.isoformat(),
                    "tag": "base",
                    "created": issued.isoformat(),
                    "modified": issued.isoformat(),
                },
                "data": data_points,
            })
    return pl.DataFrame(rows)


def _ohlc_response_frame(n_curves: int = 3, n_days: int = 30) -> pl.DataFrame:
    base_date = dt.date(2025, 1, 1)
    rows: list[dict] = []
    for ci in range(n_curves):
        curve_name = f"DE Power OHLC EUR/MWh {ci}"
        data_points = []
        for di in range(n_days):
            delivery = base_date + dt.timedelta(days=di)
            traded_at = base_date + dt.timedelta(days=di - 1)
            data_points.append({
                "open": 50.0 + di + ci,
                "high": 55.0 + di + ci,
                "low": 48.0 + di + ci,
                "close": 52.0 + di + ci,
                "settlement": 51.0 + di + ci,
                "volume": 1000.0,
                "open_interest": 500.0,
                "product": {
                    "traded_at": str(traded_at),
                    "delivery": str(delivery),
                    "period": "day",
                    "front": di + 1,
                },
            })
        rows.append({
            "curve_name": curve_name,
            "curve_type": "OHLC",
            "curve_data_type": "OHLC",
            "unit": "EUR/MWh",
            "data": data_points,
        })
    return pl.DataFrame(rows)


def _mixed_timezone_frame(n_rows: int = 500) -> pl.DataFrame:
    base = dt.datetime(2025, 1, 1)
    tz_list = ["CET", "UTC", "Europe/Oslo", "Europe/Paris", "Europe/Gas_Day"]
    rows: list[dict] = []
    for i in range(n_rows):
        tz = tz_list[i % len(tz_list)]
        ts = base + dt.timedelta(hours=i)
        rows.append({
            "curve_name": f"curve_{i % 10}",
            "run_hash": i,
            "resolution_frequency": 3600,
            "resolution_timezone": tz,
            "data_d": ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "data_v": float(i),
        })
    return pl.DataFrame(rows)


def _mock_curation_client(curvemap: dict[str, Curve] | None = None) -> CurationClient:
    api = MagicMock()
    api.metadata.curvemap = curvemap or {}
    return CurationClient(api)


# ======================================================================
# Stage 1: Metadata loading & curve filtering
# ======================================================================


class TestBenchmarkMetadata:
    """Benchmark curve catalog operations."""

    def test_curvemap_build_1k(self):
        """Build a 1k-curve catalog from parsed mappings."""
        mappings = [
            {
                "name": f"Curve {i}",
                "area": AREAS[i % len(AREAS)],
                "curve_type": ["TIMESERIES", "INSTANCE", "OHLC"][i % 3],
                "data_type": ["ACTUAL", "FORECAST", "NORMAL"][i % 3],
                "commodity": COMMODITIES[i % len(COMMODITIES)],
                "unit": UNITS[i % len(UNITS)],
                "resolution": {"frequency": FREQUENCIES[i % len(FREQUENCIES)], "timezone": "CET"},
                "categories": [CATEGORIES_POOL[i % len(CATEGORIES_POOL)]],
            }
            for i in range(1000)
        ]
        t0 = time.perf_counter()
        cm = {m["name"]: Curve.parse_mapping(m) for m in mappings}
        elapsed = time.perf_counter() - t0
        assert len(cm) == 1000
        assert elapsed < 2.0, f"1k curvemap build took {elapsed:.2f}s"
        print(f"\n  curvemap_build_1k: {elapsed:.4f}s")

    def test_curvemap_build_10k(self):
        """Build a 10k-curve catalog (closer to production 31k)."""
        mappings = [
            {
                "name": f"Curve {i}",
                "area": AREAS[i % len(AREAS)],
                "curve_type": ["TIMESERIES", "INSTANCE", "OHLC"][i % 3],
                "data_type": ["ACTUAL", "FORECAST", "NORMAL"][i % 3],
                "commodity": COMMODITIES[i % len(COMMODITIES)],
                "unit": UNITS[i % len(UNITS)],
                "resolution": {"frequency": FREQUENCIES[i % len(FREQUENCIES)], "timezone": "CET"},
                "categories": [CATEGORIES_POOL[i % len(CATEGORIES_POOL)]],
            }
            for i in range(10_000)
        ]
        t0 = time.perf_counter()
        cm = {m["name"]: Curve.parse_mapping(m) for m in mappings}
        elapsed = time.perf_counter() - t0
        assert len(cm) == 10_000
        assert elapsed < 10.0, f"10k curvemap build took {elapsed:.2f}s"
        print(f"\n  curvemap_build_10k: {elapsed:.4f}s")

    def test_curve_filtering_by_category(self):
        """Filter 10k curves by category (simulates plan_categories fan-out)."""
        cm = _generate_curvemap(10_000)
        curves_list = list(cm.values())
        categories = sorted({c.categories[0] for c in curves_list if c.categories})

        t0 = time.perf_counter()
        for cat in categories:
            filtered = [c for c in curves_list if c.categories and cat in c.categories]
        elapsed = time.perf_counter() - t0
        assert elapsed < 5.0, f"filtering 10k curves by {len(categories)} categories took {elapsed:.2f}s"
        print(f"\n  filter_by_category ({len(categories)} cats over 10k curves): {elapsed:.4f}s")

    def test_table_name_distribution(self):
        """Verify table_name routing across diverse curve types."""
        cm = _generate_curvemap(1000)
        t0 = time.perf_counter()
        table_names: dict[str, int] = {}
        for c in cm.values():
            tn = c.table_name(prefix="curated_")
            table_names[tn] = table_names.get(tn, 0) + 1
        elapsed = time.perf_counter() - t0
        assert elapsed < 1.0
        assert len(table_names) > 1
        print(f"\n  table_name_distribution: {len(table_names)} tables in {elapsed:.4f}s")
        for tn, count in sorted(table_names.items(), key=lambda x: -x[1])[:10]:
            print(f"    {tn}: {count} curves")


# ======================================================================
# Stage 2: CurveRequest creation & expansion
# ======================================================================


class TestBenchmarkRequestExpansion:
    """Benchmark CurveRequest creation, copy, deduplication, and URL generation."""

    def test_request_creation_1k(self):
        """Create 1k CurveRequests for different curves."""
        curves = [_random_curve(i) for i in range(1000)]
        begin = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2025, 3, 1, tzinfo=dt.timezone.utc)

        t0 = time.perf_counter()
        reqs = [CurveRequest(curve=c, begin=begin, end=end) for c in curves]
        elapsed = time.perf_counter() - t0
        assert len(reqs) == 1000
        assert elapsed < 3.0, f"1k request creation took {elapsed:.2f}s"
        print(f"\n  request_creation_1k: {elapsed:.4f}s")

    def test_request_to_prepared_1k(self):
        """Materialize 1k CurveRequests to PreparedRequests (URL generation)."""
        curves = [_random_curve(i) for i in range(1000)]
        begin = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2025, 2, 1, tzinfo=dt.timezone.utc)
        reqs = [CurveRequest(curve=c, begin=begin, end=end) for c in curves]

        t0 = time.perf_counter()
        prepared = [r.to_request() for r in reqs]
        elapsed = time.perf_counter() - t0
        assert len(prepared) == 1000
        assert elapsed < 5.0, f"1k to_request took {elapsed:.2f}s"
        print(f"\n  to_prepared_1k: {elapsed:.4f}s")

    def test_request_deduplication_1k(self):
        """Deduplicate 1k requests (with ~50% duplicates)."""
        curves = [_random_curve(i % 500) for i in range(1000)]
        begin = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2025, 2, 1, tzinfo=dt.timezone.utc)
        reqs = [CurveRequest(curve=c, begin=begin, end=end) for c in curves]

        t0 = time.perf_counter()
        deduped = list(CurveRequest.deduplicate(reqs))
        elapsed = time.perf_counter() - t0
        assert len(deduped) == 500
        assert elapsed < 5.0, f"deduplication took {elapsed:.2f}s"
        print(f"\n  deduplication_1k (50% dupes): {elapsed:.4f}s, {len(deduped)} unique")

    def test_timeseries_date_range_expansion(self):
        """Expand 100 timeseries requests into monthly chunks (P1M)."""
        curves = [_random_curve(i, curve_type=CurveType.TIMESERIES) for i in range(100)]
        begin = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2025, 7, 1, tzinfo=dt.timezone.utc)
        reqs = [CurveRequest(curve=c, begin=begin, end=end) for c in curves]

        from yggdrasil.data.cast import iter_datetime_ranges

        t0 = time.perf_counter()
        expanded = 0
        for req in reqs:
            for start, end_ in iter_datetime_ranges(req.begin, req.end, interval=req.fetch_interval):
                expanded += 1
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0
        print(f"\n  date_range_expansion: 100 reqs → {expanded} chunks in {elapsed:.4f}s")

    def test_request_copy_performance(self):
        """Benchmark CurveRequest.copy() which is used heavily in fan-out."""
        c = _random_curve(0)
        req = CurveRequest(curve=c)
        t0 = time.perf_counter()
        for i in range(10_000):
            req.copy(
                begin=req.begin + dt.timedelta(hours=i),
                end=req.end + dt.timedelta(hours=i),
            )
        elapsed = time.perf_counter() - t0
        assert elapsed < 10.0
        print(f"\n  request_copy_10k: {elapsed:.4f}s")


# ======================================================================
# Stage 3: Response curation
# ======================================================================


class TestBenchmarkCuration:
    """Benchmark the curation transform (the heaviest CPU stage)."""

    def test_timeseries_curation_small(self):
        """Curate 10 curves x 100 points = 1k rows."""
        client = _mock_curation_client()
        df = _timeseries_response_frame(n_curves=10, n_points_per_curve=100)

        t0 = time.perf_counter()
        result = client.curate(df)
        elapsed = time.perf_counter() - t0
        expected = 10 * 100
        assert result.height == expected, f"expected {expected}, got {result.height}"
        assert elapsed < 5.0
        print(f"\n  timeseries_1k_rows: {elapsed:.4f}s ({result.height} rows)")

    def test_timeseries_curation_medium(self):
        """Curate 50 curves x 200 points = 10k rows."""
        client = _mock_curation_client()
        df = _timeseries_response_frame(n_curves=50, n_points_per_curve=200)

        t0 = time.perf_counter()
        result = client.curate(df)
        elapsed = time.perf_counter() - t0
        expected = 50 * 200
        assert result.height == expected, f"expected {expected}, got {result.height}"
        print(f"\n  timeseries_10k_rows: {elapsed:.4f}s ({result.height} rows)")

    def test_timeseries_curation_large(self):
        """Curate 100 curves x 500 points = 50k rows."""
        client = _mock_curation_client()
        df = _timeseries_response_frame(n_curves=100, n_points_per_curve=500)

        t0 = time.perf_counter()
        result = client.curate(df)
        elapsed = time.perf_counter() - t0
        expected = 100 * 500
        assert result.height == expected, f"expected {expected}, got {result.height}"
        print(f"\n  timeseries_50k_rows: {elapsed:.4f}s ({result.height} rows)")

    def test_instance_curation(self):
        """Curate instance data: 5 curves x 3 instances x 48 points = 720 rows."""
        client = _mock_curation_client()
        df = _instance_response_frame()

        t0 = time.perf_counter()
        result = client.curate(df)
        elapsed = time.perf_counter() - t0
        expected = 5 * 3 * 48
        assert result.height == expected, f"expected {expected}, got {result.height}"
        print(f"\n  instance_{expected}_rows: {elapsed:.4f}s ({result.height} rows)")

    def test_instance_curation_large(self):
        """Curate larger instance data: 20 curves x 10 instances x 96 points."""
        client = _mock_curation_client()
        df = _instance_response_frame(n_curves=20, n_instances_per_curve=10, n_points_per_instance=96)

        t0 = time.perf_counter()
        result = client.curate(df)
        elapsed = time.perf_counter() - t0
        expected = 20 * 10 * 96
        assert result.height == expected, f"expected {expected}, got {result.height}"
        print(f"\n  instance_{expected}_rows: {elapsed:.4f}s ({result.height} rows)")

    def test_ohlc_curation(self):
        """Curate OHLC data: 3 curves x 30 days = 90 rows."""
        client = _mock_curation_client()
        df = _ohlc_response_frame()

        t0 = time.perf_counter()
        result = client.curate(df)
        elapsed = time.perf_counter() - t0
        print(f"\n  ohlc_90_rows: {elapsed:.4f}s ({result.height} rows)")

    def test_curation_repeated_small(self):
        """Simulate repeated curation calls (as in batch processing)."""
        client = _mock_curation_client()
        frames = [
            _timeseries_response_frame(n_curves=5, n_points_per_curve=50)
            for _ in range(20)
        ]

        t0 = time.perf_counter()
        total_rows = 0
        for df in frames:
            result = client.curate(df)
            total_rows += result.height
        elapsed = time.perf_counter() - t0
        expected = 20 * 5 * 50
        assert total_rows == expected, f"expected {expected}, got {total_rows}"
        print(f"\n  repeated_curation_20x{5*50}: {elapsed:.4f}s ({total_rows} total rows)")


# ======================================================================
# Stage 4: Hashing benchmarks
# ======================================================================


class TestBenchmarkHashing:
    """Benchmark xxh3_64 hashing operations."""

    def test_xxh3_scalar_10k(self):
        """Scalar xxh3_64 hashing (used for curve_id)."""
        names = [f"DE Power Curve Name {i} MWh/h H Actual" for i in range(10_000)]
        t0 = time.perf_counter()
        results = [_xxh3_64_signed(n) for n in names]
        elapsed = time.perf_counter() - t0
        assert len(results) == 10_000
        assert elapsed < 1.0
        print(f"\n  xxh3_scalar_10k: {elapsed:.4f}s")

    def test_xxh3_batch_polars_10k(self):
        """Polars batch xxh3 hashing (used for curve_id column)."""
        series = pl.Series("name", [f"curve_{i}" for i in range(10_000)])
        t0 = time.perf_counter()
        result = _xxh3_batch(series)
        elapsed = time.perf_counter() - t0
        assert len(result) == 10_000
        print(f"\n  xxh3_batch_10k: {elapsed:.4f}s")

    def test_xxh3_batch_polars_100k(self):
        """Larger batch hashing."""
        series = pl.Series("name", [f"curve_{i}" for i in range(100_000)])
        t0 = time.perf_counter()
        result = _xxh3_batch(series)
        elapsed = time.perf_counter() - t0
        assert len(result) == 100_000
        print(f"\n  xxh3_batch_100k: {elapsed:.4f}s")

    def test_run_hash_computation_10k(self):
        """Compute run_hash (canonical JSON → xxh3) for 10k rows."""
        base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        df = pl.DataFrame({
            "curve_name": [f"curve_{i}" for i in range(10_000)],
            "scenario_name": [None] * 10_000,
            "instance_created": [base + dt.timedelta(hours=i) for i in range(10_000)],
            "instance_modified": [base + dt.timedelta(hours=i) for i in range(10_000)],
            "instance_tag": ["base"] * 5_000 + [None] * 5_000,
        })
        cols = ["curve_name", "scenario_name", "instance_created", "instance_modified", "instance_tag"]

        t0 = time.perf_counter()
        result = df.with_columns(run_hash=_stable_xxh3_hash_expr(df, cols))
        elapsed = time.perf_counter() - t0
        assert result.height == 10_000
        print(f"\n  run_hash_10k: {elapsed:.4f}s")

    def test_canonical_json_construction_10k(self):
        """Benchmark just the canonical JSON string construction."""
        base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        df = pl.DataFrame({
            "curve_name": [f"curve_{i}" for i in range(10_000)],
            "scenario_name": [None] * 10_000,
            "instance_created": [base + dt.timedelta(hours=i) for i in range(10_000)],
            "instance_modified": [base + dt.timedelta(hours=i) for i in range(10_000)],
            "instance_tag": ["base"] * 5_000 + [None] * 5_000,
        })
        cols = ["curve_name", "scenario_name", "instance_created", "instance_modified", "instance_tag"]

        t0 = time.perf_counter()
        result = df.select(_canonical_struct_json_expr(df, cols).alias("json"))
        elapsed = time.perf_counter() - t0
        assert result.height == 10_000
        print(f"\n  canonical_json_10k: {elapsed:.4f}s")


# ======================================================================
# Stage 5: Data struct assembly & timezone localization
# ======================================================================


class TestBenchmarkDataAssembly:
    """Benchmark make_data and timezone localization."""

    def test_make_data_simple_1k(self):
        """make_data with 1k rows, single timezone, pre-parsed timestamps."""
        base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        df = pl.DataFrame({
            "curve_name": ["curve_a"] * 1000,
            "run_hash": [1] * 1000,
            "resolution_frequency": [3600] * 1000,
            "resolution_timezone": ["UTC"] * 1000,
            "data_d": [(base + dt.timedelta(hours=i)).isoformat() for i in range(1000)],
            "data_v": [float(i) for i in range(1000)],
        })

        t0 = time.perf_counter()
        result = make_data(df)
        elapsed = time.perf_counter() - t0
        assert result.height == 1000
        print(f"\n  make_data_simple_1k: {elapsed:.4f}s")

    def test_make_data_simple_10k(self):
        """make_data with 10k rows, single timezone."""
        base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        df = pl.DataFrame({
            "curve_name": [f"curve_{i % 10}" for i in range(10_000)],
            "run_hash": [i % 10 for i in range(10_000)],
            "resolution_frequency": [3600] * 10_000,
            "resolution_timezone": ["CET"] * 10_000,
            "data_d": [(base + dt.timedelta(hours=i)).isoformat() for i in range(10_000)],
            "data_v": [float(i) for i in range(10_000)],
        })

        t0 = time.perf_counter()
        result = make_data(df)
        elapsed = time.perf_counter() - t0
        assert result.height == 10_000
        print(f"\n  make_data_simple_10k: {elapsed:.4f}s")

    def test_make_data_mixed_timezones(self):
        """make_data with mixed timezones (exercises per-tz localization paths)."""
        df = _mixed_timezone_frame(n_rows=5000)

        t0 = time.perf_counter()
        result = make_data(df)
        elapsed = time.perf_counter() - t0
        assert result.height > 0
        print(f"\n  make_data_mixed_tz_5k: {elapsed:.4f}s ({result.height} rows)")

    def test_localize_by_timezones(self):
        """Benchmark timezone localization for naive timestamps."""
        base = dt.datetime(2025, 1, 1)
        tz_list = ["CET", "UTC", "Europe/Oslo", "Europe/Paris"]
        n = 4000
        df = pl.DataFrame({
            "ts": [(base + dt.timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M:%S") for i in range(n)],
            "tz": [tz_list[i % len(tz_list)] for i in range(n)],
        })

        t0 = time.perf_counter()
        result = localize_by_distinct_timezones(
            df, source_col="ts", timezone_col="tz",
            parsed_col="ts_utc", offset_col="utc_offset",
        )
        elapsed = time.perf_counter() - t0
        assert result.height == n
        print(f"\n  localize_timezones_4k: {elapsed:.4f}s")

    def test_make_data_with_begin_end(self):
        """make_data with both begin and end timestamps (period data)."""
        base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        n = 5000
        df = pl.DataFrame({
            "curve_name": [f"curve_{i % 5}" for i in range(n)],
            "run_hash": [i % 5 for i in range(n)],
            "resolution_frequency": [3600] * n,
            "resolution_timezone": ["UTC"] * n,
            "data_begin": [(base + dt.timedelta(hours=i)).isoformat() for i in range(n)],
            "data_end": [(base + dt.timedelta(hours=i + 1)).isoformat() for i in range(n)],
            "data_v": [float(i) for i in range(n)],
        })

        t0 = time.perf_counter()
        result = make_data(df)
        elapsed = time.perf_counter() - t0
        assert result.height == n
        print(f"\n  make_data_begin_end_5k: {elapsed:.4f}s")


# ======================================================================
# Stage 6: Schema reordering
# ======================================================================


class TestBenchmarkSchemaReorder:
    """Benchmark final schema projection."""

    def test_reorder_1k_rows(self):
        """Reorder 1k rows to final schema."""
        df = pl.DataFrame({
            "curve_name": [f"c_{i}" for i in range(1000)],
            "curve_id": list(range(1000)),
            "curve_type": ["TIMESERIES"] * 1000,
            "curve_data_type": ["ACTUAL"] * 1000,
            "run_hash": list(range(1000)),
            "from_timestamp": [dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)] * 1000,
            "to_timestamp": [dt.datetime(2025, 1, 1, 1, tzinfo=dt.timezone.utc)] * 1000,
            "from_utc_offset": [0] * 1000,
            "to_utc_offset": [0] * 1000,
            "value": [1.0] * 1000,
        })
        t0 = time.perf_counter()
        for _ in range(100):
            reorder_columns(df, schema=FINAL_SCHEMA)
        elapsed = time.perf_counter() - t0
        print(f"\n  reorder_1k_x100: {elapsed:.4f}s ({elapsed/100:.5f}s per call)")

    def test_reorder_10k_rows(self):
        """Reorder 10k rows to final schema."""
        df = pl.DataFrame({
            "curve_name": [f"c_{i}" for i in range(10_000)],
            "curve_id": list(range(10_000)),
            "curve_type": ["TIMESERIES"] * 10_000,
            "curve_data_type": ["ACTUAL"] * 10_000,
            "run_hash": list(range(10_000)),
            "from_timestamp": [dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)] * 10_000,
            "to_timestamp": [dt.datetime(2025, 1, 1, 1, tzinfo=dt.timezone.utc)] * 10_000,
            "from_utc_offset": [0] * 10_000,
            "to_utc_offset": [0] * 10_000,
            "value": [1.0] * 10_000,
        })
        t0 = time.perf_counter()
        for _ in range(10):
            reorder_columns(df, schema=FINAL_SCHEMA)
        elapsed = time.perf_counter() - t0
        print(f"\n  reorder_10k_x10: {elapsed:.4f}s ({elapsed/10:.5f}s per call)")


# ======================================================================
# Stage 7: Batch concatenation & table routing
# ======================================================================


class TestBenchmarkBatchRouting:
    """Benchmark batch grouping by table_name (the routing step before insert)."""

    def test_group_by_table_name(self):
        """Group curated results by destination table."""
        cm = _generate_curvemap(500)
        curve_names = list(cm.keys())[:100]

        n_rows = 10_000
        df = pl.DataFrame({
            "curve_name": [curve_names[i % len(curve_names)] for i in range(n_rows)],
            "curve_id": [cm[curve_names[i % len(curve_names)]].id for i in range(n_rows)],
            "value": [float(i) for i in range(n_rows)],
        })

        t0 = time.perf_counter()
        groups: dict[str, list[str]] = {}
        for n in df["curve_name"].unique().to_list():
            c = cm.get(n)
            if c:
                tb = c.table_name(prefix="curated_")
                groups.setdefault(tb, []).append(n)

        for tb, names in groups.items():
            sub = df.filter(pl.col("curve_name").is_in(names))
        elapsed = time.perf_counter() - t0
        print(f"\n  group_by_table: {len(groups)} tables, {n_rows} rows in {elapsed:.4f}s")

    def test_concat_curated_batches(self):
        """Concatenate multiple curated DataFrames (diagonal_relaxed)."""
        client = _mock_curation_client()
        frames = [
            client.curate(_timeseries_response_frame(n_curves=3, n_points_per_curve=50))
            for _ in range(20)
        ]

        t0 = time.perf_counter()
        combined = pl.concat(frames, how="diagonal_relaxed")
        elapsed = time.perf_counter() - t0
        expected = 20 * 3 * 50
        assert combined.height == expected, f"expected {expected}, got {combined.height}"
        print(f"\n  concat_20_batches: {elapsed:.4f}s ({combined.height} rows)")


# ======================================================================
# Stage 8: End-to-end simulated pipeline
# ======================================================================


class TestBenchmarkEndToEnd:
    """Full pipeline simulation without API or Databricks."""

    def test_e2e_single_category_small(self):
        """Simulate ingesting a small category: 10 curves x 100 points."""
        cm = _generate_curvemap(100)
        client = _mock_curation_client(cm)

        t0 = time.perf_counter()

        # Phase 1: Filter curves by category
        wind_curves = [c for c in cm.values() if "Wind" in c.categories]

        # Phase 2: Generate response frames (simulates fetch)
        response = _timeseries_response_frame(
            n_curves=min(len(wind_curves), 10),
            n_points_per_curve=100,
        )

        # Phase 3: Curate
        curated = client.curate(response)

        # Phase 4: Route by table
        groups: dict[str, list[str]] = {}
        for n in curated["curve_name"].unique().to_list():
            c = cm.get(n)
            if c:
                tb = c.table_name(prefix="curated_")
                groups.setdefault(tb, []).append(n)

        for tb, names in groups.items():
            sub = curated.filter(pl.col("curve_name").is_in(names))

        elapsed = time.perf_counter() - t0
        print(f"\n  e2e_small: {curated.height} rows in {elapsed:.4f}s")

    def test_e2e_mixed_types_medium(self):
        """Simulate ingesting mixed curve types: TS + Instance + OHLC."""
        cm = _generate_curvemap(500)
        client = _mock_curation_client(cm)

        t0 = time.perf_counter()

        ts_response = _timeseries_response_frame(n_curves=30, n_points_per_curve=200)
        inst_response = _instance_response_frame(n_curves=10, n_instances_per_curve=5, n_points_per_instance=48)
        ohlc_response = _ohlc_response_frame(n_curves=5, n_days=60)

        ts_curated = client.curate(ts_response)
        inst_curated = client.curate(inst_response)
        ohlc_curated = client.curate(ohlc_response)

        all_curated = pl.concat([ts_curated, inst_curated, ohlc_curated], how="diagonal_relaxed")

        groups: dict[str, int] = {}
        for n in all_curated["curve_name"].unique().to_list():
            c = cm.get(n)
            if c:
                tb = c.table_name(prefix="curated_")
                sub = all_curated.filter(pl.col("curve_name") == n)
                groups[tb] = groups.get(tb, 0) + sub.height

        elapsed = time.perf_counter() - t0
        print(f"\n  e2e_mixed_medium: {all_curated.height} rows, {len(groups)} tables in {elapsed:.4f}s")

    def test_e2e_large_timeseries(self):
        """Simulate a large timeseries ingestion: 100 curves x 720 points (30 days hourly)."""
        cm = _generate_curvemap(200)
        client = _mock_curation_client(cm)

        t0 = time.perf_counter()

        response = _timeseries_response_frame(n_curves=100, n_points_per_curve=720)
        curated = client.curate(response)

        groups: dict[str, list[str]] = {}
        for n in curated["curve_name"].unique().to_list():
            c = cm.get(n)
            if c:
                tb = c.table_name(prefix="curated_")
                groups.setdefault(tb, []).append(n)

        elapsed = time.perf_counter() - t0
        print(f"\n  e2e_large_ts: {curated.height} rows in {elapsed:.4f}s")

    def test_e2e_repeated_batches(self):
        """Simulate processing 10 sequential batches (as in a real hourly run)."""
        cm = _generate_curvemap(300)
        client = _mock_curation_client(cm)

        t0 = time.perf_counter()
        total_rows = 0
        total_tables: set[str] = set()

        for batch_idx in range(10):
            response = _timeseries_response_frame(
                n_curves=20,
                n_points_per_curve=200,
            )
            curated = client.curate(response)
            total_rows += curated.height

            for n in curated["curve_name"].unique().to_list():
                c = cm.get(n)
                if c:
                    total_tables.add(c.table_name(prefix="curated_"))

        elapsed = time.perf_counter() - t0
        print(f"\n  e2e_10_batches: {total_rows} rows, {len(total_tables)} tables in {elapsed:.4f}s")


# ======================================================================
# Profiling individual bottleneck operations
# ======================================================================


class TestBenchmarkBottlenecks:
    """Isolate and benchmark individual operations suspected as bottlenecks."""

    def test_iso_duration_parsing_10k(self):
        """ISO 8601 duration string parsing via regex."""
        durations = ["PT1H", "PT15M", "P1D", "P1W", "P1M", "P1Y", "P1DT2H30M"]
        df = pl.DataFrame({
            "dur": [durations[i % len(durations)] for i in range(10_000)]
        })
        t0 = time.perf_counter()
        for _ in range(10):
            df.select(iso_duration_to_seconds_expr("dur"))
        elapsed = time.perf_counter() - t0
        print(f"\n  iso_duration_10k_x10: {elapsed:.4f}s")

    def test_unit_parsing_10k(self):
        """Unit string parsing (split numerator/denominator)."""
        units = ["MWh/h", "EUR/MWh", "GW", "MW", "GBP/therm", "USD/bbl"]
        df = pl.DataFrame({
            "unit": [units[i % len(units)] for i in range(10_000)]
        })
        t0 = time.perf_counter()
        for _ in range(10):
            df.select(*parse_unit_columns_expr("unit"))
        elapsed = time.perf_counter() - t0
        print(f"\n  unit_parsing_10k_x10: {elapsed:.4f}s")

    def test_polars_concat_diagonal_relaxed(self):
        """Benchmark polars.concat with diagonal_relaxed (used in batch merging)."""
        client = _mock_curation_client()
        frames = [
            client.curate(_timeseries_response_frame(n_curves=2, n_points_per_curve=50))
            for _ in range(50)
        ]

        t0 = time.perf_counter()
        result = pl.concat(frames, how="diagonal_relaxed")
        elapsed = time.perf_counter() - t0
        expected = 50 * 2 * 50
        assert result.height == expected, f"expected {expected}, got {result.height}"
        print(f"\n  concat_diagonal_50x100: {elapsed:.4f}s ({result.height} rows)")

    def test_polars_filter_is_in(self):
        """Benchmark polars is_in filter (used in table routing)."""
        names = [f"curve_{i}" for i in range(1000)]
        df = pl.DataFrame({
            "curve_name": [names[i % len(names)] for i in range(50_000)],
            "value": [float(i) for i in range(50_000)],
        })
        subset = names[:100]

        t0 = time.perf_counter()
        for _ in range(10):
            df.filter(pl.col("curve_name").is_in(subset))
        elapsed = time.perf_counter() - t0
        print(f"\n  filter_is_in_50k_x10: {elapsed:.4f}s")

    def test_polars_struct_unnest(self):
        """Benchmark struct unnesting (used in resolution, instance, curve parsing)."""
        n = 10_000
        df = pl.DataFrame({
            "resolution": [{"frequency": "PT1H", "timezone": "CET"}] * n,
            "instance": [{"issued": "2025-01-01T00:00:00Z", "tag": "base", "created": "2025-01-01T00:00:00Z", "modified": "2025-01-01T00:00:00Z"}] * n,
        })
        t0 = time.perf_counter()
        for _ in range(10):
            result = df.unnest("resolution", separator="_").unnest("instance", separator="_")
        elapsed = time.perf_counter() - t0
        print(f"\n  struct_unnest_10k_x10: {elapsed:.4f}s")

    def test_curvemap_defaults_join(self):
        """Benchmark the curvemap defaults left-join (null-filling metadata)."""
        cm = _generate_curvemap(100)
        client = _mock_curation_client(cm)

        n = 10_000
        curve_names = list(cm.keys())[:50]
        names_col = [curve_names[i % len(curve_names)] for i in range(n)]

        t0 = time.perf_counter()
        for _ in range(10):
            meta_df = client._curvemap_defaults(curve_names)
            df = pl.DataFrame({"curve_name": names_col, "unit": [None] * n})
            if meta_df.height > 0:
                joined = df.join(meta_df, on="curve_name", how="left")
        elapsed = time.perf_counter() - t0
        print(f"\n  curvemap_defaults_join_10k_x10: {elapsed:.4f}s")

    def test_string_datetime_parsing(self):
        """Benchmark string-to-datetime parsing via polars str.to_datetime."""
        base = dt.datetime(2025, 1, 1)
        n = 10_000
        ts_strings = [(base + dt.timedelta(hours=i)).isoformat() + "+00:00" for i in range(n)]
        df = pl.DataFrame({"ts": ts_strings})

        t0 = time.perf_counter()
        for _ in range(10):
            result = df.with_columns(
                pl.col("ts").str.to_datetime(time_unit="us", strict=False, time_zone="UTC")
            )
        elapsed = time.perf_counter() - t0
        print(f"\n  str_to_datetime_10k_x10: {elapsed:.4f}s")

    def test_data_struct_expr_assembly(self):
        """Benchmark the data struct expression assembly."""
        n = 10_000
        base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        df = pl.DataFrame({
            "from_ts": [base + dt.timedelta(hours=i) for i in range(n)],
            "from_offset": [0] * n,
            "to_ts": [base + dt.timedelta(hours=i + 1) for i in range(n)],
            "to_offset": [0] * n,
            "resolution_frequency": [3600] * n,
            "data_v": [float(i) for i in range(n)],
        })

        t0 = time.perf_counter()
        for _ in range(10):
            result = df.with_columns(
                data=data_struct_expr(
                    from_ts=pl.col("from_ts"),
                    from_offset=pl.col("from_offset"),
                    to_ts=pl.col("to_ts"),
                    to_offset=pl.col("to_offset"),
                    frequency=pl.col("resolution_frequency"),
                    value_col="data_v",
                )
            )
        elapsed = time.perf_counter() - t0
        print(f"\n  data_struct_10k_x10: {elapsed:.4f}s")
