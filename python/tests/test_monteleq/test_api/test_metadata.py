"""Unit tests and benchmarks for MetadataClient curve filtering and name matching."""
from __future__ import annotations

import time

import polars as pl
import pytest
from energyquantified.metadata import CurveType, DataType

from monteleq.model import Curve, Resolution
from monteleq.api.metadata_client import (
    _build_polars_pattern,
    _glob_to_rust_regex,
    _name_match_indices,
)

AREAS = ["DE", "FR", "AT", "NL", "BE", "ES", "IT", "PL", "CZ", "NO"]
CATEGORY_OPTIONS = [
    ("Production", "Wind"),
    ("Production", "Solar"),
    ("Production", "Nuclear"),
    ("Consumption",),
    ("Exchange",),
    ("Price", "Spot"),
    ("Price", "Balancing"),
    ("Volume",),
    ("Capacity",),
    ("Temperature",),
]
CURVE_TYPES = [CurveType.TIMESERIES, CurveType.INSTANCE, CurveType.OHLC, CurveType.PERIOD]
DATA_TYPES = [DataType.ACTUAL, DataType.FORECAST, DataType.REMIT, DataType.NORMAL]


def _build_catalog(n: int = 10_000) -> tuple[dict[str, Curve], tuple[str, ...], pl.Series]:
    curves: dict[str, Curve] = {}
    for i in range(n):
        area = AREAS[i % len(AREAS)]
        cats = CATEGORY_OPTIONS[i % len(CATEGORY_OPTIONS)]
        ct = CURVE_TYPES[i % len(CURVE_TYPES)]
        dt_ = DATA_TYPES[i % len(DATA_TYPES)]
        name = f'{area} {" ".join(cats)} Curve {i} MWh/h H {dt_.name}'
        curves[name] = Curve(
            name=name,
            area=area,
            categories=cats,
            curve_type=ct,
            data_type=dt_,
            resolution=Resolution(frequency="PT1H", timezone="CET"),
            unit="MWh/h",
            commodity="Power",
        )
    keys = tuple(curves.keys())
    lc = pl.Series("name_lc", keys, dtype=pl.Utf8).str.to_lowercase()
    return curves, keys, lc


# ======================================================================
# Glob → regex conversion
# ======================================================================


class TestGlobToRustRegex:
    def test_simple_glob(self):
        pat = _glob_to_rust_regex("*solar*")
        assert pat == "^.*solar.*$"

    def test_question_mark(self):
        pat = _glob_to_rust_regex("D? Wind*")
        assert pat == "^D.\\ Wind.*$"

    def test_no_wildcards(self):
        pat = _glob_to_rust_regex("exact match")
        assert pat == "^exact\\ match$"

    def test_special_chars_escaped(self):
        pat = _glob_to_rust_regex("EUR/MWh*")
        assert "EUR/MWh" in pat
        assert pat.endswith(".*$")


class TestBuildPolarsPattern:
    def test_substring(self):
        pat = _build_polars_pattern(("wind",))
        assert pat == "(?:wind)"

    def test_glob(self):
        pat = _build_polars_pattern(("*solar*",))
        assert "^" in pat and "$" in pat

    def test_multi(self):
        pat = _build_polars_pattern(("wind", "*solar*"))
        assert "|" in pat

    def test_special_chars(self):
        pat = _build_polars_pattern(("EUR/MWh",))
        assert "EUR/MWh" in pat


# ======================================================================
# Name matching
# ======================================================================


class TestNameMatchIndices:
    @pytest.fixture(scope="class")
    def catalog(self):
        return _build_catalog(1000)

    def test_substring_finds_matches(self, catalog):
        _, _, lc = catalog
        indices = _name_match_indices(("wind",), lc)
        assert len(indices) > 0

    def test_glob_finds_matches(self, catalog):
        _, _, lc = catalog
        indices = _name_match_indices(("*solar*",), lc)
        assert len(indices) > 0

    def test_no_match_returns_empty(self, catalog):
        _, _, lc = catalog
        indices = _name_match_indices(("zzz_nonexistent_zzz",), lc)
        assert indices == []

    def test_multi_pattern_union(self, catalog):
        _, _, lc = catalog
        wind = _name_match_indices(("wind",), lc)
        solar = _name_match_indices(("solar",), lc)
        both = _name_match_indices(("wind", "solar"), lc)
        assert len(both) == len(set(wind) | set(solar))

    def test_exact_match(self, catalog):
        _, keys, lc = catalog
        target = keys[42].lower()
        indices = _name_match_indices((target,), lc)
        assert len(indices) == 1
        assert indices[0] == 42


# ======================================================================
# Curve filtering (single-pass predicate)
# ======================================================================


class TestCurveFiltering:
    @pytest.fixture(scope="class")
    def all_curves(self):
        curves, _, _ = _build_catalog(1000)
        return list(curves.values())

    def test_filter_by_area(self, all_curves):
        s = frozenset(["DE"])
        result = [c for c in all_curves if c.area in s]
        assert len(result) == 100
        assert all(c.area == "DE" for c in result)

    def test_filter_by_data_type(self, all_curves):
        s = frozenset([DataType.ACTUAL])
        result = [c for c in all_curves if c.data_type in s]
        assert len(result) == 250

    def test_filter_by_categories(self, all_curves):
        s = frozenset(["Production"])
        result = [c for c in all_curves if s.issubset(c.categories)]
        assert len(result) == 300

    def test_filter_combined(self, all_curves):
        preds = [
            lambda c: c.data_type == DataType.ACTUAL,
            lambda c: c.area == "DE",
            lambda c: frozenset(["Production"]).issubset(c.categories),
        ]
        result = [c for c in all_curves if all(p(c) for p in preds)]
        assert all(
            c.data_type == DataType.ACTUAL
            and c.area == "DE"
            and "Production" in c.categories
            for c in result
        )

    def test_filter_by_curve_type(self, all_curves):
        s = frozenset([CurveType.OHLC])
        result = [c for c in all_curves if c.curve_type in s]
        assert all(c.curve_type == CurveType.OHLC for c in result)


# ======================================================================
# Benchmarks
# ======================================================================


class TestMetadataBenchmarks:
    @pytest.fixture(scope="class")
    def catalog(self):
        return _build_catalog(10_000)

    def test_parse_mapping_10k(self):
        sample = {
            "name": "DE Wind Power MWh/h H Actual",
            "area": "DE",
            "curve_type": "TIMESERIES",
            "data_type": "ACTUAL",
            "commodity": "Power",
            "unit": "MWh/h",
            "resolution": {"frequency": "PT1H", "timezone": "CET"},
            "categories": ["Production", "Wind"],
            "access": {"by": "area", "package": "Power"},
        }
        t0 = time.perf_counter()
        for _ in range(10_000):
            Curve.parse_mapping(sample)
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"10k parse_mapping took {elapsed:.2f}s"

    def test_name_index_build(self, catalog):
        _, keys, _ = catalog
        t0 = time.perf_counter()
        for _ in range(100):
            pl.Series("x", keys, dtype=pl.Utf8).str.to_lowercase()
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"100x name_index build took {elapsed:.2f}s"

    def test_exact_lookup_100k(self, catalog):
        curves, keys, _ = catalog
        target = keys[5000]
        t0 = time.perf_counter()
        for _ in range(100_000):
            curves.get(target)
        elapsed = time.perf_counter() - t0
        assert elapsed < 0.5, f"100k exact lookups took {elapsed:.2f}s"

    def test_substring_match(self, catalog):
        _, _, lc = catalog
        t0 = time.perf_counter()
        for _ in range(100):
            _name_match_indices(("wind",), lc)
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"100x substring match took {elapsed:.2f}s"

    def test_glob_match(self, catalog):
        _, _, lc = catalog
        t0 = time.perf_counter()
        for _ in range(100):
            _name_match_indices(("*solar*",), lc)
        elapsed = time.perf_counter() - t0
        assert elapsed < 3.0, f"100x glob match took {elapsed:.2f}s"

    def test_multi_pattern_match(self, catalog):
        _, _, lc = catalog
        t0 = time.perf_counter()
        for _ in range(100):
            _name_match_indices(("wind", "solar", "nuclear"), lc)
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"100x multi-pattern match took {elapsed:.2f}s"

    def test_single_pass_3_filter(self, catalog):
        curves, _, _ = catalog
        all_curves = list(curves.values())
        dt_set = frozenset([DataType.ACTUAL])
        area_set = frozenset(["DE"])
        cats_set = frozenset(["Production"])
        preds = [
            lambda c, s=dt_set: c.data_type in s,
            lambda c, s=area_set: c.area in s,
            lambda c, s=cats_set: s.issubset(c.categories),
        ]
        t0 = time.perf_counter()
        for _ in range(100):
            [c for c in all_curves if all(p(c) for p in preds)]
        elapsed = time.perf_counter() - t0
        assert elapsed < 3.0, f"100x 3-filter took {elapsed:.2f}s"

    def test_categories_filter_10k(self, catalog):
        curves, _, _ = catalog
        all_curves = list(curves.values())
        s = frozenset(["Production"])
        t0 = time.perf_counter()
        for _ in range(100):
            [c for c in all_curves if s.issubset(c.categories)]
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"100x categories filter took {elapsed:.2f}s"
