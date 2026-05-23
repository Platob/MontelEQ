"""Unit tests and benchmarks for MetadataClient curve filtering and name matching.

Benchmarks use a realistic 31k-curve catalog built from eq_curves.csv.
"""
from __future__ import annotations

import ast
import csv
import time
from pathlib import Path

import polars as pl
import pytest
from energyquantified.metadata import CurveType, DataType

from monteleq.model import Curve, Resolution
from monteleq.api.metadata_client import (
    _build_polars_pattern,
    _glob_to_rust_regex,
    _name_match_indices,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
EQ_CURVES_CSV = PROJECT_ROOT / "eq_curves.csv"


def _build_synthetic_catalog(n: int = 10_000) -> dict[str, Curve]:
    areas = ["DE", "FR", "AT", "NL", "BE", "ES", "IT", "PL", "CZ", "NO"]
    cats_options = [
        ("Production", "Wind"), ("Production", "Solar"), ("Consumption",),
        ("Exchange",), ("Price", "Spot"), ("Volume",), ("Capacity",),
    ]
    curve_types = [CurveType.TIMESERIES, CurveType.INSTANCE, CurveType.OHLC]
    data_types = [DataType.ACTUAL, DataType.FORECAST, DataType.REMIT]
    curves: dict[str, Curve] = {}
    for i in range(n):
        area = areas[i % len(areas)]
        cats = cats_options[i % len(cats_options)]
        ct = curve_types[i % len(curve_types)]
        dt_ = data_types[i % len(data_types)]
        name = f'{area} {" ".join(cats)} Curve {i} MWh/h H {dt_.name}'
        curves[name] = Curve(
            name=name, area=area, categories=cats,
            curve_type=ct, data_type=dt_,
            resolution=Resolution(frequency="PT1H", timezone="CET"),
            unit="MWh/h", commodity="Power",
        )
    return curves


def _load_real_catalog() -> dict[str, Curve]:
    """Load the 31k-curve catalog from eq_curves.csv."""
    curves: dict[str, Curve] = {}
    with open(EQ_CURVES_CSV) as f:
        for row in csv.DictReader(f):
            name = row["name"]
            try:
                cats = ast.literal_eval(row["categories"])
            except (ValueError, SyntaxError):
                cats = ()
            try:
                res = ast.literal_eval(row["resolution"])
            except (ValueError, SyntaxError):
                res = {}
            dt_str = row.get("data_type", "NORMAL") or "NORMAL"
            ct_str = row.get("curve_type", "TIMESERIES") or "TIMESERIES"
            try:
                data_type = DataType[dt_str]
            except KeyError:
                data_type = DataType.NORMAL
            try:
                curve_type = CurveType[ct_str]
            except KeyError:
                curve_type = CurveType.TIMESERIES
            curves[name] = Curve(
                name=name,
                area=row.get("area") or None,
                area_sink=row.get("area_sink") or None,
                categories=tuple(cats) if isinstance(cats, (list, tuple)) else (),
                resolution=Resolution(
                    frequency=res.get("frequency"),
                    timezone=res.get("timezone"),
                ),
                unit=row.get("unit") or None,
                denominator=row.get("denominator") or None,
                source=row.get("source") or None,
                data_type=data_type,
                curve_type=curve_type,
                commodity=row.get("commodity") or None,
            )
    return curves


def _name_index(curves: dict[str, Curve]) -> tuple[tuple[str, ...], pl.Series]:
    keys = tuple(curves.keys())
    lc = pl.Series("name_lc", keys, dtype=pl.Utf8).str.to_lowercase()
    return keys, lc


# ======================================================================
# Glob → regex
# ======================================================================


class TestGlobToRustRegex:
    def test_simple_glob(self):
        assert _glob_to_rust_regex("*solar*") == "^.*solar.*$"

    def test_question_mark(self):
        pat = _glob_to_rust_regex("D? Wind*")
        assert pat == "^D.\\ Wind.*$"

    def test_no_wildcards(self):
        assert _glob_to_rust_regex("exact match") == "^exact\\ match$"

    def test_special_chars(self):
        pat = _glob_to_rust_regex("EUR/MWh*")
        assert "EUR/MWh" in pat and pat.endswith(".*$")


class TestBuildPolarsPattern:
    def test_substring(self):
        assert _build_polars_pattern(("wind",)) == "(?:wind)"

    def test_glob(self):
        pat = _build_polars_pattern(("*solar*",))
        assert "^" in pat and "$" in pat

    def test_multi(self):
        assert "|" in _build_polars_pattern(("wind", "*solar*"))

    def test_special_chars(self):
        assert "EUR/MWh" in _build_polars_pattern(("EUR/MWh",))


# ======================================================================
# Name matching
# ======================================================================


class TestNameMatchIndices:
    @pytest.fixture(scope="class")
    def idx(self):
        return _name_index(_build_synthetic_catalog(1000))

    def test_substring(self, idx):
        assert len(_name_match_indices(("wind",), idx[1])) > 0

    def test_glob(self, idx):
        assert len(_name_match_indices(("*solar*",), idx[1])) > 0

    def test_no_match(self, idx):
        assert _name_match_indices(("zzz_nonexistent",), idx[1]) == []

    def test_multi_is_union(self, idx):
        _, lc = idx
        w = set(_name_match_indices(("wind",), lc))
        s = set(_name_match_indices(("solar",), lc))
        both = set(_name_match_indices(("wind", "solar"), lc))
        assert both == w | s

    def test_exact(self, idx):
        keys, lc = idx
        target = keys[42].lower()
        r = _name_match_indices((target,), lc)
        assert r == [42]


# ======================================================================
# Filtering
# ======================================================================


class TestCurveFiltering:
    @pytest.fixture(scope="class")
    def curves(self):
        return list(_build_synthetic_catalog(1000).values())

    def test_filter_area(self, curves):
        s = frozenset(["DE"])
        r = [c for c in curves if c.area in s]
        assert len(r) == 100

    def test_filter_data_type(self, curves):
        s = frozenset([DataType.ACTUAL])
        r = [c for c in curves if c.data_type in s]
        assert len(r) > 0 and all(c.data_type == DataType.ACTUAL for c in r)

    def test_filter_categories(self, curves):
        s = frozenset(["Production"])
        r = [c for c in curves if s.issubset(c.categories)]
        assert len(r) > 0 and all("Production" in c.categories for c in r)

    def test_filter_combined(self, curves):
        r = [
            c for c in curves
            if c.data_type == DataType.ACTUAL
            and c.area == "DE"
            and "Production" in c.categories
        ]
        assert all(c.area == "DE" for c in r)

    def test_filter_curve_type(self, curves):
        s = frozenset([CurveType.OHLC])
        r = [c for c in curves if c.curve_type in s]
        assert all(c.curve_type == CurveType.OHLC for c in r)


# ======================================================================
# Pipeline
# ======================================================================


class TestPipelineCategories:
    def test_static_categories_sorted(self):
        from monteleq.pipeline import CATEGORIES
        assert CATEGORIES == sorted(CATEGORIES)

    def test_static_categories_count(self):
        from monteleq.pipeline import CATEGORIES
        assert len(CATEGORIES) == 39

    def test_plan_categories_fallback(self):
        from monteleq.pipeline import plan_categories
        cats = plan_categories()
        assert len(cats) > 0
        assert cats == sorted(cats)


# ======================================================================
# Benchmarks — synthetic 10k catalog
# ======================================================================


class TestSyntheticBenchmarks:
    @pytest.fixture(scope="class")
    def catalog(self):
        curves = _build_synthetic_catalog(10_000)
        keys, lc = _name_index(curves)
        return curves, keys, lc

    def test_parse_mapping_10k(self):
        sample = {
            "name": "DE Wind Power MWh/h H Actual", "area": "DE",
            "curve_type": "TIMESERIES", "data_type": "ACTUAL",
            "commodity": "Power", "unit": "MWh/h",
            "resolution": {"frequency": "PT1H", "timezone": "CET"},
            "categories": ["Production", "Wind"],
            "access": {"by": "area", "package": "Power"},
        }
        t0 = time.perf_counter()
        for _ in range(10_000):
            Curve.parse_mapping(sample)
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"10k parse_mapping: {elapsed:.2f}s"

    def test_name_index_build(self, catalog):
        _, keys, _ = catalog
        t0 = time.perf_counter()
        for _ in range(100):
            pl.Series("x", keys, dtype=pl.Utf8).str.to_lowercase()
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"100x name_index build: {elapsed:.2f}s"

    def test_exact_lookup(self, catalog):
        curves, keys, _ = catalog
        target = keys[5000]
        t0 = time.perf_counter()
        for _ in range(100_000):
            curves.get(target)
        elapsed = time.perf_counter() - t0
        assert elapsed < 0.5, f"100k exact lookups: {elapsed:.2f}s"

    def test_substring_match(self, catalog):
        _, _, lc = catalog
        t0 = time.perf_counter()
        for _ in range(100):
            _name_match_indices(("wind",), lc)
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"100x substring: {elapsed:.2f}s"

    def test_glob_match(self, catalog):
        _, _, lc = catalog
        t0 = time.perf_counter()
        for _ in range(100):
            _name_match_indices(("*solar*",), lc)
        elapsed = time.perf_counter() - t0
        assert elapsed < 3.0, f"100x glob: {elapsed:.2f}s"

    def test_multi_pattern(self, catalog):
        _, _, lc = catalog
        t0 = time.perf_counter()
        for _ in range(100):
            _name_match_indices(("wind", "solar", "nuclear"), lc)
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"100x multi-pattern: {elapsed:.2f}s"

    def test_single_pass_filter(self, catalog):
        curves, _, _ = catalog
        all_c = list(curves.values())
        dt_s = frozenset([DataType.ACTUAL])
        area_s = frozenset(["DE"])
        cats_s = frozenset(["Production"])
        preds = [
            lambda c, s=dt_s: c.data_type in s,
            lambda c, s=area_s: c.area in s,
            lambda c, s=cats_s: s.issubset(c.categories),
        ]
        t0 = time.perf_counter()
        for _ in range(100):
            [c for c in all_c if all(p(c) for p in preds)]
        elapsed = time.perf_counter() - t0
        assert elapsed < 3.0, f"100x 3-filter: {elapsed:.2f}s"


# ======================================================================
# Benchmarks — real 31k catalog from eq_curves.csv
# ======================================================================


@pytest.mark.skipif(not EQ_CURVES_CSV.exists(), reason="eq_curves.csv not present")
class TestRealCatalogBenchmarks:
    @pytest.fixture(scope="class")
    def catalog(self):
        curves = _load_real_catalog()
        keys, lc = _name_index(curves)
        return curves, keys, lc

    def test_catalog_size(self, catalog):
        curves, _, _ = catalog
        assert len(curves) > 30_000

    def test_load_catalog(self):
        t0 = time.perf_counter()
        curves = _load_real_catalog()
        elapsed = time.perf_counter() - t0
        assert elapsed < 5.0, f"Load 31k catalog: {elapsed:.2f}s"
        assert len(curves) > 30_000

    def test_name_index_build_31k(self, catalog):
        _, keys, _ = catalog
        t0 = time.perf_counter()
        for _ in range(10):
            pl.Series("x", keys, dtype=pl.Utf8).str.to_lowercase()
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"10x name_index build (31k): {elapsed:.2f}s"

    def test_substring_match_31k(self, catalog):
        _, _, lc = catalog
        t0 = time.perf_counter()
        for _ in range(100):
            _name_match_indices(("wind",), lc)
        elapsed = time.perf_counter() - t0
        n = len(_name_match_indices(("wind",), lc))
        assert elapsed < 3.0, f"100x substring 'wind' ({n} hits, 31k): {elapsed:.2f}s"

    def test_glob_match_31k(self, catalog):
        _, _, lc = catalog
        t0 = time.perf_counter()
        for _ in range(100):
            _name_match_indices(("*solar*photovoltaic*",), lc)
        elapsed = time.perf_counter() - t0
        n = len(_name_match_indices(("*solar*photovoltaic*",), lc))
        assert elapsed < 3.0, f"100x glob (31k, {n} hits): {elapsed:.2f}s"

    def test_multi_pattern_31k(self, catalog):
        _, _, lc = catalog
        t0 = time.perf_counter()
        for _ in range(100):
            _name_match_indices(("wind", "solar", "hydro", "nuclear", "gas"), lc)
        elapsed = time.perf_counter() - t0
        n = len(_name_match_indices(("wind", "solar", "hydro", "nuclear", "gas"), lc))
        assert elapsed < 3.0, f"100x 5-pattern (31k, {n} hits): {elapsed:.2f}s"

    def test_filter_area_31k(self, catalog):
        curves, _, _ = catalog
        all_c = list(curves.values())
        s = frozenset(["DE"])
        t0 = time.perf_counter()
        for _ in range(100):
            [c for c in all_c if c.area in s]
        elapsed = time.perf_counter() - t0
        n = len([c for c in all_c if c.area in s])
        assert elapsed < 3.0, f"100x area=DE (31k, {n} hits): {elapsed:.2f}s"

    def test_filter_category_31k(self, catalog):
        curves, _, _ = catalog
        all_c = list(curves.values())
        s = frozenset(["Battery"])
        t0 = time.perf_counter()
        for _ in range(100):
            [c for c in all_c if s.issubset(c.categories)]
        elapsed = time.perf_counter() - t0
        n = len([c for c in all_c if s.issubset(c.categories)])
        assert elapsed < 3.0, f"100x category=Battery (31k, {n} hits): {elapsed:.2f}s"

    def test_combined_filter_31k(self, catalog):
        curves, _, _ = catalog
        all_c = list(curves.values())
        preds = [
            lambda c: c.data_type == DataType.ACTUAL,
            lambda c: c.area == "DE",
            lambda c: c.curve_type == CurveType.TIMESERIES,
        ]
        t0 = time.perf_counter()
        for _ in range(100):
            [c for c in all_c if all(p(c) for p in preds)]
        elapsed = time.perf_counter() - t0
        n = len([c for c in all_c if all(p(c) for p in preds)])
        assert elapsed < 5.0, f"100x 3-filter (31k, {n} hits): {elapsed:.2f}s"

    def test_unique_categories_31k(self, catalog):
        curves, _, _ = catalog
        t0 = time.perf_counter()
        for _ in range(100):
            cats = set()
            for c in curves.values():
                if c.categories:
                    cats.add(c.categories[0])
        elapsed = time.perf_counter() - t0
        n = len(cats)
        assert elapsed < 2.0, f"100x unique categories (31k, {n} cats): {elapsed:.2f}s"
        assert n == 39

    def test_table_name_31k(self, catalog):
        curves, _, _ = catalog
        all_c = list(curves.values())
        t0 = time.perf_counter()
        for c in all_c:
            c.table_name(prefix="curated_")
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"table_name x{len(all_c)}: {elapsed:.2f}s"

    def test_group_by_table_31k(self, catalog):
        curves, _, _ = catalog
        all_c = list(curves.values())
        t0 = time.perf_counter()
        for _ in range(10):
            groups: dict[str, list[str]] = {}
            for c in all_c:
                tb = c.table_name(prefix="curated_")
                groups.setdefault(tb, []).append(c.name)
        elapsed = time.perf_counter() - t0
        n = len(groups)
        assert elapsed < 5.0, f"10x group_by_table (31k, {n} tables): {elapsed:.2f}s"
