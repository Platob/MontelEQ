"""Unit tests for model classes — Resolution, Access, Subscription, Place, Curve, Instance."""
from __future__ import annotations

import ctypes
import datetime as dt
import time

import pytest
import xxhash
from energyquantified.metadata import CurveType, DataType

from monteleq.model import (
    Access,
    Curve,
    Instance,
    Place,
    Resolution,
    Subscription,
    _safe_name,
    _str_or_none,
    _parse_str_tuple,
    _parse_float_tuple,
    _xxh3_id,
    _coerce_data_type,
    _coerce_curve_type,
)


# ======================================================================
# Helper functions
# ======================================================================


class TestHelpers:
    def test_safe_name_basic(self):
        assert _safe_name("Hello World") == "hello_world"

    def test_safe_name_special_chars(self):
        assert _safe_name("MWh/h") == "mwh_h"

    def test_safe_name_leading_trailing(self):
        assert _safe_name("__test__") == "test"

    def test_safe_name_empty(self):
        assert _safe_name("") == ""

    def test_safe_name_consecutive_specials(self):
        assert _safe_name("a---b///c") == "a_b_c"

    def test_str_or_none_with_none(self):
        assert _str_or_none(None) is None

    def test_str_or_none_with_string(self):
        assert _str_or_none("hello") == "hello"

    def test_str_or_none_with_empty(self):
        assert _str_or_none("") is None

    def test_str_or_none_with_whitespace(self):
        assert _str_or_none("  ") is None

    def test_str_or_none_with_int(self):
        assert _str_or_none(42) == "42"

    def test_parse_str_tuple_none(self):
        assert _parse_str_tuple(None) == ()

    def test_parse_str_tuple_list(self):
        assert _parse_str_tuple(["a", "b"]) == ("a", "b")

    def test_parse_str_tuple_single_string(self):
        assert _parse_str_tuple("single") == ("single",)

    def test_parse_str_tuple_with_none_values(self):
        assert _parse_str_tuple(["a", None, "b"]) == ("a", "b")

    def test_parse_str_tuple_invalid(self):
        with pytest.raises(TypeError):
            _parse_str_tuple(42)

    def test_parse_float_tuple_none(self):
        assert _parse_float_tuple(None) == ()

    def test_parse_float_tuple_list(self):
        assert _parse_float_tuple([1, 2.5]) == (1.0, 2.5)

    def test_parse_float_tuple_with_none_values(self):
        assert _parse_float_tuple([1.0, None, 3.0]) == (1.0, 3.0)

    def test_parse_float_tuple_invalid(self):
        with pytest.raises(TypeError):
            _parse_float_tuple("not_a_list")

    def test_xxh3_id_deterministic(self):
        assert _xxh3_id("test") == _xxh3_id("test")

    def test_xxh3_id_different(self):
        assert _xxh3_id("a") != _xxh3_id("b")

    def test_xxh3_id_matches_xxhash(self):
        name = "DE Spot Power Price EUR/MWh H Actual"
        expected = ctypes.c_int64(xxhash.xxh3_64_intdigest(name.encode("utf-8"))).value
        assert _xxh3_id(name) == expected

    def test_coerce_data_type_none(self):
        assert _coerce_data_type(None) == DataType.NORMAL

    def test_coerce_data_type_enum(self):
        assert _coerce_data_type(DataType.ACTUAL) == DataType.ACTUAL

    def test_coerce_data_type_string(self):
        assert _coerce_data_type("FORECAST") == DataType.FORECAST

    def test_coerce_data_type_lowercase(self):
        assert _coerce_data_type("actual") == DataType.ACTUAL

    def test_coerce_data_type_invalid(self):
        with pytest.raises(ValueError):
            _coerce_data_type("INVALID_TYPE")

    def test_coerce_curve_type_none(self):
        assert _coerce_curve_type(None) == CurveType.TIMESERIES

    def test_coerce_curve_type_enum(self):
        assert _coerce_curve_type(CurveType.OHLC) == CurveType.OHLC

    def test_coerce_curve_type_string(self):
        assert _coerce_curve_type("INSTANCE") == CurveType.INSTANCE

    def test_coerce_curve_type_invalid(self):
        with pytest.raises(ValueError):
            _coerce_curve_type("NOT_A_TYPE")


# ======================================================================
# Resolution
# ======================================================================


class TestResolution:
    def test_defaults(self):
        r = Resolution()
        assert r.frequency is None
        assert r.timezone is None

    def test_parse_none(self):
        assert Resolution.parse(None) == Resolution()

    def test_parse_self(self):
        r = Resolution(frequency="PT1H", timezone="UTC")
        assert Resolution.parse(r) is r

    def test_parse_mapping(self):
        r = Resolution.parse({"frequency": "PT15M", "timezone": "CET"})
        assert r.frequency == "PT15M"
        assert r.timezone == "CET"

    def test_parse_mapping_partial(self):
        r = Resolution.parse({"frequency": "P1D"})
        assert r.frequency == "P1D"
        assert r.timezone is None

    def test_parse_invalid_type(self):
        with pytest.raises(TypeError):
            Resolution.parse(42)

    def test_frequency_none_string_normalized(self):
        r = Resolution(frequency="none")
        assert r.frequency is None

    def test_frequency_empty_string_normalized(self):
        r = Resolution(frequency="")
        assert r.frequency is None

    def test_frequency_whitespace_normalized(self):
        r = Resolution(frequency="  PT1H  ")
        assert r.frequency == "PT1H"

    def test_frozen(self):
        r = Resolution(frequency="PT1H")
        with pytest.raises(AttributeError):
            r.frequency = "PT15M"  # type: ignore[misc]


# ======================================================================
# Access
# ======================================================================


class TestAccess:
    def test_defaults(self):
        a = Access()
        assert a.by is None
        assert a.package is None

    def test_parse_mapping(self):
        a = Access.parse({"by": "area", "package": "Power"})
        assert a.by == "area"
        assert a.package == "Power"

    def test_parse_none(self):
        assert Access.parse(None) == Access()

    def test_parse_self(self):
        a = Access(by="test")
        assert Access.parse(a) is a

    def test_parse_invalid(self):
        with pytest.raises(TypeError):
            Access.parse(42)


# ======================================================================
# Subscription
# ======================================================================


class TestSubscription:
    def test_defaults(self):
        s = Subscription()
        assert s.access is None
        assert s.type is None

    def test_parse_mapping(self):
        s = Subscription.parse({"access": "full", "area": "DE", "type": "paid"})
        assert s.access == "full"
        assert s.area == "DE"
        assert s.type == "paid"

    def test_all_fields(self):
        s = Subscription(
            access="full", area="DE", label="L", package="P", type="paid"
        )
        assert s.label == "L"
        assert s.package == "P"


# ======================================================================
# Place
# ======================================================================


class TestPlace:
    def test_defaults(self):
        p = Place()
        assert p.type is None
        assert p.areas == ()
        assert p.fuels == ()

    def test_parse_mapping(self):
        p = Place.parse({
            "type": "plant",
            "key": "ABC",
            "name": "Test Plant",
            "areas": ["DE", "AT"],
            "fuels": ["gas"],
            "location": [51.0, 9.0],
        })
        assert p.type == "plant"
        assert p.key == "ABC"
        assert p.areas == ("DE", "AT")
        assert p.fuels == ("gas",)
        assert p.location == (51.0, 9.0)

    def test_parse_none(self):
        assert Place.parse(None) == Place()

    def test_parse_self(self):
        p = Place(type="hub")
        assert Place.parse(p) is p

    def test_remit_units(self):
        p = Place.parse({"remit_units": ["MW", "GW"]})
        assert p.remit_units == ("MW", "GW")


# ======================================================================
# Curve
# ======================================================================


class TestCurve:
    def test_defaults(self):
        c = Curve(name="test")
        assert c.name == "test"
        assert c.curve_type == CurveType.TIMESERIES
        assert c.data_type == DataType.NORMAL
        assert c.commodity is None

    def test_id_is_xxh3_of_name(self):
        name = "DE Spot Power Price EUR/MWh H Actual"
        c = Curve(name=name)
        expected = ctypes.c_int64(
            xxhash.xxh3_64(name.encode("utf-8")).intdigest()
        ).value
        assert c.id == expected

    def test_id_is_deterministic(self):
        c1 = Curve(name="same_name")
        c2 = Curve(name="same_name")
        assert c1.id == c2.id

    def test_different_names_different_ids(self):
        c1 = Curve(name="curve_a")
        c2 = Curve(name="curve_b")
        assert c1.id != c2.id

    def test_id_zero_when_empty_name(self):
        c = Curve(name="")
        assert c.id == 0

    def test_explicit_id_preserved(self):
        c = Curve(id=42, name="test")
        assert c.id == 42

    def test_table_name_with_prefix(self):
        c = Curve(
            name="test",
            curve_type=CurveType.TIMESERIES,
            data_type=DataType.ACTUAL,
            categories=("Power",),
        )
        tn = c.table_name(prefix="curated_")
        assert tn == "curated_actual_timeseries_power"

    def test_table_name_no_prefix(self):
        c = Curve(
            name="test",
            curve_type=CurveType.OHLC,
            data_type=DataType.NORMAL,
        )
        tn = c.table_name()
        assert tn == "normal_ohlc"

    def test_table_name_categories_capped_at_two(self):
        c = Curve(
            name="test",
            curve_type=CurveType.TIMESERIES,
            data_type=DataType.ACTUAL,
            categories=("Power", "Wind", "Onshore"),
        )
        tn = c.table_name(prefix="raw_")
        assert tn == "raw_actual_timeseries_power_wind"

    def test_parse_mapping(self):
        c = Curve.parse_mapping({
            "name": "DE Wind MWh/h H Actual",
            "area": "DE",
            "curve_type": "TIMESERIES",
            "data_type": "ACTUAL",
            "commodity": "Power",
            "resolution": {"frequency": "PT1H", "timezone": "CET"},
        })
        assert c.name == "DE Wind MWh/h H Actual"
        assert c.area == "DE"
        assert c.curve_type == CurveType.TIMESERIES
        assert c.data_type == DataType.ACTUAL
        assert c.resolution.frequency == "PT1H"

    def test_parse_mapping_missing_optional_fields(self):
        c = Curve.parse_mapping({
            "name": "test",
            "data_type": "NORMAL",
            "curve_type": "TIMESERIES",
        })
        assert c.name == "test"
        assert c.area is None
        assert c.unit is None
        assert c.categories == ()

    def test_parse_mapping_none_data_type(self):
        c = Curve.parse_mapping({
            "name": "test",
            "curve_type": "TIMESERIES",
        })
        assert c.data_type == DataType.NORMAL

    def test_parse_self(self):
        c = Curve(name="test")
        assert Curve.parse(c) is c

    def test_parse_none_raises(self):
        with pytest.raises(ValueError):
            Curve.parse(None)

    def test_parse_invalid_type(self):
        with pytest.raises(TypeError):
            Curve.parse(42)

    def test_frozen(self):
        c = Curve(name="test")
        with pytest.raises(AttributeError):
            c.name = "other"  # type: ignore[misc]

    def test_tags(self):
        c = Curve(
            name="test",
            area="DE",
            unit="MWh/h",
            commodity="Power",
        )
        tags = c.tags
        assert tags["name"] == "test"
        assert tags["area"] == "DE"
        assert tags["unit"] == "MWh/h"
        assert "area_sink" not in tags

    def test_tags_excludes_none_values(self):
        c = Curve(name="test")
        tags = c.tags
        assert "area" not in tags
        assert "unit" not in tags
        assert "commodity" not in tags

    def test_tags_includes_categories(self):
        c = Curve(name="test", categories=("Power", "Wind"))
        assert c.tags["categories"] == "Power,Wind"

    def test_is_instance(self):
        c = Curve(name="test", curve_type=CurveType.INSTANCE)
        assert c.is_instance is True
        assert c.is_period_instance is False

    def test_is_period_instance(self):
        c = Curve(name="test", curve_type=CurveType.INSTANCE_PERIOD)
        assert c.is_instance is False
        assert c.is_period_instance is True

    def test_is_neither_instance(self):
        c = Curve(name="test", curve_type=CurveType.TIMESERIES)
        assert c.is_instance is False
        assert c.is_period_instance is False


# ======================================================================
# Instance
# ======================================================================


class TestInstance:
    def test_basic(self):
        c = Curve(name="test")
        i = Instance(
            curve=c,
            issued_at=dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc),
            tag="base",
        )
        assert i.curve is c
        assert i.issued_at == dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        assert i.tag == "base"
        assert i.tags == ["base"]

    def test_none_tag(self):
        c = Curve(name="test")
        i = Instance(curve=c, tag=None)
        assert i.tags == []

    def test_issued_at_string_coerced(self):
        c = Curve(name="test")
        i = Instance(curve=c, issued_at="2025-06-15T12:00:00Z")
        assert isinstance(i.issued_at, dt.datetime)
        assert i.issued_at.tzinfo is not None

    def test_none_timestamps(self):
        c = Curve(name="test")
        i = Instance(curve=c)
        assert i.issued_at is None
        assert i.created_at is None
        assert i.modified_at is None

    def test_naive_datetime_gets_utc(self):
        c = Curve(name="test")
        naive = dt.datetime(2025, 1, 1, 0, 0, 0)
        i = Instance(curve=c, issued_at=naive)
        assert i.issued_at.tzinfo == dt.timezone.utc

    def test_frozen(self):
        c = Curve(name="test")
        i = Instance(curve=c, tag="x")
        with pytest.raises(AttributeError):
            i.tag = "y"  # type: ignore[misc]


# ======================================================================
# Benchmarks
# ======================================================================


class TestModelBenchmarks:
    def test_curve_creation_performance(self):
        t0 = time.perf_counter()
        for i in range(10_000):
            Curve(name=f"curve_{i}", data_type=DataType.ACTUAL)
        elapsed = time.perf_counter() - t0
        assert elapsed < 5.0, f"10k curve creations took {elapsed:.2f}s"

    def test_curve_parse_mapping_performance(self):
        mapping = {
            "name": "DE Wind MWh/h H Actual",
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
            Curve.parse_mapping(mapping)
        elapsed = time.perf_counter() - t0
        assert elapsed < 5.0, f"10k parse_mapping calls took {elapsed:.2f}s"

    def test_xxh3_id_performance(self):
        names = [f"curve_name_{i}" for i in range(10_000)]
        t0 = time.perf_counter()
        for name in names:
            _xxh3_id(name)
        elapsed = time.perf_counter() - t0
        assert elapsed < 1.0, f"10k xxh3 hashes took {elapsed:.2f}s"

    def test_table_name_performance(self):
        c = Curve(
            name="test",
            curve_type=CurveType.TIMESERIES,
            data_type=DataType.ACTUAL,
            categories=("Power", "Wind"),
        )
        t0 = time.perf_counter()
        for _ in range(10_000):
            c.table_name(prefix="curated_")
        elapsed = time.perf_counter() - t0
        assert elapsed < 1.0, f"10k table_name calls took {elapsed:.2f}s"
