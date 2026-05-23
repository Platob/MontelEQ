"""Unit tests for the Curve, Resolution, Access, Subscription, Place model classes."""
from __future__ import annotations

import ctypes

import pytest
import xxhash
from energyquantified.metadata import CurveType, DataType

from monteleq.model import Access, Curve, Place, Resolution, Subscription


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


# ======================================================================
# Curve
# ======================================================================


class TestCurve:
    def test_defaults(self):
        c = Curve(name="test")
        assert c.name == "test"
        assert c.curve_type == CurveType.TIMESERIES
        assert c.data_type == DataType.NORMAL
        assert c.commodity == "None"

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

    def test_table_name(self):
        c = Curve(
            name="test",
            curve_type=CurveType.TIMESERIES,
            data_type=DataType.ACTUAL,
            commodity="Power",
        )
        assert c.table_name(prefix="curated_") == "curated_timeseries_actual_power"

    def test_table_name_no_commodity(self):
        c = Curve(name="test", commodity="None")
        assert "none" in c.table_name(prefix="raw_")

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

    def test_parse_self(self):
        c = Curve(name="test")
        assert Curve.parse(c) is c

    def test_parse_none_raises(self):
        with pytest.raises(ValueError):
            Curve.parse(None)

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
        assert "area_sink" not in tags  # None values excluded

