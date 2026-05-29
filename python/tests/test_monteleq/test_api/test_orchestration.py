"""Unit tests for the dispatcher/worker orchestration helpers on APIClient.

These exercise the pure-Python helpers (``categories``, ``category_curves``,
``curve_requests``) without constructing a real client or touching Databricks:
the methods only read ``self.metadata.curvemap``, so a lightweight stand-in is
sufficient.
"""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest
from energyquantified.metadata import CurveType, DataType

from monteleq.model import Curve, Resolution
from monteleq.api.client import APIClient, FOREIGN_UNITS


def _curve(name: str, **kwargs) -> Curve:
    defaults = dict(
        resolution=Resolution(frequency="PT1H", timezone="CET"),
        data_type=DataType.ACTUAL,
        curve_type=CurveType.TIMESERIES,
        unit="MWh/h",
    )
    defaults.update(kwargs)
    return Curve(name=name, **defaults)


def _client(curves: list[Curve]) -> SimpleNamespace:
    """A stand-in exposing only ``metadata.curvemap`` for the unbound helpers."""
    return SimpleNamespace(
        metadata=SimpleNamespace(curvemap={c.name: c for c in curves})
    )


@pytest.fixture
def curves() -> list[Curve]:
    return [
        _curve("DE Wind Power MWh/h H Forecast", categories=("Wind", "Power"),
               data_type=DataType.FORECAST, curve_type=CurveType.INSTANCE),
        _curve("DE Power Actual MWh/h", categories=("Power",)),
        _curve("GB Power Price GBP/MWh", categories=("Price",), unit="GBP/MWh"),
        _curve("US Gas Price USD/MMBtu", categories=("Price",), unit="USD/MMBtu",
               data_type=DataType.ACTUAL),
    ]


class TestCategories:
    def test_sorted_and_distinct(self, curves):
        cats = APIClient.categories(_client(curves))
        assert cats == sorted(set(cats))
        # Both "Price" curves collapse into one actual_timeseries_price category.
        assert "actual_timeseries_price" in cats
        assert "forecast_instance_wind_power" in cats

    def test_empty_catalog(self):
        assert APIClient.categories(_client([])) == []


class TestCategoryCurves:
    def test_filters_by_table_name(self, curves):
        wind = curves[0]
        result = APIClient.category_curves(_client(curves), wind.table_name())
        assert [c.name for c in result] == [wind.name]

    def test_groups_share_a_category(self, curves):
        # The two Price curves route to the same table category.
        cat = curves[2].table_name()
        names = {c.name for c in APIClient.category_curves(_client(curves), cat)}
        assert names == {"GB Power Price GBP/MWh", "US Gas Price USD/MMBtu"}

    def test_unknown_category(self, curves):
        assert APIClient.category_curves(_client(curves), "does_not_exist") == []


class TestCurveRequests:
    def _window(self):
        return (
            dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc),
            dt.datetime(2025, 1, 2, tzinfo=dt.timezone.utc),
        )

    def test_plain_timeseries_single_request(self, curves):
        begin, end = self._window()
        reqs = APIClient.curve_requests(
            _client(curves), [curves[1]], begin=begin, end=end, raise_error=False
        )
        assert len(reqs) == 1
        r = reqs[0]
        assert r.begin == begin and r.end == end
        assert r.issued_at_earliest == begin
        assert r.ensembles is False

    def test_instance_adds_ensembles(self, curves):
        begin, end = self._window()
        reqs = APIClient.curve_requests(
            _client(curves), [curves[0]], begin=begin, end=end, raise_error=False
        )
        assert len(reqs) == 2
        assert sorted(r.ensembles for r in reqs) == [False, True]

    @pytest.mark.parametrize("idx,foreign,eur", [
        (2, "GBP/MWh", "EUR/MWh"),
        (3, "USD/MMBtu", "EUR/MMBtu"),
    ])
    def test_foreign_unit_adds_eur_variant(self, curves, idx, foreign, eur):
        begin, end = self._window()
        reqs = APIClient.curve_requests(
            _client(curves), [curves[idx]], begin=begin, end=end, raise_error=False
        )
        units = sorted(r.unit for r in reqs)
        assert units == sorted([foreign, eur])

    def test_full_expansion_count(self, curves):
        # 1 plain TS + (instance: base+ensembles) + (GBP: base+EUR) + (USD: base+EUR)
        begin, end = self._window()
        reqs = APIClient.curve_requests(
            _client(curves), curves, begin=begin, end=end, raise_error=False
        )
        assert len(reqs) == 1 + 2 + 2 + 2

    def test_foreign_units_constant(self):
        assert "GBP" in FOREIGN_UNITS and "USD" in FOREIGN_UNITS
