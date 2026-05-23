"""Unit tests for CurveRequest — construction, fan-out, URL generation, parameters."""
from __future__ import annotations

import datetime as dt
import time

import pytest
from energyquantified.metadata import CurveType, DataType

from monteleq.model import Curve, Resolution
from monteleq.api.request import (
    CurveRequest,
    is_timezone_relevant_frequency,
    _normalize_timezone_param,
    _effective_timezone,
    utc_now_ceil_hour,
)


def _ts_curve(**kwargs) -> Curve:
    defaults = dict(
        name="DE Power MWh/h H Actual",
        curve_type=CurveType.TIMESERIES,
        data_type=DataType.ACTUAL,
        resolution=Resolution(frequency="PT1H", timezone="CET"),
        unit="MWh/h",
    )
    defaults.update(kwargs)
    return Curve(**defaults)


def _instance_curve(**kwargs) -> Curve:
    defaults = dict(
        name="DE Solar Forecast MWh/h H",
        curve_type=CurveType.INSTANCE,
        data_type=DataType.FORECAST,
        resolution=Resolution(frequency="PT1H", timezone="CET"),
        unit="MWh/h",
    )
    defaults.update(kwargs)
    return Curve(**defaults)


def _ohlc_curve(**kwargs) -> Curve:
    defaults = dict(
        name="DE Power OHLC EUR/MWh",
        curve_type=CurveType.OHLC,
        data_type=DataType.OHLC,
        unit="EUR/MWh",
    )
    defaults.update(kwargs)
    return Curve(**defaults)


# ======================================================================
# Helper functions
# ======================================================================


class TestHelpers:
    def test_utc_now_ceil_hour(self):
        result = utc_now_ceil_hour()
        assert result.tzinfo == dt.timezone.utc
        assert result.minute == 0
        assert result.second == 0
        assert result.microsecond == 0
        assert result >= dt.datetime.now(dt.timezone.utc).replace(
            minute=0, second=0, microsecond=0
        )

    def test_is_timezone_relevant_none(self):
        assert is_timezone_relevant_frequency(None) is True

    def test_is_timezone_relevant_hourly(self):
        assert is_timezone_relevant_frequency("PT1H") is True

    def test_is_timezone_relevant_daily(self):
        assert is_timezone_relevant_frequency("P1D") is False

    def test_is_timezone_relevant_yearly(self):
        assert is_timezone_relevant_frequency("P1Y") is False

    def test_normalize_tz_none(self):
        assert _normalize_timezone_param(None) is None

    def test_normalize_tz_empty(self):
        assert _normalize_timezone_param("") is None

    def test_normalize_tz_utc_variants(self):
        assert _normalize_timezone_param("UTC") == "UTC"
        assert _normalize_timezone_param("Z") == "UTC"
        assert _normalize_timezone_param("Etc/UTC") == "UTC"

    def test_normalize_tz_valid_iana(self):
        assert _normalize_timezone_param("Europe/Berlin") == "Europe/Berlin"

    def test_normalize_tz_unknown(self):
        result = _normalize_timezone_param("UnknownTZ")
        assert result == "UnknownTZ"

    def test_effective_timezone_ohlc(self):
        c = _ohlc_curve()
        assert _effective_timezone(c, "PT1H", "CET") is None

    def test_effective_timezone_daily(self):
        c = _ts_curve()
        assert _effective_timezone(c, "P1D", "CET") is None

    def test_effective_timezone_hourly(self):
        c = _ts_curve()
        assert _effective_timezone(c, "PT1H", None) == "UTC"


# ======================================================================
# CurveRequest construction
# ======================================================================


class TestCurveRequestConstruction:
    def test_basic_timeseries(self):
        c = _ts_curve()
        req = CurveRequest(curve=c)
        assert req.curve is c
        assert req.begin is not None
        assert req.end is not None
        assert req.begin < req.end
        assert req.end.tzinfo == dt.timezone.utc

    def test_default_begin_is_14d_before_end(self):
        c = _ts_curve()
        end = dt.datetime(2025, 6, 15, tzinfo=dt.timezone.utc)
        req = CurveRequest(curve=c, end=end)
        expected_begin = end - dt.timedelta(days=14)
        assert req.begin == expected_begin

    def test_explicit_begin_end(self):
        c = _ts_curve()
        begin = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2025, 6, 1, tzinfo=dt.timezone.utc)
        req = CurveRequest(curve=c, begin=begin, end=end)
        assert req.begin == begin
        assert req.end == end

    def test_string_begin_end(self):
        c = _ts_curve()
        req = CurveRequest(curve=c, begin="2025-01-01", end="2025-06-01")
        assert req.begin.year == 2025
        assert req.begin.month == 1
        assert req.end.month == 6

    def test_missing_curve_raises(self):
        with pytest.raises(TypeError):
            CurveRequest(curve="not_a_curve")

    def test_frequency_from_curve(self):
        c = _ts_curve(resolution=Resolution(frequency="PT15M"))
        req = CurveRequest(curve=c)
        assert req.frequency == "PT15M"

    def test_unit_from_curve(self):
        c = _ts_curve(unit="GWh")
        req = CurveRequest(curve=c)
        assert req.unit == "GWh"

    def test_explicit_frequency_override(self):
        c = _ts_curve(resolution=Resolution(frequency="PT1H"))
        req = CurveRequest(curve=c, frequency="PT15M")
        assert req.frequency == "PT15M"

    def test_instance_limit_default(self):
        c = _instance_curve()
        req = CurveRequest(curve=c)
        assert req.limit == 25

    def test_instance_limit_clamped(self):
        c = _instance_curve()
        req = CurveRequest(curve=c, limit=100)
        assert req.limit == 25

    def test_instance_ensemble_limit(self):
        c = _instance_curve()
        req = CurveRequest(curve=c, ensembles=True)
        assert req.limit == 10

    def test_period_instance_limit(self):
        c = Curve(
            name="test",
            curve_type=CurveType.INSTANCE_PERIOD,
            data_type=DataType.REMIT,
        )
        req = CurveRequest(curve=c)
        assert req.limit == 20

    def test_timeseries_no_limit(self):
        c = _ts_curve()
        req = CurveRequest(curve=c)
        assert req.limit is None

    def test_request_tags_string(self):
        c = _ts_curve()
        req = CurveRequest(curve=c, request_tags="base")
        assert req.request_tags == ["base"]

    def test_request_tags_list(self):
        c = _ts_curve()
        req = CurveRequest(curve=c, request_tags=["base", "peak"])
        assert req.request_tags == ["base", "peak"]

    def test_request_tags_empty(self):
        c = _ts_curve()
        req = CurveRequest(curve=c, request_tags=[])
        assert req.request_tags == []

    def test_request_tags_filters_none(self):
        c = _ts_curve()
        req = CurveRequest(curve=c, request_tags=["a", None, "b"])
        assert req.request_tags == ["a", "b"]

    def test_timezone_ohlc_is_none(self):
        c = _ohlc_curve()
        req = CurveRequest(curve=c)
        assert req.timezone is None

    def test_issued_at_coerced(self):
        c = _instance_curve()
        req = CurveRequest(
            curve=c,
            issued_at="2025-06-01T12:00:00Z",
        )
        assert isinstance(req.issued_at, dt.datetime)
        assert req.issued_at.tzinfo == dt.timezone.utc

    def test_issued_at_earliest_default(self):
        c = _instance_curve()
        req = CurveRequest(curve=c)
        diff = req.issued_at_latest - req.issued_at_earliest
        assert diff == dt.timedelta(days=7)


# ======================================================================
# CurveRequest properties
# ======================================================================


class TestCurveRequestProperties:
    def test_endpoint_timeseries(self):
        req = CurveRequest(curve=_ts_curve())
        assert req.endpoint == "timeseries"

    def test_endpoint_instance(self):
        req = CurveRequest(curve=_instance_curve())
        assert req.endpoint == "instances"

    def test_endpoint_instance_ensembles(self):
        req = CurveRequest(curve=_instance_curve(), ensembles=True)
        assert req.endpoint == "ensembles"

    def test_endpoint_period_instance(self):
        c = Curve(name="t", curve_type=CurveType.INSTANCE_PERIOD, data_type=DataType.REMIT)
        req = CurveRequest(curve=c)
        assert req.endpoint == "period-instances"

    def test_endpoint_ohlc(self):
        req = CurveRequest(curve=_ohlc_curve())
        assert req.endpoint == "ohlc"

    def test_endpoint_period(self):
        c = Curve(name="t", curve_type=CurveType.PERIOD, data_type=DataType.NORMAL)
        req = CurveRequest(curve=c)
        assert req.endpoint == "periods"

    def test_endpoint_scenario_timeseries(self):
        c = Curve(name="t", curve_type=CurveType.SCENARIO_TIMESERIES, data_type=DataType.CLIMATE)
        req = CurveRequest(curve=c)
        assert req.endpoint == "timeseries"

    def test_curve_type_property(self):
        c = _ohlc_curve()
        req = CurveRequest(curve=c)
        assert req.curve_type == CurveType.OHLC

    def test_fetch_interval_hourly(self):
        c = _ts_curve(resolution=Resolution(frequency="PT1H"))
        req = CurveRequest(curve=c)
        assert req.fetch_interval == "P1M"

    def test_fetch_interval_daily(self):
        c = _ts_curve(resolution=Resolution(frequency="P1D"))
        req = CurveRequest(curve=c, frequency="P1D")
        assert req.fetch_interval == "P1Y"


# ======================================================================
# CurveRequest.parameters()
# ======================================================================


class TestCurveRequestParameters:
    def test_timeseries_params(self):
        c = _ts_curve()
        req = CurveRequest(
            curve=c,
            begin=dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2025, 6, 1, tzinfo=dt.timezone.utc),
        )
        params = req.parameters()
        assert "begin" in params
        assert "end" in params
        assert "unit" in params
        assert "frequency" in params

    def test_ohlc_date_format(self):
        c = _ohlc_curve()
        req = CurveRequest(
            curve=c,
            begin=dt.datetime(2025, 1, 15, tzinfo=dt.timezone.utc),
            end=dt.datetime(2025, 6, 15, tzinfo=dt.timezone.utc),
        )
        params = req.parameters()
        assert params["begin"] == "2025-01-15"
        assert params["end"] == "2025-06-15"

    def test_instance_with_issued_at_no_range_params(self):
        c = _instance_curve()
        req = CurveRequest(
            curve=c,
            issued_at=dt.datetime(2025, 6, 1, tzinfo=dt.timezone.utc),
            request_tags=["base"],
        )
        params = req.parameters()
        assert "issued-at-earliest" not in params
        assert "issued-at-latest" not in params
        assert "limit" not in params

    def test_instance_without_issued_at_has_range(self):
        c = _instance_curve()
        req = CurveRequest(curve=c)
        params = req.parameters()
        assert "issued-at-earliest" in params
        assert "issued-at-latest" in params
        assert "limit" in params

    def test_timezone_included(self):
        c = _ts_curve()
        req = CurveRequest(curve=c, timezone="CET")
        params = req.parameters()
        assert "timezone" in params


# ======================================================================
# CurveRequest.copy()
# ======================================================================


class TestCurveRequestCopy:
    def test_copy_preserves(self):
        c = _ts_curve()
        req = CurveRequest(curve=c, frequency="PT15M")
        cp = req.copy()
        assert cp.curve is c
        assert cp.frequency == "PT15M"

    def test_copy_overrides(self):
        c = _ts_curve()
        c2 = _ts_curve(name="other")
        req = CurveRequest(curve=c)
        cp = req.copy(curve=c2, frequency="P1D")
        assert cp.curve is c2
        assert cp.frequency == "P1D"

    def test_copy_is_new_object(self):
        c = _ts_curve()
        req = CurveRequest(curve=c)
        cp = req.copy()
        assert req is not cp


# ======================================================================
# CurveRequest.to_request()
# ======================================================================


class TestCurveRequestToRequest:
    def test_to_request_returns_prepared(self):
        c = _ts_curve()
        req = CurveRequest(curve=c)
        prepared = req.to_request()
        assert prepared is not None
        assert prepared.method == "GET"

    def test_url_contains_endpoint(self):
        c = _ts_curve()
        req = CurveRequest(curve=c)
        prepared = req.to_request()
        assert "timeseries" in str(prepared.url)

    def test_url_contains_curve_name(self):
        c = _ts_curve()
        req = CurveRequest(curve=c)
        prepared = req.to_request()
        url_str = str(prepared.url)
        assert "DE" in url_str or "Power" in url_str

    def test_ohlc_url(self):
        c = _ohlc_curve()
        req = CurveRequest(curve=c)
        prepared = req.to_request()
        assert "ohlc" in str(prepared.url)

    def test_instance_with_issued_at_url(self):
        c = _instance_curve()
        req = CurveRequest(
            curve=c,
            issued_at=dt.datetime(2025, 6, 1, 12, 0, tzinfo=dt.timezone.utc),
            request_tags=["base"],
        )
        prepared = req.to_request()
        url_str = str(prepared.url)
        assert "get" in url_str
        assert "base" in url_str

    def test_headers(self):
        c = _ts_curve()
        req = CurveRequest(curve=c)
        prepared = req.to_request()
        assert prepared.headers["Accept"] == "application/json"
        assert prepared.headers["Accept-Encoding"] == "gzip"


# ======================================================================
# CurveRequest.deduplicate()
# ======================================================================


class TestCurveRequestDeduplicate:
    def test_deduplicates_identical(self):
        c = _ts_curve()
        begin = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2025, 2, 1, tzinfo=dt.timezone.utc)
        reqs = [
            CurveRequest(curve=c, begin=begin, end=end),
            CurveRequest(curve=c, begin=begin, end=end),
        ]
        deduplicated = list(CurveRequest.deduplicate(reqs))
        assert len(deduplicated) == 1

    def test_keeps_different(self):
        c1 = _ts_curve(name="curve_a")
        c2 = _ts_curve(name="curve_b")
        begin = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2025, 2, 1, tzinfo=dt.timezone.utc)
        reqs = [
            CurveRequest(curve=c1, begin=begin, end=end),
            CurveRequest(curve=c2, begin=begin, end=end),
        ]
        deduplicated = list(CurveRequest.deduplicate(reqs))
        assert len(deduplicated) == 2


# ======================================================================
# Benchmarks
# ======================================================================


class TestCurveRequestBenchmarks:
    def test_construction_performance(self):
        c = _ts_curve()
        t0 = time.perf_counter()
        for _ in range(10_000):
            CurveRequest(curve=c)
        elapsed = time.perf_counter() - t0
        assert elapsed < 5.0, f"10k CurveRequest constructions took {elapsed:.2f}s"

    def test_to_request_performance(self):
        c = _ts_curve()
        req = CurveRequest(curve=c)
        t0 = time.perf_counter()
        for _ in range(10_000):
            req.to_request()
        elapsed = time.perf_counter() - t0
        assert elapsed < 10.0, f"10k to_request calls took {elapsed:.2f}s"

    def test_copy_performance(self):
        c = _ts_curve()
        req = CurveRequest(curve=c)
        t0 = time.perf_counter()
        for _ in range(10_000):
            req.copy(frequency="PT15M")
        elapsed = time.perf_counter() - t0
        assert elapsed < 5.0, f"10k copy calls took {elapsed:.2f}s"

    def test_parameters_performance(self):
        c = _ts_curve()
        req = CurveRequest(curve=c)
        t0 = time.perf_counter()
        for _ in range(10_000):
            req.parameters()
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0, f"10k parameters calls took {elapsed:.2f}s"
