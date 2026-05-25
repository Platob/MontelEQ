import datetime
import json
from unittest import TestCase

from energyquantified.metadata import CurveType

from monteleq.api.client import APIClient
from monteleq.api.request import CurveRequest


class TestClient(TestCase):
    """Integration tests – require live API credentials via Databricks secrets."""

    @classmethod
    def setUpClass(cls):
        cls.client = APIClient(
            catalog_name="trading_tgp_prd",
            mode="databricks",
        )

    def test_metadata(self):
        curves = self.client.metadata.curves()

        assert isinstance(curves, list)

    def test_events(self):
        for df in self.client.curate_curves(
            self.client.events.requests(
                batch_size=100,
            ), raise_error=False
        ):
            print(df)

    def test_instance(self):
        for response in self.client.curate_curves(
            "*Solar*Photovoltaic*",
            issued_at_earliest=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1),
            begin="2026-01-01",
            raise_error=False
        ):
            assert response is not None

    def test_period_instance(self):
        for response in self.client.fetch_curves(
            self.client.metadata.curves(
                curve_type=CurveType.INSTANCE_PERIOD
            ),
            issued_at_earliest=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1),
            begin="2026-01-01",
        ):
            assert response is not None

    def test_timeseries(self):
        for response in self.client.fetch_curves(
            self.client.metadata.curves(
                curve_type=CurveType.TIMESERIES
            ),
            issued_at_earliest=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1),
            begin="2026-01-01",
        ):
            assert response is not None

    def test_scenario_timeseries(self):
        for response in self.client.fetch_curves(
            self.client.metadata.curves(
                curve_type=CurveType.SCENARIO_TIMESERIES
            ),
            issued_at_earliest=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1),
            begin="2026-01-01",
        ):
            assert response is not None

    def test_period(self):
        for response in self.client.fetch_curves(
            self.client.metadata.curves(
                curve_type=CurveType.PERIOD
            ),
            issued_at_earliest=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1),
            begin="2026-01-01",
        ):
            assert response is not None

    def test_ohlc(self):
        wind_power_curves = [
            'GB Solar Photovoltaic Production MWh/h 15min Forecast',
            'GB Solar Photovoltaic Production MWh/h 30min Actual',
            'GB Solar Photovoltaic Installed MW Capacity'
        ]

        for response in self.client.fetch_curves(
            wind_power_curves,
            issued_at_earliest=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1),
            issued_at_latest=datetime.datetime.now(tz=datetime.timezone.utc),
            begin="2026-01-01",
            end="now"
        ):
            assert response is not None

    def test_uk(self):
        existing_names = json.load(open("existing_curve_names.json"))


        for df in self.client.curate_curves(
            self.client.metadata.curves(existing_names),
            begin="2026-01-01", end="now",
            issued_at_earliest="2026-05-01",
            insert=True,
            local_cache=False,
            raise_error=False
        ):
            assert df is not None
