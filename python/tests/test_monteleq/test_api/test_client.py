import datetime
import json
import logging
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
        )

    def test_metadata(self):
        curves = self.client.metadata.curves()

        assert isinstance(curves, list)

    def test_events(self):
        for df in self.client.curate_curves(
            self.client.events.requests(
                batch_size=100,
            ), raise_error=False,
        ):
            print(df)

    @staticmethod
    def _make_logger(category):
        logger = logging.getLogger(f"test_instance.{category}")
        handler = logging.FileHandler(f"{category}.log", mode="w")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger, handler

    @staticmethod
    def _curve_names(client, category):
        return client.metadata.metadata_df().sql(
            f"select * from self where table_category = '{category}'"
        )["curve_name"].unique().to_list()

    @staticmethod
    def _curate_one(client, name, logger):
        logger.info("curating %s", name)
        for response in client.curate_curves(
            [name],
            issued_at_earliest="2019-01-01",
            issued_at_latest="2025-05-01",
            begin="2019-01-01",
            end="2025-05-01",
            raise_error=False,
        ):
            assert response is not None
            logger.info("ok: %s", response)

    def _run_category(self, category):
        logger, handler = self._make_logger(category)
        try:
            names = self._curve_names(self.client, category)
            logger.info("resolved %d curves for %s", len(names), category)
            for name in names:
                self._curate_one(self.client, name, logger)
        finally:
            logger.removeHandler(handler)
            handler.close()

    def test_instance_forecast_solar_photovoltaic(self):
        self._run_category("forecast_instance_solar_photovoltaic")

    def test_instance_forecast_wind_power(self):
        self._run_category("forecast_instance_wind_power")

    def test_instance_forecast_residual_load(self):
        self._run_category("forecast_instance_residual_load")

    def test_instance_forecast_hydro_run_of_river(self):
        self._run_category("forecast_instance_hydro_run_of_river")

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
