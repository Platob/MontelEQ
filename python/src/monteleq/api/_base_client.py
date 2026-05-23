"""BaseClient – low-level HTTP session with auth, Databricks integration, and request helpers."""
from __future__ import annotations

import datetime as dt
from typing import Optional

from energyquantified import EnergyQuantified
from yggdrasil.data.enums import Mode
from yggdrasil.databricks import DatabricksClient
from yggdrasil.databricks.table import Table
from yggdrasil.environ import PyEnv
from yggdrasil.io import URL
from yggdrasil.io.http_ import HTTPSession
from yggdrasil.io.send_config import CacheConfig

from monteleq.model import Curve

__all__ = ["BaseClient"]


class BaseClient(HTTPSession):
    """
    Low-level HTTP session wired to Databricks / EnergyQuantified auth.

    Holds shared infrastructure for all sub-clients:
    - auth/bootstrap
    - Databricks, EQ client, and SQL accessors
    - cache normalization
    - parameter normalization
    - low-level curve request preparation
    """

    def __init__(
        self,
        base_url: URL | str | None = None,
        *,
        catalog_name: str | None = "trading_tgp_dev",
        schema_name: str | None = "src_monteleq",
        mode: str | None = None,
        databricks: Optional[DatabricksClient] = None,
        **kwargs: Optional[dict]
    ):
        super().__init__(
            base_url=base_url or URL.from_str("https://app.energyquantified.com/api/"),
            **kwargs
        )
        self.catalog_name = catalog_name or "trading_tgp_dev"
        self.schema_name = schema_name or "src_monteleq"
        self.mode = mode
        self._databricks = databricks
        self._eqclient = None
        self._auto_init()

    def _auto_init(self):
        if PyEnv.in_databricks():
            self._databricks = DatabricksClient.current()
        elif self._databricks is None and not self.x_api_key:
            host = (
                "https://dbc-82edd6f4-1e97.cloud.databricks.com"
                if self.catalog_name and self.catalog_name.endswith("_prd")
                else "https://dbc-0150e9a2-ae64.cloud.databricks.com"
            )
            self._databricks = DatabricksClient(host=host)

        if not self.mode:
            self.mode = "databricks+api"

        if not self.x_api_key:
            try:
                self.x_api_key = self.databricks.secrets["monteleq"]["api_key"].svalue()
                self.mode = "databricks+api"
            except Exception:
                self.mode = "databricks"

    def __getstate__(self):
        state = super().__getstate__()
        state.pop("_eqclient", None)
        return state

    def __setstate__(self, state):
        state["_eqclient"] = None
        super().__setstate__(state)
        self._auto_init()

    @property
    def databricks(self) -> DatabricksClient:
        if self._databricks is None:
            self._databricks = DatabricksClient.current()
        return self._databricks

    @property
    def eqclient(self) -> EnergyQuantified:
        if self._eqclient is None:
            self._eqclient = self.new_eqclient()
        return self._eqclient

    def new_eqclient(self) -> EnergyQuantified:
        return EnergyQuantified(api_key=self.x_api_key, ssl_verify=False)

    @property
    def sql(self):
        return self.databricks.sql(
            catalog_name=self.catalog_name,
            schema_name=self.schema_name,
        )

    def check_cache_param(
        self,
        cache: Table | bool | None,
        table_name: str | None = None,
        curve: Curve | None = None,
        prefix: str | None = None,
    ) -> Table | None:
        if cache is None:
            cache = "databricks" in self.mode

        if table_name is None and curve is not None:
            table_name = curve.table_name(prefix=prefix)

        if isinstance(cache, bool):
            return self.sql.table(table_name=table_name) if cache else None

        return cache

    def cache_configs(
        self,
        curve: Curve,
        upsert: bool
    ):
        local_cache = CacheConfig(
            received_ttl=dt.timedelta(days=2),
            mode=Mode.UPSERT if upsert else Mode.APPEND,
        )
        remote_cache = CacheConfig(
            tabular=self.check_cache_param(cache=None, curve=curve, prefix="raw_"),
            mode=Mode.UPSERT if upsert else Mode.APPEND,
        )

        return local_cache, remote_cache
