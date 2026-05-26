"""BaseClient – low-level HTTP session with auth, Databricks integration, and request helpers."""
from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from yggdrasil.databricks.sql.engine import SQLEngine

from energyquantified import EnergyQuantified
from yggdrasil.enums import Mode
from yggdrasil.databricks import DatabricksClient
from yggdrasil.databricks.table import Table
from yggdrasil.environ import PyEnv
from yggdrasil.http_ import HTTPSession
from yggdrasil.http_.session import Authorization, WaitingConfig
from yggdrasil.io import URL
from yggdrasil.io.send_config import CacheConfig, SendConfig

from monteleq.model import Curve

__all__ = ["BaseClient"]

Headers = dict[str, str] | None


class BaseClient(HTTPSession):
    """Low-level HTTP session wired to Databricks / EnergyQuantified auth."""

    def __init__(
        self,
        base_url: URL | str | None = None,
        *,
        catalog_name: str | None = "trading_tgp_dev",
        schema_name: str | None = "src_monteleq",
        mode: str | None = None,
        databricks: Optional[DatabricksClient] = None,
        verify: bool = True,
        pool_maxsize: int = 10,
        headers: Headers = None,
        waiting: WaitingConfig | None = None,
        auth: Authorization | None = None,
        **kwargs,
    ) -> None:
        if waiting is not None:
            kwargs["waiting"] = waiting
        if auth is not None:
            kwargs["auth"] = auth
        super().__init__(
            base_url=base_url or URL.from_str("https://app.energyquantified.com/api/"),
            verify=verify,
            pool_maxsize=pool_maxsize,
            headers=headers,
            **kwargs,
        )
        self.catalog_name = catalog_name or "trading_tgp_dev"
        self.schema_name = schema_name or "src_monteleq"
        self.mode = mode
        self._databricks = databricks
        self._eqclient = None
        self._auto_init()

    def _auto_init(self) -> None:
        if PyEnv.in_databricks():
            self._databricks = DatabricksClient.current()
        elif self._databricks is None and not self.headers.get("X-API-Key"):
            host = (
                "https://dbc-82edd6f4-1e97.cloud.databricks.com/"
                if self.catalog_name and self.catalog_name.endswith("_prd")
                else "https://dbc-0150e9a2-ae64.cloud.databricks.com/"
            )
            self._databricks = DatabricksClient(host=host)

        if not self.mode:
            self.mode = "databricks+api"

        if not self.headers.get("X-API-Key"):
            try:
                self.headers["X-API-Key"] = self.databricks.secrets["monteleq"]["api_key"].svalue()
                self.mode = "databricks+api"
            except Exception:
                self.mode = "databricks"

    def __getstate__(self) -> dict:
        state = super().__getstate__()
        state.pop("_eqclient", None)
        return state

    def __setstate__(self, state: dict) -> None:
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
    def sql(self) -> "SQLEngine":
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

    def send_config(
        self,
        curve: Curve,
        upsert: bool,
    ) -> SendConfig:
        mode = Mode.UPSERT if upsert else Mode.APPEND
        return SendConfig(
            local_cache=CacheConfig(
                received_ttl=dt.timedelta(days=2),
                mode=mode,
            ),
            remote_cache=CacheConfig(
                tabular=self.check_cache_param(cache=None, curve=curve, prefix="raw_"),
                mode=mode,
            ),
        )
