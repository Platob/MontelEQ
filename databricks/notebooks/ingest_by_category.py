# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — Ingest by Category
# MAGIC
# MAGIC Spark-distributed ingestion of EnergyQuantified curves for a single category.
# MAGIC Called as a downstream task from the plan task.
# MAGIC
# MAGIC Receives pinned `end_date` and `seconds` from the plan task so that
# MAGIC retries use the exact same time window.
# MAGIC
# MAGIC For INSTANCE curves, an additional ensembles query is generated.
# MAGIC For curves with GBP/USD units, an additional EUR query is generated.

# COMMAND ----------

import sys
import datetime as dt
import logging

sys.path.insert(0, "/Workspace/Shared/MontelEQ/python/src")

from yggdrasil.data.enums import Mode
from yggdrasil.environ.parameters import SystemParameters

logger = logging.getLogger(__name__)

# COMMAND ----------


class Config(SystemParameters):
    end_date: dt.datetime = "now"
    seconds: int = 3600
    table_category: str = "curated_actual_period_carbon_tax"
    catalog_name: str = "trading_tgp_prd"
    schema_name: str = "src_monteleq"
    mode: str = "append"

    @property
    def start_date(self) -> dt.datetime:
        return self.end_date - dt.timedelta(seconds=self.seconds)

    @property
    def mode_enum(self):
        return Mode.from_(self.mode)


config = Config().init_job()

if not config.table_category:
    raise ValueError("`table_category` is required")

print(config)

# COMMAND ----------

# DBTITLE 1,Resolve time window
begin_dt = config.start_date
end_dt = config.end_date

issued_at_earliest = begin_dt

insert_mode = config.mode_enum.name if config.mode_enum != Mode.APPEND else None

logger.info(
    "Starting ingestion: table_category=%s begin=%s end=%s insert_mode=%s",
    config.table_category, begin_dt, end_dt, insert_mode or "append",
)

# COMMAND ----------

# DBTITLE 1,Run ingestion
from energyquantified.metadata import CurveType

from monteleq.api.client import APIClient
from monteleq.api.request import CurveRequest

FOREIGN_UNITS = {"GBP", "USD"}

client = APIClient(catalog_name=config.catalog_name, schema_name=config.schema_name)

curves = [
    c for c in client.metadata.curves()
    if c.table_name(prefix="curated_") == config.table_category
]
if not curves:
    logger.warning("No curves found for table_category=%s", config.table_category)
    print({"table_category": config.table_category, "curves": 0, "status": "empty"})
    dbutils.notebook.exit("empty")  # noqa: F821

logger.info("Found %d curves for table_category=%s", len(curves), config.table_category)

requests: list[CurveRequest] = []
for c in curves:
    base = CurveRequest(
        curve=c,
        begin=begin_dt,
        end=end_dt,
        issued_at_earliest=issued_at_earliest,
        client=client,
        raise_error=False,
    )
    requests.append(base)

    if c.curve_type == CurveType.INSTANCE:
        requests.append(base.copy(ensembles=True))

    if c.unit and any(cu in c.unit for cu in FOREIGN_UNITS):
        eur_unit = c.unit
        for cu in FOREIGN_UNITS:
            eur_unit = eur_unit.replace(cu, "EUR")
        requests.append(base.copy(unit=eur_unit))

stats = client.ingest_spark(
    requests,
    spark=True,
    raise_error=False,
    insert_mode=insert_mode,
)

result = {"table_category": config.table_category, "curves": len(curves), **stats}
logger.info("Ingestion complete: %s", result)

# COMMAND ----------

print(result)
