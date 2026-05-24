# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — Ingest by Category
# MAGIC
# MAGIC Spark-distributed ingestion of EnergyQuantified curves for a single category.
# MAGIC Called as a downstream task from the plan task.
# MAGIC
# MAGIC Receives pinned `start_date`/`end_date` from the plan task so that
# MAGIC retries use the exact same time window.

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
    start_date: str = ""
    end_date: str = ""
    table_category: str = ""
    catalog_name: str = "trading_tgp_prd"
    schema_name: str = "src_monteleq"
    mode: Mode = Mode.APPEND


config = Config().init_job()

if not config.table_category:
    raise ValueError("`table_category` is required")
if not config.start_date:
    raise ValueError("`start_date` is required")
if not config.end_date:
    raise ValueError("`end_date` is required")

print(config)

# COMMAND ----------

# DBTITLE 1,Parse time window
begin_dt = dt.datetime.fromisoformat(config.start_date)
end_dt = dt.datetime.fromisoformat(config.end_date)

issued_at_earliest = begin_dt

insert_mode = config.mode.name if config.mode != Mode.APPEND else None

logger.info(
    "Starting ingestion: table_category=%s begin=%s end=%s insert_mode=%s",
    config.table_category, begin_dt, end_dt, insert_mode or "append",
)

# COMMAND ----------

# DBTITLE 1,Run ingestion
from monteleq.api.client import APIClient
from monteleq.api.request import CurveRequest

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

requests = [
    CurveRequest(
        curve=c,
        begin=begin_dt,
        end=end_dt,
        issued_at_earliest=issued_at_earliest,
        client=client,
        raise_error=False,
    )
    for c in curves
]

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
