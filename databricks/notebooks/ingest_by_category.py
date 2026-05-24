# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — Ingest by Category
# MAGIC
# MAGIC Spark-distributed ingestion of EnergyQuantified curves for a single category.
# MAGIC Called as a downstream task from the plan task.
# MAGIC
# MAGIC * **latest=True** (default, scheduled) — uses `period_hours` as a
# MAGIC   lookback window from now.  Incremental append.
# MAGIC * **latest=False** (manual backfill) — uses explicit `start`/`end`
# MAGIC   datetime range.  `mode` can be set to `overwrite` to replace
# MAGIC   curated data for the window.

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
    latest: bool = True
    start: str = ""
    end: str = ""
    table_category: str = ""
    catalog_name: str = "trading_tgp_prd"
    schema_name: str = "src_monteleq"
    period_hours: int = 1
    mode: Mode = Mode.APPEND


config = Config().init_job()

if not config.table_category:
    raise ValueError("`table_category` widget is required")

print(config)

# COMMAND ----------

# DBTITLE 1,Resolve time window
def _parse_dt(value: str | None) -> dt.datetime | None:
    if not value or not value.strip():
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(s)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


now = dt.datetime.now(dt.timezone.utc)

if config.latest:
    end_dt = now
    begin_dt = now - dt.timedelta(hours=config.period_hours)
else:
    begin_dt = _parse_dt(config.start or None)
    end_dt = _parse_dt(config.end or None)
    if begin_dt is None:
        raise ValueError("`start` is required when latest=False")
    if end_dt is None:
        end_dt = now

issued_at_earliest = begin_dt

insert_mode = config.mode.name if config.mode != Mode.APPEND else None

logger.info(
    "Starting ingestion: table_category=%s begin=%s end=%s latest=%s insert_mode=%s",
    config.table_category, begin_dt, end_dt, config.latest, insert_mode or "append",
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
