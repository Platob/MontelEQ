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

from yggdrasil.data.enums import Mode
from yggdrasil.environ.parameters import SystemParameters

# COMMAND ----------


class Config(SystemParameters):
    latest: bool = True
    start: str = ""
    end: str = ""
    curve_category: str = ""
    catalog_name: str = "trading_tgp_prd"
    schema_name: str = "src_monteleq"
    period_hours: int = 1
    mode: Mode = Mode.APPEND


config = Config().init_job()

if not config.curve_category:
    raise ValueError("`curve_category` widget is required")

print(config)

# COMMAND ----------

# DBTITLE 1,Run ingestion
from monteleq.pipeline import ingest_category

result = ingest_category(
    config.curve_category,
    catalog_name=config.catalog_name,
    schema_name=config.schema_name,
    latest=config.latest,
    start=config.start or None,
    end=config.end or None,
    period_hours=config.period_hours,
    insert_mode=config.mode.name if config.mode != Mode.APPEND else None,
)

# COMMAND ----------

print(result)
