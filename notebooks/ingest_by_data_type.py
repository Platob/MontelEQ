# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ Ingestion Pipeline
# MAGIC
# MAGIC Spark-distributed ingestion of EnergyQuantified curves by category.
# MAGIC Fetches raw HTTP responses via `mapInArrow`, caches in `raw_*` tables,
# MAGIC curates only `new_hits`, and batch-inserts into `curated_*` Delta tables.

# COMMAND ----------

import datetime as dt
import logging
import time

logger = logging.getLogger("monteleq")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s"))
    logger.addHandler(handler)

# COMMAND ----------

# DBTITLE 1,Parameters
curve_category = dbutils.widgets.get("curve_category")  # noqa: F821
catalog_name = dbutils.widgets.get("catalog_name")  # noqa: F821
period_days = int(dbutils.widgets.get("period_days"))  # noqa: F821

print(f"curve_category={curve_category}, catalog={catalog_name}, period_days={period_days}")

# COMMAND ----------

# DBTITLE 1,Run ingestion
from monteleq.pipeline import ingest_category

result = ingest_category(
    curve_category,
    catalog_name=catalog_name,
    period_days=period_days,
)

# COMMAND ----------

# DBTITLE 1,Summary
print(result)
