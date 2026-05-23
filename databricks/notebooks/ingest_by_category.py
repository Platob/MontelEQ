# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — Ingest by Category
# MAGIC
# MAGIC Spark-distributed ingestion of EnergyQuantified curves for a single category.
# MAGIC Called as a downstream task from the plan task.

# COMMAND ----------

import logging

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
insert_mode = dbutils.widgets.get("insert_mode") or None  # noqa: F821

print(f"category={curve_category}, catalog={catalog_name}, period_days={period_days}, insert_mode={insert_mode}")

# COMMAND ----------

# DBTITLE 1,Run ingestion
from monteleq.pipeline import ingest_category

result = ingest_category(
    curve_category,
    catalog_name=catalog_name,
    period_days=period_days,
    insert_mode=insert_mode,
)

# COMMAND ----------

print(result)
