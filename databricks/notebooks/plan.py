# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — Plan
# MAGIC
# MAGIC Fetches the curve catalog, upserts the `curated_curve_metadata`
# MAGIC referential (with `table_category` mapping each curve to its
# MAGIC destination Delta table), resolves categories, and outputs them
# MAGIC for downstream `ingest_by_category` tasks.

# COMMAND ----------

import json

from yggdrasil.environ.parameters import SystemParameters

# COMMAND ----------


class Config(SystemParameters):
    catalog_name: str = "trading_tgp_prd"
    schema_name: str = "src_monteleq"


config = Config().init_job()

print(config)

# COMMAND ----------

# DBTITLE 1,Refresh curve metadata referential
from monteleq.pipeline import refresh_curve_metadata

n_curves = refresh_curve_metadata(
    catalog_name=config.catalog_name,
    schema_name=config.schema_name,
)

print(f"Upserted {n_curves} curves into curated_curve_metadata")

# COMMAND ----------

# DBTITLE 1,Resolve categories
from monteleq.pipeline import plan_categories

categories = plan_categories(
    catalog_name=config.catalog_name,
    schema_name=config.schema_name,
)

print(f"Resolved {len(categories)} categories: {categories}")

# COMMAND ----------

# DBTITLE 1,Output for downstream tasks
dbutils.notebook.exit(json.dumps(categories))  # noqa: F821
