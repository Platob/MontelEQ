# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — Plan
# MAGIC
# MAGIC Fetches the curve catalog, resolves which categories to ingest,
# MAGIC and outputs the list for downstream `ingest_by_category` tasks.

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

# DBTITLE 1,Fetch catalog and resolve categories
from monteleq.pipeline import plan_categories

categories = plan_categories(
    catalog_name=config.catalog_name,
    schema_name=config.schema_name,
)

print(f"Resolved {len(categories)} categories: {categories}")

# COMMAND ----------

# DBTITLE 1,Output for downstream tasks
dbutils.notebook.exit(json.dumps(categories))  # noqa: F821
