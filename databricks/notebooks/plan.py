# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — Plan
# MAGIC
# MAGIC Fetches the curve catalog, resolves which categories to ingest,
# MAGIC and outputs the list for downstream `ingest_by_category` tasks.

# COMMAND ----------

import json
import logging

logger = logging.getLogger("monteleq")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s"))
    logger.addHandler(handler)

# COMMAND ----------

# DBTITLE 1,Parameters
catalog_name = dbutils.widgets.get("catalog_name")  # noqa: F821

# COMMAND ----------

# DBTITLE 1,Fetch catalog and resolve categories
from monteleq.pipeline import plan_categories

categories = plan_categories(catalog_name=catalog_name)
logger.info("Plan resolved %d categories: %s", len(categories), categories)

# COMMAND ----------

# DBTITLE 1,Output for downstream tasks
dbutils.notebook.exit(json.dumps(categories))  # noqa: F821
