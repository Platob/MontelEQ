# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — Plan
# MAGIC
# MAGIC Fetches the curve catalog, upserts the `curated_curve_metadata`
# MAGIC referential, resolves categories, and outputs them
# MAGIC for downstream `ingest_by_category` tasks.

# COMMAND ----------

import json
import sys

sys.path.insert(0, "/Workspace/Shared/MontelEQ/python/src")

from yggdrasil.environ.parameters import SystemParameters

# COMMAND ----------


class Config(SystemParameters):
    catalog_name: str = "trading_tgp_prd"
    schema_name: str = "src_monteleq"


config = Config().init_job()

print(config)

# COMMAND ----------

# DBTITLE 1,Refresh curve metadata referential
from yggdrasil.data.enums import Mode
from yggdrasil.execution.expr.builder import col

from monteleq.api.client import APIClient
from monteleq.api.schemas import CURVE_METADATA_SCHEMA

client = APIClient(catalog_name=config.catalog_name, schema_name=config.schema_name)

df = client.metadata.metadata_df()

if df.height == 0:
    raise RuntimeError("refresh_curve_metadata: no curves found")

table = client.sql.table(table_name="curated_curve_metadata").ensure_created(
    CURVE_METADATA_SCHEMA
)
table.insert(
    df,
    mode=Mode.APPEND,
    match_by=["curve_id"],
    where=col("curve_id").is_in(df["curve_id"].to_list()),
)

print(f"Upserted {df.height} curves into curated_curve_metadata")

# COMMAND ----------

# DBTITLE 1,Resolve table categories
table_categories = sorted(df["table_category"].unique().to_list())

print(f"Resolved {len(table_categories)} table categories: {table_categories}")

# COMMAND ----------

# DBTITLE 1,Output for downstream tasks
dbutils.notebook.exit(json.dumps(table_categories))  # noqa: F821
