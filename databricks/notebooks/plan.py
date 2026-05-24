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
import datetime as dt
import polars as pl
from yggdrasil.data.enums import Mode
from yggdrasil.execution.expr.builder import col

from monteleq.api.client import APIClient
from monteleq.api.schemas import CURVE_METADATA_SCHEMA

client = APIClient(catalog_name=config.catalog_name, schema_name=config.schema_name)
curves = client.metadata.curves()

if not curves:
    raise RuntimeError("refresh_curve_metadata: no curves found")

now = dt.datetime.now(dt.timezone.utc)

df = pl.DataFrame(
    [c.to_metadata_row(now=now) for c in curves],
    schema=CURVE_METADATA_SCHEMA.to_polars_schema(),
)

curve_ids = tuple(c.id for c in curves)

table = client.sql.table(table_name="curated_curve_metadata").ensure_created(
    CURVE_METADATA_SCHEMA
)
table.insert(
    df,
    mode=Mode.APPEND,
    match_by=["curve_id"],
    where=col("curve_id").is_in(curve_ids),
    prune_by="auto",
)

print(f"Upserted {len(curves)} curves into curated_curve_metadata")

# COMMAND ----------

# DBTITLE 1,Resolve categories
categories = sorted({c.categories[0] for c in curves if c.categories})

print(f"Resolved {len(categories)} categories: {categories}")

# COMMAND ----------

# DBTITLE 1,Output for downstream tasks
dbutils.notebook.exit(json.dumps(categories))  # noqa: F821
