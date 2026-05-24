# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — Plan
# MAGIC
# MAGIC Fetches the curve catalog, upserts the `curated_curve_metadata`
# MAGIC referential, resolves categories, and outputs them
# MAGIC for downstream `ingest_by_category` tasks.

# COMMAND ----------

import datetime as dt
import json
import sys

sys.path.insert(0, "/Workspace/Shared/MontelEQ/python/src")

from yggdrasil.data.enums import Mode
from yggdrasil.environ.parameters import SystemParameters

# COMMAND ----------


class Config(SystemParameters):
    catalog_name: str = "trading_tgp_prd"
    schema_name: str = "src_monteleq"
    categories: str = ""
    end_date: dt.datetime = "now"
    seconds: int = 3600
    mode: Mode = Mode.APPEND

    @property
    def start_date(self) -> dt.datetime:
        return self.end_date - dt.timedelta(seconds=self.seconds)


config = Config().init_job()

print(config)

# COMMAND ----------

# DBTITLE 1,Resolve time window
begin_dt = config.start_date
end_dt = config.end_date

print(f"Time window: {begin_dt} → {end_dt}")

# COMMAND ----------

# DBTITLE 1,Refresh curve metadata referential
from yggdrasil.execution.expr.builder import col

from monteleq.api.client import APIClient
from monteleq.api.schemas import CURVE_METADATA_SCHEMA

client = APIClient(catalog_name=config.catalog_name, schema_name=config.schema_name)

df = client.metadata.metadata_df()

if df.height == 0:
    raise RuntimeError("refresh_curated_curve_metadata: no curves found")

table = client.sql.table(table_name="curated_curve_metadata").ensure_created(
    CURVE_METADATA_SCHEMA
)
table.insert(
    df,
    mode=config.mode,
    match_by=["curve_id"],
    where=col("curve_id").is_in(df["curve_id"].to_list()),
)

print(f"Upserted {df.height} curves into curated_curve_metadata")

# COMMAND ----------

# DBTITLE 1,Resolve table categories
table_categories = sorted(df["table_category"].unique().to_list())

if config.categories:
    requested = [c.strip() for c in config.categories.split(",") if c.strip()]
    unknown = set(requested) - set(table_categories)
    if unknown:
        raise ValueError(f"Unknown categories: {sorted(unknown)}")
    table_categories = [c for c in table_categories if c in requested]

print(f"Resolved {len(table_categories)} table categories: {table_categories}")

# COMMAND ----------

# DBTITLE 1,Output for downstream tasks
end_date_iso = end_dt.isoformat()

output = [
    {
        "table_category": c,
        "end_date": end_date_iso,
        "seconds": str(config.seconds),
    }
    for c in table_categories
]

dbutils.jobs.taskValues.set(key="categories", value=output)  # noqa: F821
dbutils.notebook.exit(json.dumps(output))  # noqa: F821
