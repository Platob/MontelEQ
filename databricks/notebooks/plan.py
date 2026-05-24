# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — Plan
# MAGIC
# MAGIC Fetches the curve catalog, upserts the `curve_metadata`
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
    start_date: str = ""
    end_date: str = ""
    mode: Mode = Mode.APPEND


config = Config().init_job()

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

begin_dt = _parse_dt(config.start_date or None)
end_dt = _parse_dt(config.end_date or None)

if begin_dt is None:
    end_dt = now
    begin_dt = now - dt.timedelta(hours=1)
elif end_dt is None:
    end_dt = now

print(f"Time window: {begin_dt} → {end_dt}")

# COMMAND ----------

# DBTITLE 1,Refresh curve metadata referential
from yggdrasil.execution.expr.builder import col

from monteleq.api.client import APIClient
from monteleq.api.schemas import CURVE_METADATA_SCHEMA

client = APIClient(catalog_name=config.catalog_name, schema_name=config.schema_name)

df = client.metadata.metadata_df()

if df.height == 0:
    raise RuntimeError("refresh_curve_metadata: no curves found")

table = client.sql.table(table_name="curve_metadata").ensure_created(
    CURVE_METADATA_SCHEMA
)
table.insert(
    df,
    mode=config.mode,
    match_by=["curve_id"],
    where=col("curve_id").is_in(df["curve_id"].to_list()),
)

print(f"Upserted {df.height} curves into curve_metadata")

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
start_date_iso = begin_dt.isoformat()
end_date_iso = end_dt.isoformat()

output = [
    {"table_category": c, "start_date": start_date_iso, "end_date": end_date_iso}
    for c in table_categories
]

dbutils.notebook.exit(json.dumps(output))  # noqa: F821
