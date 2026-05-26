# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — Plan
# MAGIC
# MAGIC Fetches the curve catalog, upserts the `curated_curve_metadata`
# MAGIC referential, resolves table categories, and outputs them
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
    table_category: str = ""
    end_date: dt.datetime = "now"
    seconds: int = 3600
    mode: str = "append"

    @property
    def start_date(self) -> dt.datetime:
        return self.end_date - dt.timedelta(seconds=self.seconds)

    @property
    def mode_enum(self):
        return Mode.from_(self.mode)


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
    mode=config.mode_enum,
    match_by=["curve_id"],
    where=col("curve_id").is_in(df["curve_id"].to_list()),
)

print(f"Upserted {df.height} curves into curated_curve_metadata")

# COMMAND ----------

# DBTITLE 1,Resolve table categories
table_categories = sorted(df["table_category"].unique().to_list())

if config.table_category:
    requested = [c.strip() for c in config.table_category.split(",") if c.strip()]
    unknown = set(requested) - set(table_categories)
    if unknown:
        raise ValueError(f"Unknown table categories: {sorted(unknown)}")
    table_categories = [c for c in table_categories if c in requested]

print(f"Resolved {len(table_categories)} table categories: {table_categories}")

# COMMAND ----------

# DBTITLE 1,Group categories by (data_type, curve_type)
from monteleq.model import _safe_name

cluster_key_map = {}
for row in (
    df.select("table_category", "curve_data_type", "curve_type")
    .unique(subset=["table_category"])
    .iter_rows(named=True)
):
    ck = (
        f"{_safe_name(row['curve_data_type'] or '')}_{_safe_name(row['curve_type'] or '')}"
    )
    cluster_key_map[row["table_category"]] = ck

groups: dict[str, list[str]] = {}
for cat in table_categories:
    ck = cluster_key_map.get(cat, "unknown")
    groups.setdefault(ck, []).append(cat)

print(
    f"Grouped {len(table_categories)} categories into "
    f"{len(groups)} cluster groups: {sorted(groups.keys())}"
)

# COMMAND ----------

# DBTITLE 1,Output for downstream tasks
end_date_iso = end_dt.isoformat()

output = [
    {
        "cluster_key": ck,
        "table_categories": ",".join(cats),
        "end_date": end_date_iso,
        "seconds": str(config.seconds),
    }
    for ck, cats in sorted(groups.items())
]

dbutils.jobs.taskValues.set(key="groups", value=output)  # noqa: F821
dbutils.notebook.exit(json.dumps(output))  # noqa: F821
