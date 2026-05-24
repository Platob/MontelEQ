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
import sys
import datetime as dt
import logging

sys.path.insert(0, "/Workspace/Shared/MontelEQ/python/src")

from yggdrasil.environ.parameters import SystemParameters

logger = logging.getLogger(__name__)

# COMMAND ----------


class Config(SystemParameters):
    catalog_name: str = "trading_tgp_prd"
    schema_name: str = "src_monteleq"


config = Config().init_job()

print(config)

# COMMAND ----------

# DBTITLE 1,Refresh curve metadata referential
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
    {
        "curve_id": [c.id for c in curves],
        "curve_name": [c.name for c in curves],
        "curve_type": [c.curve_type.name for c in curves],
        "curve_data_type": [c.data_type.name for c in curves],
        "curve_area": [c.area for c in curves],
        "curve_area_sink": [c.area_sink for c in curves],
        "curve_commodity": [c.commodity for c in curves],
        "curve_source": [c.source for c in curves],
        "curve_unit": [c.unit for c in curves],
        "curve_denominator": [c.denominator for c in curves],
        "curve_categories": [list(c.categories) for c in curves],
        "curve_resolution_frequency": [c.resolution.frequency for c in curves],
        "curve_resolution_timezone": [c.resolution.timezone for c in curves],
        "curve_access_by": [c.access.by for c in curves],
        "curve_access_package": [c.access.package for c in curves],
        "curve_instance_issued_timezone": [c.instance_issued_timezone for c in curves],
        "table_category": [c.table_name(prefix="curated_") for c in curves],
        "updated_at": [now] * len(curves),
    },
    schema={
        "curve_id": pl.Int64,
        "curve_name": pl.Utf8,
        "curve_type": pl.Utf8,
        "curve_data_type": pl.Utf8,
        "curve_area": pl.Utf8,
        "curve_area_sink": pl.Utf8,
        "curve_commodity": pl.Utf8,
        "curve_source": pl.Utf8,
        "curve_unit": pl.Utf8,
        "curve_denominator": pl.Utf8,
        "curve_categories": pl.List(pl.Utf8),
        "curve_resolution_frequency": pl.Utf8,
        "curve_resolution_timezone": pl.Utf8,
        "curve_access_by": pl.Utf8,
        "curve_access_package": pl.Utf8,
        "curve_instance_issued_timezone": pl.Utf8,
        "table_category": pl.Utf8,
        "updated_at": pl.Datetime("us", "UTC"),
    },
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
