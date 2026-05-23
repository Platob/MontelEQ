# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ Ingestion Pipeline
# MAGIC
# MAGIC Spark-distributed ingestion of EnergyQuantified curves, parallelized by data type.
# MAGIC Each run fetches raw HTTP responses via `mapInArrow`, caches them in `raw_*` tables,
# MAGIC curates only `new_hits`, and batch-inserts into `curated_*` Delta tables.

# COMMAND ----------

import datetime as dt
import logging
import time

logger = logging.getLogger("monteleq")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s"))
    logger.addHandler(handler)

# COMMAND ----------

# DBTITLE 1,Parameters
data_type = dbutils.widgets.get("data_type")  # noqa: F821
catalog_name = dbutils.widgets.get("catalog_name") if "catalog_name" in dbutils.widgets.getArgument("catalog_name", "trading_tgp_prd") else "trading_tgp_prd"  # noqa: F821
schema_name = "src_monteleq"
period_days = int(dbutils.widgets.get("period_days")) if "period_days" in dbutils.widgets.getArgument("period_days", "60") else 60  # noqa: F821

print(f"data_type={data_type}, catalog={catalog_name}, period_days={period_days}")

# COMMAND ----------

# DBTITLE 1,Initialize client
from monteleq.api.client import APIClient
from monteleq.api.request import CurveRequest

client = APIClient(catalog_name=catalog_name, schema_name=schema_name)

# COMMAND ----------

# DBTITLE 1,Discover curves
now = dt.datetime.now(dt.timezone.utc)
end = now
begin = now - dt.timedelta(days=period_days)

curves = client.metadata.curves(data_type=data_type)
logger.info("Found %d curves for data_type=%s", len(curves), data_type)

if not curves:
    dbutils.notebook.exit(f"No curves for data_type={data_type}")  # noqa: F821

# COMMAND ----------

# DBTITLE 1,Build requests
requests = [
    CurveRequest(
        curve=c,
        begin=begin,
        end=end,
        issued_at_earliest=begin,
        client=client,
        raise_error=False,
    )
    for c in curves
]

logger.info("Built %d requests for %d curves", len(requests), len(curves))

# COMMAND ----------

# DBTITLE 1,Distributed ingestion
t0 = time.perf_counter()

stats = client.ingest_spark(
    requests,
    spark_session=spark,  # noqa: F821
    raise_error=False,
)

elapsed = time.perf_counter() - t0
logger.info("Ingestion complete in %.2fs: %s", elapsed, stats)

# COMMAND ----------

# DBTITLE 1,Summary
print(f"Data type: {data_type}")
print(f"Curves: {len(curves)}")
print(f"Batches fetched: {stats.get('fetched', 0)}")
print(f"Rows curated: {stats.get('curated', 0)}")
print(f"Tables written: {stats.get('tables', 0)}")
print(f"Elapsed: {stats.get('elapsed', 0)}s")
