# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — ingest_category (per-category worker)
# MAGIC
# MAGIC Runs on a dedicated per-category cluster, dispatched by `dispatcher`.
# MAGIC Following the [Meteologica](https://github.com/Platob/Meteologica) pattern,
# MAGIC each worker ingests one `table_category` from EnergyQuantified and curates
# MAGIC it into the shared `curated_<data_type>_<curve_type>_<categories>` table.
# MAGIC
# MAGIC Receives a pinned `end_date` and `seconds` from the dispatcher so retries
# MAGIC reuse the exact same time window.
# MAGIC
# MAGIC * In *scheduled* runs, the dispatcher has queued the updated curves into
# MAGIC   `pending_requests`; this worker ingests exactly those curves and then
# MAGIC   drains the rows it consumed (idempotent replay on failure).
# MAGIC * In *backfill* runs (`seconds > 3600`) the queue is empty, so the worker
# MAGIC   ingests every curve in the category for the full window.
# MAGIC
# MAGIC For INSTANCE curves an additional ensembles request is generated, and for
# MAGIC GBP/USD-priced curves an additional EUR request is generated.
# MAGIC
# MAGIC Parameters:
# MAGIC
# MAGIC * `table_category` — table category key (e.g. `actual_timeseries_power`).
# MAGIC * `end_date` — end of the ingestion window (ISO-8601, default now).
# MAGIC * `seconds` — lookback in seconds (default 3600).
# MAGIC * `mode` — insert mode (`append`, `overwrite`, `upsert`).

# COMMAND ----------

import datetime as dt
import logging
import sys

sys.path.insert(0, "/Workspace/Shared/MontelEQ/python/src")

from yggdrasil.enums import Mode
from yggdrasil.environ.parameters import SystemParameters

from monteleq.api.client import APIClient

logger = logging.getLogger("monteleq.databricks.ingest_category")
logger.setLevel(logging.INFO)

# COMMAND ----------


class Config(SystemParameters):
    table_category: str = ""
    end_date: str = ""
    seconds: int = 3600
    catalog_name: str = "trading_tgp_prd"
    schema_name: str = "src_monteleq"
    mode: str = "append"
    pending_table: str = "pending_requests"

    @property
    def mode_enum(self) -> Mode:
        return Mode.from_(self.mode)


config = Config().init_job()

if not config.table_category:
    raise ValueError("`table_category` is required")

print(config)

# COMMAND ----------

# DBTITLE 1,Resolve time window
now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
end_dt = dt.datetime.fromisoformat(config.end_date) if config.end_date.strip() else now
begin_dt = end_dt - dt.timedelta(seconds=config.seconds)

insert_mode = config.mode_enum.name if config.mode_enum != Mode.APPEND else None

logger.info(
    "Starting ingestion: table_category=%s begin=%s end=%s insert_mode=%s",
    config.table_category, begin_dt, end_dt, insert_mode or "append",
)

# COMMAND ----------

# DBTITLE 1,Resolve curves (pending queue → fallback to full category)
client = APIClient(catalog_name=config.catalog_name, schema_name=config.schema_name)

category_curves = {c.name: c for c in client.category_curves(config.table_category)}
if not category_curves:
    logger.warning("No curves found for table_category=%s", config.table_category)
    print({"table_category": config.table_category, "curves": 0, "status": "empty"})
    dbutils.notebook.exit("empty")  # noqa: F821

# Read the pending queue this dispatcher run populated for the category.
pending = client.pending_requests_table(table_name=config.pending_table)
category_lit = config.table_category.replace("'", "''")
pending_df = pending.lazy(
    sql=(
        "SELECT request_id, curve_name FROM {self} "
        f"WHERE table_category = '{category_lit}'"
    )
).read_polars_frame()

queued_names = (
    pending_df["curve_name"].unique().to_list() if pending_df.height else []
)

if queued_names:
    curves = [category_curves[n] for n in queued_names if n in category_curves]
    logger.info(
        "Category %s: ingesting %d queued curves (of %d in category)",
        config.table_category, len(curves), len(category_curves),
    )
else:
    curves = list(category_curves.values())
    logger.info(
        "Category %s: no queue, ingesting all %d curves",
        config.table_category, len(curves),
    )

if not curves:
    dbutils.notebook.exit(f"empty:{config.table_category}")  # noqa: F821

# COMMAND ----------

# DBTITLE 1,Run ingestion
requests = client.curve_requests(
    curves,
    begin=begin_dt,
    end=end_dt,
    issued_at_earliest=begin_dt,
    raise_error=False,
)

stats = client.ingest_spark(
    requests,
    spark=True,
    raise_error=False,
    insert_mode=insert_mode,
)

result = {"table_category": config.table_category, "curves": len(curves), **stats}
logger.info("Ingestion complete: %s", result)

# COMMAND ----------

# DBTITLE 1,Drain consumed pending rows (idempotent)
if pending_df.height:
    request_ids = pending_df["request_id"].to_list()
    id_csv = ", ".join("'" + rid.replace("'", "''") + "'" for rid in request_ids)
    client.sql.execute(
        f"DELETE FROM {pending.full_name()} "
        f"WHERE table_category = '{category_lit}' AND request_id IN ({id_csv})"
    )
    logger.info(
        "Drained %d pending rows for category=%s",
        len(request_ids), config.table_category,
    )

# COMMAND ----------

print(result)
