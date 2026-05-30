# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — Dispatcher
# MAGIC
# MAGIC Scheduled entry point for the MontelEQ ingestion pipeline, following the
# MAGIC [Meteologica](https://github.com/Platob/Meteologica) ingestion pattern but
# MAGIC sourcing data from EnergyQuantified (EQ). Three-phase pipeline:
# MAGIC
# MAGIC 1. **Scan & queue** — refresh the curve-metadata referential, then either:
# MAGIC    * *scheduled* (`seconds <= 3600`): stream EQ curve-update events and
# MAGIC      queue the updated curves into the `pending_requests` Delta table,
# MAGIC      bucketed by `table_category`; or
# MAGIC    * *backfill* (`seconds > 3600`): dispatch every known category, fetching
# MAGIC      the full window directly (no queue needed).
# MAGIC 2. **Cluster** — group the in-scope categories by their coarse
# MAGIC    `cluster_key` (`data_type_curve_type`) and get-or-create one dedicated
# MAGIC    Databricks all-purpose cluster per cluster_key via
# MAGIC    `client.databricks.clusters.get_or_create`, in parallel using a thread
# MAGIC    pool. Categories sharing a cluster_key share a cluster, so the cluster
# MAGIC    count stays bounded regardless of how many categories exist.
# MAGIC 3. **Dispatch** — submit a one-off `ingest_category` job run per
# MAGIC    `table_category` onto its cluster_key's shared cluster. When a
# MAGIC    `curve_ids` filter is active, each worker is handed the explicit
# MAGIC    in-scope curve ids for its category; otherwise it gets an empty
# MAGIC    `curve_ids` and ingests every curve in the category.
# MAGIC
# MAGIC Parameters:
# MAGIC
# MAGIC * `seconds` — lookback window in seconds (default 3600 = 1 h). When
# MAGIC   `> 3600`, switches to backfill mode.
# MAGIC * `end_date` — optional end-of-window (ISO-8601); defaults to now. Pinned and
# MAGIC   forwarded to each `ingest_category` job so retries use the same window.
# MAGIC * `table_category` — optional comma-separated list to restrict the run to a
# MAGIC   subset of categories (default: all).
# MAGIC * `curve_ids` — optional comma-separated list of curve ids or names to
# MAGIC   restrict the run to specific curves (combines with `table_category`).
# MAGIC * `batch_size` — number of HTTP requests fetched + curated + inserted per
# MAGIC   batch by each worker (default 200); bounds worker memory on backfills.
# MAGIC * `mode` — insert mode for the curated writes (`append`, `overwrite`, `upsert`).
# MAGIC
# MAGIC Deploy / run:
# MAGIC
# MAGIC     databricks bundle run -t prd monteleq_dispatcher
# MAGIC
# MAGIC Backfill (6-month window, overwrite):
# MAGIC
# MAGIC     databricks bundle run -t prd monteleq_dispatcher \
# MAGIC         --params seconds=15552000 --params mode=overwrite

# COMMAND ----------

import datetime as dt
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/Workspace/Shared/MontelEQ/python/src")

import polars as pl
from databricks.sdk.service.jobs import NotebookTask, SubmitTask
from energyquantified.metadata import CurveType

from yggdrasil.enums import Mode
from yggdrasil.execution.expr.builder import col
from yggdrasil.environ.parameters import SystemParameters

from monteleq.api.client import APIClient, FOREIGN_UNITS
from monteleq.api.schemas import PENDING_REQUESTS_SCHEMA
from monteleq.model import _safe_name

logger = logging.getLogger("monteleq.databricks.dispatcher")
logger.setLevel(logging.INFO)

# COMMAND ----------


class Config(SystemParameters):
    seconds: int = 3600
    end_date: str = ""
    table_category: str = ""
    curve_ids: str = ""
    batch_size: int = 200
    catalog_name: str = "trading_tgp_prd"
    schema_name: str = "src_monteleq"
    mode: str = "append"
    pending_table: str = "pending_requests"
    metadata_table: str = "curated_curve_metadata"
    notebook_root: str = "/Workspace/Shared/MontelEQ/databricks/notebooks"
    events_checkpoint: str = "/dbfs/tmp/monteleq/dispatcher-events-checkpoint.json"
    max_clusters: int = 16


config = Config().init_job()
print(config)

# COMMAND ----------

# DBTITLE 1,Resolve scope & refresh referential
client = APIClient(catalog_name=config.catalog_name, schema_name=config.schema_name)

now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
end_dt = (
    dt.datetime.fromisoformat(config.end_date)
    if config.end_date.strip()
    else now
)
begin_dt = end_dt - dt.timedelta(seconds=config.seconds)
run_id = int(now.timestamp() * 1_000_000)

requested = [c.strip() for c in config.table_category.split(",") if c.strip()]
requested_ids = client.parse_curve_ids(config.curve_ids)

table, meta_df = client.refresh_metadata(table_name=config.metadata_table)
logger.info("Refreshed %d curves into %s", meta_df.height, table.name)

curvemap = client.metadata.curvemap
known_categories = set(client.categories())
if requested:
    unknown = set(requested) - known_categories
    if unknown:
        raise ValueError(f"Unknown table categories: {sorted(unknown)}")

if requested_ids:
    known_ids = {str(c.id) for c in curvemap.values()} | set(curvemap.keys())
    unknown_ids = requested_ids - known_ids
    if unknown_ids:
        raise ValueError(f"Unknown curve ids/names: {sorted(unknown_ids)}")

# Map each table_category to its coarser cluster_key (data_type_curve_type) so
# Phase 2 can provision one shared cluster per cluster_key rather than one per
# category. ``setdefault`` keeps the first cluster_key seen; all curves routed
# to a given category share the same (data_type, curve_type) by construction.
cat_to_cluster_key: dict[str, str] = {}
for c in curvemap.values():
    cat_to_cluster_key.setdefault(c.table_name(), c.cluster_key())


def _selected(curve) -> bool:
    """Whether a curve is in scope given the table_category / curve_ids filters."""
    if requested and curve.table_name() not in requested:
        return False
    if requested_ids and str(curve.id) not in requested_ids and curve.name not in requested_ids:
        return False
    return True


print(f"Window: {begin_dt.isoformat()} -> {end_dt.isoformat()}  run_id={run_id}")

# COMMAND ----------

# DBTITLE 1,Phase 1 — scan & queue
if config.seconds > 3600:
    # Backfill: dispatch every in-scope category. The per-category job fetches
    # the full window directly; no pending queue required.
    categories = sorted({c.table_name() for c in curvemap.values() if _selected(c)})
    logger.info("Backfill mode: %d categories", len(categories))
else:
    # Scheduled: stream EQ curve-update events and queue the updated curves,
    # bucketed by table_category.
    target: dict[str, object] = {}
    for batch in client.events.stream(
        batch_size=None,
        max_batch_seconds=30.0,
        reconnect=False,
        idle_reconnect_seconds=10.0,
        checkpoint_path=config.events_checkpoint,
        progress=False,
    ):
        for event in batch:
            curve = curvemap.get(event.curve.name)
            if curve is None or not _selected(curve):
                continue
            target[curve.name] = curve

    logger.info("Events mode: %d curves with updates", len(target))

    if not target:
        dbutils.notebook.exit("no_new_data")  # noqa: F821

    rows: list[dict] = []
    for c in target.values():
        cat = c.table_name()
        base = {
            "curve_name": c.name,
            "curve_type": c.curve_type.name,
            "data_type": c.data_type.name,
            "table_category": cat,
            "cluster_key": c.cluster_key(),
            "begin": begin_dt,
            "end": end_dt,
            "ensembles": False,
            "unit": c.unit,
            "mode": config.mode,
            "created_at": now,
        }
        base["request_id"] = f"{c.name}|{begin_dt.isoformat()}|{end_dt.isoformat()}"
        rows.append(base)

        if c.curve_type == CurveType.INSTANCE:
            ens = {**base, "ensembles": True}
            ens["request_id"] = f"{c.name}|ensembles|{begin_dt.isoformat()}|{end_dt.isoformat()}"
            rows.append(ens)

        if c.unit and any(cu in c.unit for cu in FOREIGN_UNITS):
            eur_unit = c.unit
            for cu in FOREIGN_UNITS:
                eur_unit = eur_unit.replace(cu, "EUR")
            eur = {**base, "unit": eur_unit}
            eur["request_id"] = f"{c.name}|{eur_unit}|{begin_dt.isoformat()}|{end_dt.isoformat()}"
            rows.append(eur)

    requests_df = pl.DataFrame(rows, schema=PENDING_REQUESTS_SCHEMA.to_polars_schema())
    pending = client.pending_requests_table(table_name=config.pending_table)
    pending.insert(
        requests_df,
        mode=Mode.UPSERT,
        match_by=["request_id"],
        predicate=col("request_id").is_in(requests_df["request_id"].to_list()),
    )

    categories = sorted({r["table_category"] for r in rows})
    logger.info(
        "Queued %d rows across %d categories into %s",
        len(rows), len(categories), pending.name,
    )

if not categories:
    dbutils.notebook.exit("no_categories")  # noqa: F821

# Resolve, per category, the explicit in-scope curve ids to forward to that
# category's worker. Only populated when a curve_ids filter is active: each
# worker then ingests exactly those curves. With no filter the mapping is empty,
# so workers receive an empty curve_ids and fall back to their default —
# fetching every curve in the category (or every queued curve in scheduled mode).
category_curve_ids: dict[str, list[str]] = {}
if requested_ids:
    for c in curvemap.values():
        if _selected(c):
            category_curve_ids.setdefault(c.table_name(), []).append(str(c.id))


def _curve_ids_param(category: str) -> str:
    return ",".join(category_curve_ids.get(category, []))


# COMMAND ----------

# DBTITLE 1,Phase 2 — get/create one cluster per cluster_key (parallel)
# Group the in-scope categories by their coarse cluster_key so we provision one
# shared all-purpose cluster per (data_type, curve_type) instead of one per
# table_category — the latter would spawn an unbounded number of clusters.
cluster_key_categories: dict[str, list[str]] = {}
for cat in categories:
    ck = cat_to_cluster_key.get(cat, cat)
    cluster_key_categories.setdefault(ck, []).append(cat)

logger.info(
    "Dispatching %d categories across %d cluster keys",
    len(categories), len(cluster_key_categories),
)


# Fault-tolerant: a failure provisioning one cluster_key's cluster is logged and
# skipped so the remaining cluster keys still get dispatched.
def _get_or_create_cluster(cluster_key: str):
    try:
        cluster = client.databricks.clusters.get_or_create(
            cluster_name=f"monteleq-ingest-{cluster_key}",
            custom_tags={"package": "monteleq", "cluster_key": cluster_key},
            libraries=["energyquantified"],
            wait=False,
        ).start(wait=False)
        return cluster_key, cluster
    except Exception:
        logger.exception("Cluster get/create failed for cluster_key=%s", cluster_key)
        return cluster_key, None


with ThreadPoolExecutor(max_workers=config.max_clusters) as executor:
    clusters = {
        ck: cluster
        for ck, cluster in executor.map(_get_or_create_cluster, cluster_key_categories)
        if cluster is not None
    }

failed = sorted(set(cluster_key_categories) - set(clusters))
if failed:
    logger.warning("Cluster provisioning failed for %d cluster keys: %s", len(failed), failed)
logger.info("Started %d clusters: %s", len(clusters), sorted(clusters))

if not clusters:
    dbutils.notebook.exit("no_clusters")  # noqa: F821

# COMMAND ----------

# DBTITLE 1,Phase 3 — dispatch ingest_category jobs
end_date_iso = end_dt.isoformat()
ingest_notebook = f"{config.notebook_root}/ingest_category"

dispatched: list[dict] = []
dispatch_errors: list[str] = []

for cluster_key, cats in sorted(cluster_key_categories.items()):
    cluster = clusters.get(cluster_key)
    if cluster is None:
        # Cluster provisioning failed in Phase 2; skip all its categories.
        dispatch_errors.extend(cats)
        continue

    for category in sorted(cats):
        try:
            run = client.databricks.jobs.submit(
                run_name=f"monteleq-ingest-{category}-{run_id}",
                timeout_seconds=3600,
                raise_error=False,
                tasks=[
                    SubmitTask(
                        task_key=f"ingest_{_safe_name(category)}",
                        existing_cluster_id=cluster.cluster_id,
                        timeout_seconds=3600,
                        notebook_task=NotebookTask(
                            notebook_path=ingest_notebook,
                            base_parameters={
                                "table_category": category,
                                "curve_ids": _curve_ids_param(category),
                                "end_date": end_date_iso,
                                "seconds": str(config.seconds),
                                "batch_size": str(config.batch_size),
                                "catalog_name": config.catalog_name,
                                "schema_name": config.schema_name,
                                "mode": config.mode,
                                "pending_table": config.pending_table,
                            },
                        ),
                    )
                ],
            )
        except Exception:
            logger.exception("Dispatch failed for category=%s", category)
            dispatch_errors.append(category)
            continue

        dispatched.append(
            {
                "table_category": category,
                "cluster_key": cluster_key,
                "cluster_id": cluster.cluster_id,
                "run_id": run.run_id,
            }
        )
        logger.info(
            "Dispatched %s -> cluster_key=%s cluster=%s run=%s",
            category, cluster_key, cluster.cluster_id, run.run_id,
        )

if dispatch_errors:
    logger.warning(
        "Dispatch failed for %d categories: %s",
        len(dispatch_errors), sorted(dispatch_errors),
    )

print(f"Dispatched {len(dispatched)} category jobs across {len(clusters)} clusters")

# COMMAND ----------

# DBTITLE 1,Output for downstream tracking
output = [
    {
        "table_category": d["table_category"],
        "cluster_key": d["cluster_key"],
        "cluster_id": d["cluster_id"],
        "run_id": str(d["run_id"]),
        "end_date": end_date_iso,
        "seconds": str(config.seconds),
    }
    for d in dispatched
]

dbutils.jobs.taskValues.set(key="dispatched", value=output)  # noqa: F821
dbutils.notebook.exit(json.dumps(output))  # noqa: F821
