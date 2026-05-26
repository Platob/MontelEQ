# Databricks notebook source
# MAGIC %md
# MAGIC # MontelEQ — Plan
# MAGIC
# MAGIC Scheduled orchestrator for the MontelEQ ingestion pipeline:
# MAGIC
# MAGIC 1. Refreshes the curve metadata referential
# MAGIC 2. Scans curves and generates pending ingestion requests
# MAGIC 3. Groups table categories by `(data_type, curve_type)`
# MAGIC 4. Gets or creates a dedicated cluster per group
# MAGIC 5. Dispatches per-category ingestion jobs on the appropriate cluster

# COMMAND ----------

import datetime as dt
import json
import logging
import sys

sys.path.insert(0, "/Workspace/Shared/MontelEQ/python/src")

from yggdrasil.data.enums import Mode
from yggdrasil.environ.parameters import SystemParameters

logger = logging.getLogger(__name__)

# COMMAND ----------


class Config(SystemParameters):
    catalog_name: str = "trading_tgp_prd"
    schema_name: str = "src_monteleq"
    table_category: str = ""
    end_date: dt.datetime = "now"
    seconds: int = 3600
    mode: str = "append"
    events: bool = False
    events_checkpoint: str = "/dbfs/tmp/monteleq/plan-events-checkpoint.json"
    notebook_root: str = "/Workspace/Shared/MontelEQ/databricks/notebooks"

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
import polars as pl
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

# DBTITLE 1,Resolve target curves (events vs full scan)
from energyquantified.metadata import CurveType
from monteleq.api.schemas import PENDING_REQUESTS_SCHEMA
from monteleq.model import _safe_name

curvemap = client.metadata.curvemap
category_set = set(table_categories)

if config.events:
    target_curves: dict[str, object] = {}
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
            if curve and curve.table_name() in category_set:
                target_curves[curve.name] = curve

    print(f"Events mode: {len(target_curves)} curves with updates")

    if not target_curves:
        print("No curve updates, nothing to dispatch")
        dbutils.notebook.exit(json.dumps([]))  # noqa: F821

    table_categories = sorted({c.table_name() for c in target_curves.values()})
else:
    target_curves = {
        c.name: c
        for c in curvemap.values()
        if c.table_name() in category_set
    }

print(f"Target: {len(target_curves)} curves across {len(table_categories)} categories")

# COMMAND ----------

# DBTITLE 1,Generate pending requests
FOREIGN_UNITS = {"GBP", "USD"}

now = dt.datetime.now(dt.timezone.utc)
request_rows: list[dict] = []

for c in target_curves.values():
    cat = c.table_name()
    cluster_key = c.cluster_key()
    base_row = {
        "curve_name": c.name,
        "curve_type": c.curve_type.name,
        "data_type": c.data_type.name,
        "table_category": cat,
        "cluster_key": cluster_key,
        "begin": begin_dt,
        "end": end_dt,
        "ensembles": False,
        "unit": c.unit,
        "mode": config.mode,
        "created_at": now,
    }
    base_row["request_id"] = f"{c.name}|{begin_dt.isoformat()}|{end_dt.isoformat()}"
    request_rows.append(base_row)

    if c.curve_type == CurveType.INSTANCE:
        ens_row = {**base_row, "ensembles": True}
        ens_row["request_id"] = f"{c.name}|ensembles|{begin_dt.isoformat()}|{end_dt.isoformat()}"
        request_rows.append(ens_row)

    if c.unit and any(cu in c.unit for cu in FOREIGN_UNITS):
        eur_unit = c.unit
        for cu in FOREIGN_UNITS:
            eur_unit = eur_unit.replace(cu, "EUR")
        eur_row = {**base_row, "unit": eur_unit}
        eur_row["request_id"] = f"{c.name}|{eur_unit}|{begin_dt.isoformat()}|{end_dt.isoformat()}"
        request_rows.append(eur_row)

requests_df = pl.DataFrame(request_rows, schema=PENDING_REQUESTS_SCHEMA.to_polars_schema())

pending_table = client.sql.table(table_name="pending_requests").ensure_created(
    PENDING_REQUESTS_SCHEMA
)
pending_table.insert(
    requests_df,
    mode=Mode.UPSERT,
    match_by=["request_id"],
    where=col("request_id").is_in(requests_df["request_id"].to_list()),
)

print(f"Inserted {requests_df.height} pending requests across {len(table_categories)} categories")

# COMMAND ----------

# DBTITLE 1,Group categories by (data_type, curve_type)
groups: dict[str, list[str]] = {}
for cat in table_categories:
    cluster_keys = {c.cluster_key() for c in target_curves.values() if c.table_name() == cat}
    ck = next(iter(cluster_keys), "unknown")
    groups.setdefault(ck, []).append(cat)

print(
    f"Grouped {len(table_categories)} categories into "
    f"{len(groups)} cluster groups: {sorted(groups.keys())}"
)

# COMMAND ----------

# DBTITLE 1,Get or create clusters (non-blocking)
clusters_svc = client.databricks.compute.clusters

group_clusters: dict[str, object] = {}

for cluster_key in sorted(groups):
    cluster_name = f"monteleq-{cluster_key}"
    cluster = clusters_svc.all_purpose_cluster(
        name=cluster_name,
        custom_tags={"package": "monteleq", "cluster_key": cluster_key},
        libraries=["energyquantified"],
    )
    cluster.start(wait=False)
    group_clusters[cluster_key] = cluster
    logger.info("Cluster %s (id=%s) starting", cluster_name, cluster.cluster_id)

print(f"Initiated {len(group_clusters)} clusters: {sorted(group_clusters)}")

# COMMAND ----------

# DBTITLE 1,Dispatch category jobs per cluster
from databricks.sdk.service.jobs import NotebookTask, SubmitTask

jobs_svc = client.databricks.compute.jobs

end_date_iso = end_dt.isoformat()
ingest_notebook = f"{config.notebook_root}/ingest_by_category"

dispatched_runs = []

for cluster_key, categories in sorted(groups.items()):
    cluster = group_clusters[cluster_key]

    tasks = [
        SubmitTask(
            task_key=cat.replace("-", "_"),
            existing_cluster_id=cluster.cluster_id,
            notebook_task=NotebookTask(
                notebook_path=ingest_notebook,
                base_parameters={
                    "table_category": cat,
                    "end_date": end_date_iso,
                    "seconds": str(config.seconds),
                    "catalog_name": config.catalog_name,
                    "schema_name": config.schema_name,
                    "mode": config.mode,
                },
            ),
        )
        for cat in categories
    ]

    run = jobs_svc.submit(
        run_name=f"monteleq-ingest-{cluster_key}",
        tasks=tasks,
    )

    dispatched_runs.append({
        "cluster_key": cluster_key,
        "cluster_id": cluster.cluster_id,
        "run_id": run.run_id,
        "categories": categories,
    })

    logger.info(
        "Dispatched run %s on cluster %s with %d category tasks",
        run.run_id, cluster.cluster_name, len(tasks),
    )

print(f"Dispatched {len(dispatched_runs)} job runs across {len(groups)} clusters")

# COMMAND ----------

# DBTITLE 1,Output for downstream tracking
output = [
    {
        "cluster_key": r["cluster_key"],
        "cluster_id": r["cluster_id"],
        "run_id": r["run_id"],
        "table_categories": ",".join(r["categories"]),
        "end_date": end_date_iso,
        "seconds": str(config.seconds),
    }
    for r in dispatched_runs
]

dbutils.jobs.taskValues.set(key="groups", value=output)  # noqa: F821
dbutils.notebook.exit(json.dumps(output))  # noqa: F821
