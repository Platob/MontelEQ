"""
monteleq.pipeline
=================

Databricks ingestion pipeline parallelized by curve data type.

Each ``DataType`` (ACTUAL, FORECAST, REMIT, …) runs as a separate task
on the same cluster, fetching raw HTTP responses via Spark-distributed
``send_many_batches(spark_session=spark)`` and inserting curated
``new_hits`` into the corresponding ``curated_*`` Delta tables.

Cluster: ``0522-063219-lfvirtho`` (shared, existing).

Usage from Databricks notebook::

    from monteleq.pipeline import run_pipeline
    run_pipeline()

Or deploy as a scheduled Databricks Job::

    from monteleq.pipeline import deploy_pipeline
    deploy_pipeline()
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Optional

from energyquantified.metadata import DataType

logger = logging.getLogger(__name__)

CLUSTER_ID = "0522-063219-lfvirtho"

CATALOG_NAME = "trading_tgp_prd"
SCHEMA_NAME = "src_monteleq"

DEFAULT_PERIOD_DAYS = 60

ALL_DATA_TYPES: list[str] = [
    dt_.name for dt_ in DataType
    if dt_.name not in ("NONE",)
]


def ingest_data_type(
    data_type: str,
    *,
    catalog_name: str = CATALOG_NAME,
    schema_name: str = SCHEMA_NAME,
    period_days: int = DEFAULT_PERIOD_DAYS,
    issued_at_lookback_days: Optional[int] = None,
) -> dict:
    """Ingest all curves of a given ``data_type`` using Spark-distributed HTTP.

    Designed to run inside a Databricks task on cluster ``0522-063219-lfvirtho``.
    Uses ``APIClient.ingest_spark()`` which leverages yggdrasil's
    ``send_many_batches(spark_session=spark)`` for distributed HTTP calls
    via ``mapInArrow``, with session remote cache for raw responses and
    batch insert of curated ``new_hits`` into Delta tables.
    """
    from pyspark.sql import SparkSession

    from monteleq.api.client import APIClient
    from monteleq.api.request import CurveRequest

    spark = SparkSession.builder.getOrCreate()

    now = dt.datetime.now(dt.timezone.utc)
    end = now
    begin = now - dt.timedelta(days=period_days)
    issued_at_earliest = (
        now - dt.timedelta(days=issued_at_lookback_days)
        if issued_at_lookback_days
        else begin
    )

    logger.info(
        "Starting ingestion: data_type=%s begin=%s end=%s issued_earliest=%s",
        data_type, begin, end, issued_at_earliest,
    )

    client = APIClient(
        catalog_name=catalog_name,
        schema_name=schema_name,
    )

    curves = client.metadata.curves(data_type=data_type)
    if not curves:
        logger.warning("No curves found for data_type=%s", data_type)
        return {"data_type": data_type, "curves": 0, "status": "empty"}

    logger.info(
        "Found %d curves for data_type=%s, building requests",
        len(curves), data_type,
    )

    requests = [
        CurveRequest(
            curve=c,
            begin=begin,
            end=end,
            issued_at_earliest=issued_at_earliest,
            client=client,
            raise_error=False,
        )
        for c in curves
    ]

    stats = client.ingest_spark(
        requests,
        spark_session=spark,
        raise_error=False,
    )

    result = {"data_type": data_type, "curves": len(curves), **stats}
    logger.info("Ingestion complete: %s", result)
    return result


def run_pipeline(
    *,
    data_types: Optional[list[str]] = None,
    catalog_name: str = CATALOG_NAME,
    schema_name: str = SCHEMA_NAME,
    period_days: int = DEFAULT_PERIOD_DAYS,
    issued_at_lookback_days: Optional[int] = None,
) -> list[dict]:
    """Run ingestion for all (or selected) data types sequentially.

    For parallel execution across data types, use ``deploy_pipeline()``
    to create a Databricks Job with one task per data type.
    """
    targets = data_types or ALL_DATA_TYPES
    results = []
    for dtype in targets:
        try:
            result = ingest_data_type(
                dtype,
                catalog_name=catalog_name,
                schema_name=schema_name,
                period_days=period_days,
                issued_at_lookback_days=issued_at_lookback_days,
            )
            results.append(result)
        except Exception:
            logger.exception("Failed to ingest data_type=%s", dtype)
            results.append({"data_type": dtype, "status": "error"})
    return results


def deploy_pipeline(
    *,
    job_name: str = "monteleq-ingestion",
    cluster_id: str = CLUSTER_ID,
    catalog_name: str = CATALOG_NAME,
    schema_name: str = SCHEMA_NAME,
    period_days: int = DEFAULT_PERIOD_DAYS,
    schedule_cron: str = "0 0 5 * * ?",
    schedule_tz: str = "UTC",
    data_types: Optional[list[str]] = None,
):
    """Deploy a Databricks Job with one parallel task per data type.

    All tasks run on existing cluster ``0522-063219-lfvirtho`` and execute
    in parallel (no inter-task dependencies). Each task calls
    :func:`ingest_data_type` for its assigned ``DataType``.

    Parameters
    ----------
    job_name :
        Databricks job display name.
    cluster_id :
        Existing cluster to pin all tasks to.
    catalog_name / schema_name :
        Databricks catalog/schema for the monteleq tables.
    period_days :
        Lookback window in days for data ingestion.
    schedule_cron :
        Quartz cron expression (default: daily at 05:00 UTC).
    schedule_tz :
        Timezone for the schedule.
    data_types :
        Subset of data types to deploy. None = all.
    """
    from yggdrasil.databricks import DatabricksClient

    client = DatabricksClient.current()
    targets = data_types or ALL_DATA_TYPES

    tasks = []
    for dtype in targets:
        task_key = f"ingest_{dtype.lower()}"
        tasks.append({
            "task_key": task_key,
            "existing_cluster_id": cluster_id,
            "spark_python_task": {
                "python_file": f"/Workspace/Shared/.ygg/jobs/{job_name}/{task_key}.py",
                "parameters": [
                    dtype,
                    catalog_name,
                    schema_name,
                    str(period_days),
                ],
            },
        })

    job = client.jobs.create_or_update(
        name=job_name,
        tasks=tasks,
        schedule={
            "quartz_cron_expression": schedule_cron,
            "timezone_id": schedule_tz,
        },
        max_concurrent_runs=1,
        tags={
            "package": "monteleq",
            "version": "0.2.0",
            "pipeline": "ingestion",
        },
    )

    _stage_task_scripts(
        client,
        job_name=job_name,
        targets=targets,
        catalog_name=catalog_name,
        schema_name=schema_name,
        period_days=period_days,
    )

    logger.info("Deployed job %s with %d tasks", job_name, len(tasks))
    return job


def _stage_task_scripts(
    client,
    *,
    job_name: str,
    targets: list[str],
    catalog_name: str,
    schema_name: str,
    period_days: int,
):
    """Write per-data-type Python scripts to the Databricks workspace."""
    for dtype in targets:
        task_key = f"ingest_{dtype.lower()}"
        script = _render_task_script(
            data_type=dtype,
            catalog_name=catalog_name,
            schema_name=schema_name,
            period_days=period_days,
        )
        path = f"/Workspace/Shared/.ygg/jobs/{job_name}/{task_key}.py"
        try:
            client.workspace.upload(
                path, script.encode("utf-8"), overwrite=True,
            )
            logger.info("Staged %s", path)
        except Exception:
            logger.exception("Failed to stage %s", path)


def _render_task_script(
    *,
    data_type: str,
    catalog_name: str,
    schema_name: str,
    period_days: int,
) -> str:
    return f'''\
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

from monteleq.pipeline import ingest_data_type

data_type = sys.argv[1] if len(sys.argv) > 1 else "{data_type}"
catalog_name = sys.argv[2] if len(sys.argv) > 2 else "{catalog_name}"
schema_name = sys.argv[3] if len(sys.argv) > 3 else "{schema_name}"
period_days = int(sys.argv[4]) if len(sys.argv) > 4 else {period_days}

result = ingest_data_type(
    data_type,
    catalog_name=catalog_name,
    schema_name=schema_name,
    period_days=period_days,
)
print(result)
'''
