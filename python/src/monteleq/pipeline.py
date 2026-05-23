"""
monteleq.pipeline
=================

Databricks ingestion entry points, parallelized by curve category.

Each curve category (Production, Consumption, Exchange, Price, …) runs
as a separate task on the shared cluster, fetching raw HTTP responses via
Spark-distributed ``send_many_batches(spark_session=spark)`` and inserting
curated ``new_hits`` into the corresponding ``curated_*`` Delta tables.

Usage from a Databricks notebook or task::

    from monteleq.pipeline import ingest_category
    ingest_category("Production")
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Optional

logger = logging.getLogger(__name__)

CATALOG_NAME = "trading_tgp_prd"
SCHEMA_NAME = "src_monteleq"
DEFAULT_PERIOD_DAYS = 60


def ingest_category(
    curve_category: str,
    *,
    catalog_name: str = CATALOG_NAME,
    schema_name: str = SCHEMA_NAME,
    period_days: int = DEFAULT_PERIOD_DAYS,
    issued_at_lookback_days: Optional[int] = None,
) -> dict:
    """Ingest all curves matching ``curve_category`` using Spark-distributed HTTP.

    Filters curves whose ``categories`` tuple contains ``curve_category``,
    then runs ``APIClient.ingest_spark()`` which leverages yggdrasil's
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
        "Starting ingestion: category=%s begin=%s end=%s",
        curve_category, begin, end,
    )

    client = APIClient(
        catalog_name=catalog_name,
        schema_name=schema_name,
    )

    curves = client.metadata.curves(categories=curve_category)
    if not curves:
        logger.warning("No curves found for category=%s", curve_category)
        return {"category": curve_category, "curves": 0, "status": "empty"}

    logger.info(
        "Found %d curves for category=%s, building requests",
        len(curves), curve_category,
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

    result = {"category": curve_category, "curves": len(curves), **stats}
    logger.info("Ingestion complete: %s", result)
    return result
