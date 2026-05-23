"""
monteleq.pipeline
=================

Databricks ingestion pipeline parallelized by curve category.

The job runs hourly with two phases:

1. **plan** — fetches the curve catalog and returns the list of categories
   to ingest (by default all known categories).
2. **ingest_by_category** — one task per category, runs in parallel,
   each using Spark-distributed HTTP via ``mapInArrow``.

Static categories are derived from the EnergyQuantified catalog snapshot
(31k curves, 39 first-level categories).
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Optional

logger = logging.getLogger(__name__)

CATALOG_NAME = "trading_tgp_prd"
SCHEMA_NAME = "src_monteleq"
DEFAULT_PERIOD_DAYS = 60

CATEGORIES: list[str] = [
    "Asphaltite",
    "Battery",
    "Bioenergy",
    "Biogas",
    "Biomass",
    "Black",
    "CHP",
    "Capture",
    "Carbon",
    "Consumption",
    "Currency",
    "Derived",
    "Exchange",
    "Futures",
    "Gas",
    "Geothermal",
    "Hard",
    "Hydro",
    "Hydrology",
    "Imbalance",
    "Lignite",
    "Low-carbon",
    "Natural",
    "Net",
    "Nuclear",
    "Oil",
    "Other",
    "Peak-plant",
    "Peat",
    "Price",
    "Renewable",
    "Residual",
    "River",
    "Sensitivity",
    "Solar",
    "TB",
    "Volume",
    "Waste",
    "Wind",
]


def plan_categories(
    *,
    catalog_name: str = CATALOG_NAME,
    schema_name: str = SCHEMA_NAME,
) -> list[str]:
    """Fetch the curve catalog and return all distinct first-level categories.

    Falls back to the static ``CATEGORIES`` list if the API is unreachable.
    """
    try:
        from monteleq.api.client import APIClient

        client = APIClient(catalog_name=catalog_name, schema_name=schema_name)
        all_curves = client.metadata.curves()
        cats: set[str] = set()
        for c in all_curves:
            if c.categories:
                cats.add(c.categories[0])
        resolved = sorted(cats)
        logger.info("Plan: resolved %d categories from %d curves", len(resolved), len(all_curves))
        return resolved
    except Exception:
        logger.warning("Plan: catalog fetch failed, falling back to static categories")
        return list(CATEGORIES)


def ingest_category(
    curve_category: str,
    *,
    catalog_name: str = CATALOG_NAME,
    schema_name: str = SCHEMA_NAME,
    period_days: int = DEFAULT_PERIOD_DAYS,
    issued_at_lookback_days: Optional[int] = None,
    spark: bool = True,
    insert_mode: Optional[str] = None,
) -> dict:
    """Ingest all curves matching ``curve_category``.

    Parameters
    ----------
    spark :
        ``True`` (default) auto-detects the active SparkSession and uses
        distributed HTTP via ``mapInArrow``.  ``False`` forces the local
        Polars path even when Spark is available.
    insert_mode :
        Write mode for curated Delta table inserts.  Accepts
        ``"append"`` (default), ``"overwrite"``, or ``"upsert"``.
    """
    from monteleq.api.client import APIClient
    from monteleq.api.request import CurveRequest

    now = dt.datetime.now(dt.timezone.utc)
    end = now
    begin = now - dt.timedelta(days=period_days)
    issued_at_earliest = (
        now - dt.timedelta(days=issued_at_lookback_days)
        if issued_at_lookback_days
        else begin
    )

    logger.info("Starting ingestion: category=%s begin=%s end=%s spark=%s insert_mode=%s",
                curve_category, begin, end, spark, insert_mode or "append")

    client = APIClient(catalog_name=catalog_name, schema_name=schema_name)

    curves = client.metadata.curves(categories=curve_category)
    if not curves:
        logger.warning("No curves found for category=%s", curve_category)
        return {"category": curve_category, "curves": 0, "status": "empty"}

    logger.info("Found %d curves for category=%s", len(curves), curve_category)

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
        spark=spark,
        raise_error=False,
        insert_mode=insert_mode,
    )

    result = {"category": curve_category, "curves": len(curves), **stats}
    logger.info("Ingestion complete: %s", result)
    return result
