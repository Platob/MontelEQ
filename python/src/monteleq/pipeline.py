"""
monteleq.pipeline
=================

Databricks ingestion pipeline parallelized by curve category.

The job runs hourly with two phases:

1. **plan** — fetches the curve catalog and returns the list of categories
   to ingest (by default all known categories).
2. **ingest_by_category** — one task per category, runs in parallel,
   each using Spark-distributed HTTP via ``mapInArrow``.

Supports two modes via the ``latest`` job parameter:

* **latest=True** (default, scheduled) — uses ``period_hours`` as a
  lookback window from now.
* **latest=False** (manual backfill) — uses explicit ``start``/``end``
  datetime range.  ``insert_mode`` can be set to ``overwrite`` to
  replace curated data for the window.

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
DEFAULT_PERIOD_HOURS = 1

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


def refresh_curve_metadata(
    *,
    catalog_name: str = CATALOG_NAME,
    schema_name: str = SCHEMA_NAME,
    table_name: str = "curated_curve_metadata",
) -> int:
    """Upsert the full curve catalog into the ``curated_curve_metadata`` table.

    Each row carries a ``table_category`` field that maps the curve to
    its destination curated Delta table (e.g.
    ``curated_actual_timeseries_wind``).

    Returns the number of curves written.
    """
    import polars as pl
    from yggdrasil.data.enums import Mode

    from monteleq.api.client import APIClient
    from monteleq.api.schemas import CURVE_METADATA_SCHEMA

    client = APIClient(catalog_name=catalog_name, schema_name=schema_name)
    curves = client.metadata.curves()

    if not curves:
        logger.warning("refresh_curve_metadata: no curves found")
        return 0

    now = dt.datetime.now(dt.timezone.utc)

    df = pl.DataFrame({
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
    }, schema={
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
    })

    from yggdrasil.execution.expr.builder import col

    curve_ids = tuple(c.id for c in curves)

    table = client.sql.table(table_name=table_name).ensure_created(
        CURVE_METADATA_SCHEMA
    )
    table.insert(
        df,
        mode=Mode.APPEND,
        match_by=["curve_id"],
        where=col("curve_id").is_in(curve_ids),
        prune_by="auto",
    )

    logger.info(
        "refresh_curve_metadata: upserted %d curves into %s (%d distinct tables)",
        len(curves), table_name, df["table_category"].n_unique(),
    )
    return len(curves)


def ingest_category(
    curve_category: str,
    *,
    catalog_name: str = CATALOG_NAME,
    schema_name: str = SCHEMA_NAME,
    latest: bool = True,
    start: Optional[str] = None,
    end: Optional[str] = None,
    period_hours: int = DEFAULT_PERIOD_HOURS,
    issued_at_lookback_days: Optional[int] = None,
    spark: bool = True,
    insert_mode: Optional[str] = None,
) -> dict:
    """Ingest all curves matching ``curve_category``.

    Parameters
    ----------
    latest :
        ``True`` (default, scheduled) — lookback ``period_hours`` from
        now.  ``False`` (manual backfill) — use ``start``/``end``.
    start / end :
        Explicit ISO-8601 datetime boundaries when ``latest=False``.
    period_hours :
        Number of hours to look back when ``latest=True``.
    insert_mode :
        Write mode for curated Delta table inserts.  Accepts
        ``"append"`` (default), ``"overwrite"``, or ``"upsert"``.
    spark :
        ``True`` (default) auto-detects the active SparkSession and uses
        distributed HTTP via ``mapInArrow``.  ``False`` forces the local
        Polars path even when Spark is available.
    """
    from monteleq.api.client import APIClient
    from monteleq.api.request import CurveRequest

    now = dt.datetime.now(dt.timezone.utc)

    if latest:
        end_dt = now
        begin_dt = now - dt.timedelta(hours=period_hours)
    else:
        begin_dt = _parse_dt(start)
        end_dt = _parse_dt(end)
        if begin_dt is None:
            raise ValueError("`start` is required when latest=False")
        if end_dt is None:
            end_dt = now

    issued_at_earliest = (
        now - dt.timedelta(days=issued_at_lookback_days)
        if issued_at_lookback_days
        else begin_dt
    )

    logger.info(
        "Starting ingestion: category=%s begin=%s end=%s latest=%s insert_mode=%s spark=%s",
        curve_category, begin_dt, end_dt, latest, insert_mode or "append", spark,
    )

    client = APIClient(catalog_name=catalog_name, schema_name=schema_name)

    curves = client.metadata.curves(categories=curve_category)
    if not curves:
        logger.warning("No curves found for category=%s", curve_category)
        return {"category": curve_category, "curves": 0, "status": "empty"}

    logger.info("Found %d curves for category=%s", len(curves), curve_category)

    requests = [
        CurveRequest(
            curve=c,
            begin=begin_dt,
            end=end_dt,
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
