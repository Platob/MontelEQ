"""
monteleq.api.client
===================

Main entry-point for the Meteologica / EnergyQuantified API.

``APIClient`` extends ``BaseClient`` (which handles raw HTTP + auth) and wires
together a set of focused sub-clients, plus exposes high-level fetch/curate
methods for all curve types directly on itself:

.. code-block:: python

    client = APIClient()

    # Metadata
    client.metadata.curves(curve_type="TIMESERIES")

    # Low-level sub-client fetches (yield HTTPResponse)
    for resp in client.timeseries.fetch("Hydro NO Total >", begin="2024-01-01"):
        ...

    # High-level curate (yields curated DataFrames)
    for df in client.curate("Hydro NO Total >", begin="2024-01-01"):
        print(df.shape)

    # Events
    for event in client.events.stream():
        df = client.events.fetch(event)

    # Spark-distributed ingestion (new_hits only)
    client.ingest_spark(curves, spark_session=spark, begin=start, end=end)
"""
from __future__ import annotations

import datetime as dt
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Iterable, Any, Iterator, TYPE_CHECKING

import polars

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
import pyarrow as pa
from yggdrasil.data.cast import any_to_datetime, truncate_datetime
from yggdrasil.data.enums import Mode
from yggdrasil.environ import PyEnv
from yggdrasil.io import URL

from monteleq.api._base_client import BaseClient
from monteleq.api.curation_client import CurationClient
from monteleq.api.events_client import EventsClient
from monteleq.api.metadata_client import MetadataClient
from monteleq.api.request import CurveRequest, CurveRequestsArg
from monteleq.api.schemas import CURATED_DATA_SCHEMA
from monteleq.model import Curve, Instance, DEFAULT_ISSUE_INTERVAL

__all__ = ["APIClient"]

logger = logging.getLogger(__name__)


@dataclass
class APIClient(BaseClient):
    """
    Authenticated client for the Meteologica / EnergyQuantified API.

    Inherits all HTTP + auth infrastructure from ``BaseClient``, exposes
    focused sub-clients as attributes, and provides high-level curate methods
    for all curve types directly.
    """

    def __init__(
        self,
        base_url: URL | str | None = None,
        *,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        mode: str | None = None,
        **kwargs,
    ):
        super().__init__(
            base_url,
            catalog_name=catalog_name, schema_name=schema_name,
            mode=mode, **kwargs
        )
        self.metadata = MetadataClient(self)
        self.events = EventsClient(self)
        self.curation = CurationClient(self)

    # ------------------------------------------------------------------
    # Single-curve fetch (raw HTTPResponse generator)
    # ------------------------------------------------------------------

    def list_instances(
        self,
        requests: CurveRequest | Curve | str | Iterable[CurveRequest],
        **options
    ):
        now = dt.datetime.now(tz=dt.timezone.utc)

        for request in CurveRequest.iterate(requests, client=self, **options):
            issued_at_latest = (
                any_to_datetime(request.issued_at_latest, tz=dt.timezone.utc)
                if request.issued_at_latest else now
            )
            issued_at_earliest = (
                any_to_datetime(request.issued_at_earliest, tz=dt.timezone.utc)
                if request.issued_at_earliest else None
            )

            curve = request.curve
            endpoint = "instances" if curve.is_instance else "period-instances"
            safe_name = URL.path_encode(curve.name, safe='')
            url = f"{endpoint}/{safe_name}/list/"
            limit = 25 if curve.is_instance else 20

            params: dict[str, Any] = {"limit": limit}
            if request.request_tags:
                params["tags"] = request.request_tags

            logger.debug(
                "list_instances: curve=%s endpoint=%s latest=%s earliest=%s tags=%s",
                curve.name, endpoint, issued_at_latest, issued_at_earliest, request.request_tags,
            )

            # Snap cursor up to the next interval boundary so the cache key is stable
            # across calls within the same interval.
            cursor = truncate_datetime(
                issued_at_latest, interval=DEFAULT_ISSUE_INTERVAL, add_interval=True,
            )
            floor_earliest = (
                truncate_datetime(issued_at_earliest, interval=DEFAULT_ISSUE_INTERVAL)
                if issued_at_earliest is not None else None
            )

            seen: set[tuple[dt.datetime, str | None]] = set()

            while True:
                params["issued-at-latest"] = cursor

                batch = self.get(
                    url,
                    params=params,
                    local_cache=dt.timedelta(days=14) if cursor < now else None,
                    raise_error=False,
                )

                if not batch.ok:
                    logger.warning(
                        "list_instances: non-ok response for curve=%s cursor=%s status=%s",
                        curve.name, cursor, getattr(batch, "status_code", "?"),
                    )
                    break

                rows = batch.json()
                if not rows:
                    break

                oldest_in_batch: dt.datetime | None = None

                for resp in rows:
                    instance = Instance(
                        curve=curve,
                        issued_at=resp.get("issued"),
                        created_at=resp.get("created"),
                        modified_at=resp.get("modified"),
                        tag=resp.get("tag") or None,
                    )

                    if oldest_in_batch is None or instance.issued_at < oldest_in_batch:
                        oldest_in_batch = instance.issued_at

                    # Apply the *original*, untruncated user bounds when filtering yields
                    if issued_at_earliest is not None and instance.issued_at < issued_at_earliest:
                        continue
                    if instance.issued_at > issued_at_latest:
                        continue

                    key = (instance.issued_at, instance.tag)
                    if key in seen:
                        continue
                    seen.add(key)

                    yield instance

                if len(rows) < limit:
                    break
                if floor_earliest is not None and oldest_in_batch is not None \
                    and oldest_in_batch <= floor_earliest:
                    break
                if oldest_in_batch is None:
                    break

                # Snap the next cursor *down* to an interval boundary. This both stabilizes
                # the cache key and guarantees monotonic decrease (since rows[*].issued_at < cursor
                # and we floor strictly below cursor's boundary).
                next_cursor = truncate_datetime(oldest_in_batch, interval=DEFAULT_ISSUE_INTERVAL)
                if next_cursor >= cursor:
                    # oldest_in_batch landed on the same boundary as cursor — step one interval
                    # back to make progress.
                    next_cursor = cursor - DEFAULT_ISSUE_INTERVAL
                cursor = next_cursor

    def fetch_curves(
        self,
        requests: CurveRequestsArg,
        *,
        raise_error: bool = True,
        **options: dict[str, Any],
    ):
        # CurveRequest now IS-A PreparedRequest, so http_requests yields
        # objects send_many can consume directly — no .http_request() adapter.
        yield from self.send_many_batches(
            CurveRequest.http_requests(
                requests, client=self,
                **options
            ),
            raise_error=raise_error,
        )

    # ------------------------------------------------------------------
    # Single-curve curate (fetches + transforms → curated DataFrame)
    # ------------------------------------------------------------------

    def curate_curves(
        self,
        requests: CurveRequestsArg,
        *,
        raise_error: bool = True,
        max_workers: int | None = None,
        insert_all: bool = False,
        return_data: bool = False,
        **options: dict[str, Any],
    ):
        for batch in self.fetch_curves(
            requests,
            raise_error=raise_error,
            **options
        ):
            if PyEnv.in_databricks():
                if insert_all:
                    base = batch.to_dataframe()
                elif batch.new_hits is None:
                    continue
                else:
                    base = batch.new_hits.to_spark_frame()
                curated = self.curate_responses_spark(base).cache()
                # Materialize once so threads read from the cache instead of racing
                # to populate it
                curated.count()

                sc = curated.sparkSession.sparkContext
                sc.setLocalProperty("spark.scheduler.mode", "FAIR")

                curve_names = [
                    _["curve_name"]
                    for _ in curated.select("curve_name").distinct().collect()
                ]
                groups: dict[str, list[str]] = {}
                for n in curve_names:
                    tb = self.metadata.curves(name=n)[0].table_name(prefix="curated_")
                    groups.setdefault(tb, []).append(n)

                def _spark_insert(tb: str, names: list[str]) -> str:
                    sc.setLocalProperty("spark.scheduler.pool", tb)
                    sub = curated.filter(
                        f"curve_name in ({', '.join(repr(n) for n in names)})"
                    )
                    if sub.limit(1).count() == 0:
                        return tb
                    curves = self.metadata.curves(name=names)
                    curve_ids = {c.id for c in curves}
                    self.curation.table(curves[0]).insert(
                        sub,
                        mode=Mode.APPEND,
                        match_by=["curve_id", "curve_name", "run_hash", "from_timestamp"],
                        prune_by={"curve_id": curve_ids},
                    )
                    return tb

                if groups:
                    with ThreadPoolExecutor(max_workers=max_workers or len(groups)) as pool:
                        futures = {
                            pool.submit(_spark_insert, tb, names): tb
                            for tb, names in groups.items()
                        }
                        for fut in as_completed(futures):
                            fut.result()

                if return_data:
                    yield curated

            else:
                if insert_all:
                    responses = batch.iter_responses()
                else:
                    responses = batch.new_responses()
                curated = None
                for response in responses:
                    if not response.ok:
                        continue
                    df = self.curation.curate(response)
                    if df.height == 0:
                        continue
                    curated = df if curated is None else polars.concat(
                        [curated, df], how="diagonal_relaxed",
                    )

                if curated is None:
                    continue

                # Group curve_names by target curated_* table. Multiple curves may
                # share a physical table; collapsing first means one insert per
                # table, not one per curve.
                groups: dict[str, list[str]] = {}
                for n in curated["curve_name"].unique().to_list():
                    tb = self.metadata.curves(name=n)[0].table_name(prefix="curated_")
                    groups.setdefault(tb, []).append(n)

                def _polars_insert(tb: str, names: list[str]) -> str:
                    sub = curated.filter(polars.col("curve_name").is_in(names))
                    if sub.height == 0:
                        return tb
                    curves = self.metadata.curves(name=names)
                    self.curation.table(curves[0]).insert(
                        sub,
                        mode=Mode.APPEND,
                        schema_mode=Mode.APPEND,
                        match_by=["curve_id", "curve_name", "run_hash", "from_timestamp"],
                        wait=False,
                        prune_values={"curve_id": {c.id for c in curves}},
                    )
                    return tb

                if groups:
                    with ThreadPoolExecutor(max_workers=max_workers or len(groups)) as pool:
                        futures = {
                            pool.submit(_polars_insert, tb, names): tb
                            for tb, names in groups.items()
                        }
                        for fut in as_completed(futures):
                            fut.result()

                if return_data:
                    yield curated

                    for response in batch.local_responses():
                        if not response.ok:
                            continue
                        df = self.curation.curate(response)
                        if df.height == 0:
                            continue
                        yield df

                    for response in batch.remote_responses():
                        if not response.ok:
                            continue
                        df = self.curation.curate(response)
                        if df.height == 0:
                            continue
                        yield df

    # ------------------------------------------------------------------
    # Spark-distributed ingestion: fetch → curate new_hits → insert
    # ------------------------------------------------------------------

    def ingest_spark(
        self,
        requests: CurveRequestsArg,
        *,
        spark_session: "SparkSession",
        raise_error: bool = False,
        max_workers: int | None = None,
        batch_size: int | None = None,
        insert_all: bool = False,
    ) -> dict[str, int]:
        """Distributed fetch → curate → insert pipeline using Spark.

        Leverages ``send_many_batches(spark_session=...)`` to scatter HTTP
        calls across Spark executors via ``mapInArrow``.  Only ``new_hits``
        (freshly fetched responses not already in cache) are curated and
        inserted into the target ``curated_*`` Delta tables.

        Parameters
        ----------
        requests :
            Curves, curve names, or CurveRequest objects to ingest.
        spark_session :
            Active SparkSession (Databricks Connect or local).
        raise_error :
            Propagate HTTP errors instead of skipping.
        max_workers :
            Thread pool size for parallel table inserts.
        batch_size :
            Number of requests per send_many_batches batch.
        insert_all :
            When True, curate and insert all hits (local + remote + new).
            Default False curates only new_hits.

        Returns
        -------
        dict
            ``{"fetched": N, "curated": N, "tables": N, "elapsed": seconds}``
        """
        t0 = time.perf_counter()
        stats: dict[str, int] = {"fetched": 0, "curated": 0, "tables": 0}

        prepared = CurveRequest.http_requests(
            requests, client=self,
            raise_error=raise_error,
        )

        send_kwargs: dict[str, Any] = {
            "raise_error": raise_error,
            "spark_session": spark_session,
        }
        if batch_size is not None:
            send_kwargs["batch_size"] = batch_size

        for batch in self.send_many_batches(prepared, **send_kwargs):
            stats["fetched"] += 1

            if insert_all:
                base = batch.to_dataframe()
            elif batch.new_hits is None:
                continue
            else:
                base = batch.new_hits.to_spark_frame()

            curated = self.curate_responses_spark(base).cache()
            row_count = curated.count()
            if row_count == 0:
                continue

            stats["curated"] += row_count

            sc = curated.sparkSession.sparkContext
            sc.setLocalProperty("spark.scheduler.mode", "FAIR")

            curve_names = [
                _["curve_name"]
                for _ in curated.select("curve_name").distinct().collect()
            ]
            groups: dict[str, list[str]] = {}
            for n in curve_names:
                matches = self.metadata.curves(name=n)
                if matches:
                    tb = matches[0].table_name(prefix="curated_")
                    groups.setdefault(tb, []).append(n)

            def _insert(tb: str, names: list[str]) -> str:
                sc.setLocalProperty("spark.scheduler.pool", tb)
                sub = curated.filter(
                    f"curve_name in ({', '.join(repr(n) for n in names)})"
                )
                if sub.limit(1).count() == 0:
                    return tb
                curves = self.metadata.curves(name=names)
                curve_ids = {c.id for c in curves}
                self.curation.table(curves[0]).insert(
                    sub,
                    mode=Mode.APPEND,
                    match_by=["curve_id", "curve_name", "run_hash", "from_timestamp"],
                    prune_by={"curve_id": curve_ids},
                )
                return tb

            if groups:
                pool_size = max_workers or len(groups)
                with ThreadPoolExecutor(max_workers=pool_size) as pool:
                    futures = {
                        pool.submit(_insert, tb, names): tb
                        for tb, names in groups.items()
                    }
                    for fut in as_completed(futures):
                        try:
                            fut.result()
                            stats["tables"] += 1
                        except Exception:
                            logger.exception(
                                "Insert failed for table %s", futures[fut]
                            )

        stats["elapsed"] = round(time.perf_counter() - t0, 2)
        logger.info(
            "ingest_spark complete: %d batches fetched, %d rows curated, "
            "%d tables written in %.2fs",
            stats["fetched"], stats["curated"],
            stats["tables"], stats["elapsed"],
        )
        return stats

    # ------------------------------------------------------------------
    # Spark: curate a DataFrame of Responses via mapInArrow
    # ------------------------------------------------------------------

    def curate_responses_spark(
        self,
        df: "SparkDataFrame",
        *,
        barrier: bool = False,
    ) -> "SparkDataFrame":
        """Curate a Spark DataFrame whose rows match ``RESPONSE_SCHEMA``.

        Each partition is reconstructed into ``Response`` objects via
        :meth:`Response.from_arrow_tabular`, fed through
        :meth:`CurationClient.curate`, and emitted as Arrow batches
        matching ``curated_schema``.

        Parameters
        ----------
        df:
            Input Spark DataFrame. Its schema must be a superset of
            ``RESPONSE_SCHEMA`` — extra columns are ignored, missing
            columns surface as ``None`` per ``from_arrow_tabular``.
        barrier:
            Forwarded to ``mapInArrow``. Leave ``False`` unless you need
            barrier-mode scheduling.

        Returns
        -------
        SparkDataFrame
            A Spark DataFrame with ``curated_schema``. Nothing is
            inserted; the caller writes wherever they want.

        Notes
        -----
        Responses are buffered *within each partition* per ``curve_name``
        and flushed when the partition ends — the same per-curve concat-
        then-curate pattern as :meth:`curate_curves`, but bounded to a
        single partition rather than the whole stream. No cross-partition
        coordination; partitions curate independently.
        """
        spark_curated_schema = CURATED_DATA_SCHEMA.to_spark_schema()

        ser_client = self
        cm = self.metadata.curvemap

        def _curate_partition(
            batches: Iterable[pa.RecordBatch],
        ) -> Iterator[pa.RecordBatch]:
            from yggdrasil.io.response import Response
            from monteleq.api.schemas import CURATED_DATA_SCHEMA

            ser_client.metadata._curves = cm
            curation = ser_client.curation

            for batch in batches:
                for resp in Response.from_arrow_tabular(batch, normalize=False):
                    if resp.ok:
                        curated = curation.curate(resp)
                        if curated.height == 0:
                            continue
                        yield from CURATED_DATA_SCHEMA.cast_arrow(curated.to_arrow()).to_batches()

        return df.mapInArrow(
            _curate_partition,
            schema=spark_curated_schema,
            barrier=barrier,
        )