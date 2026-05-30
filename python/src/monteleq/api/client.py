"""
monteleq.api.client
===================

Main entry-point for the MontelEQ / EnergyQuantified API.
"""
from __future__ import annotations

import datetime as dt
import logging
import time
from typing import Iterable, Any, Iterator, TYPE_CHECKING

import polars
from yggdrasil.http_.send_config import SendConfig

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
import pyarrow as pa
from yggdrasil.data.cast import any_to_datetime, truncate_datetime
from yggdrasil.enums import Mode
from yggdrasil.execution.expr.builder import col as expr_col
from yggdrasil.io import URL
from yggdrasil.io.response import Response

from monteleq.api._base_client import BaseClient
from monteleq.api.curation_client import CurationClient
from monteleq.api.events_client import EventsClient
from monteleq.api.metadata_client import MetadataClient
from monteleq.api.request import CurveRequest, CurveRequestsArg
from monteleq.api.schemas import CURATED_DATA_SCHEMA
from monteleq.model import Curve, Instance, DEFAULT_ISSUE_INTERVAL

FOREIGN_UNITS: tuple[str, ...] = ("GBP", "USD")

__all__ = ["APIClient"]

logger = logging.getLogger(__name__)


def _get_spark() -> "SparkSession | None":
    try:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession()
    except Exception:
        return None


def _curve_id_predicate(curve_ids: tuple[int, ...]) -> Any:
    return expr_col("curve_id").is_in(curve_ids)


class APIClient(BaseClient):
    """Authenticated client for the MontelEQ / EnergyQuantified API."""

    def __init__(
        self,
        base_url: URL | str | None = None,
        *,
        catalog_name: str | None = None,
        schema_name: str | None = None,
        mode: str | None = None,
        verify: bool = True,
        pool_maxsize: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(
            base_url,
            catalog_name=catalog_name,
            schema_name=schema_name,
            mode=mode,
            verify=verify,
            pool_maxsize=pool_maxsize,
            **kwargs,
        )
        self.metadata = MetadataClient(self)
        self.events = EventsClient(self)
        self.curation = CurationClient(self)

    # ------------------------------------------------------------------
    # Referential / queue helpers (dispatcher + worker orchestration)
    # ------------------------------------------------------------------

    def refresh_metadata(
        self,
        *,
        table_name: str = "curated_curve_metadata",
        mode: Mode | str = Mode.UPSERT,
    ) -> tuple[Any, polars.DataFrame]:
        """Refresh the curve-metadata referential from the live catalog.

        Upserts one row per curve into ``<schema>.<table_name>`` and returns
        the ``(table, dataframe)`` pair.  Raises if the catalog is empty.
        """
        from monteleq.api.schemas import CURVE_METADATA_SCHEMA

        df = self.metadata.metadata_df()
        if df.height == 0:
            raise RuntimeError("refresh_metadata: no curves found")

        resolved = mode if isinstance(mode, Mode) else self._resolve_insert_mode(mode)
        table = self.sql.table(table_name=table_name).ensure_created(CURVE_METADATA_SCHEMA)
        table.insert(
            df,
            mode=resolved,
            match_by=["curve_id"],
            where=expr_col("curve_id").is_in(df["curve_id"].to_list()),
        )
        return table, df

    def pending_requests_table(self, *, table_name: str = "pending_requests") -> Any:
        """Return (creating if absent) the pending-requests queue Delta table."""
        from monteleq.api.schemas import PENDING_REQUESTS_SCHEMA

        return self.sql.table(table_name=table_name).ensure_created(PENDING_REQUESTS_SCHEMA)

    def categories(self) -> list[str]:
        """Sorted list of every table category in the current curve catalog."""
        return sorted({c.table_name() for c in self.metadata.curvemap.values()})

    def category_curves(self, category: str) -> list[Curve]:
        """All curves routed to ``category`` (i.e. ``curve.table_name() == category``)."""
        return [c for c in self.metadata.curvemap.values() if c.table_name() == category]

    @staticmethod
    def parse_curve_ids(curve_ids: str | Iterable[str] | None) -> set[str]:
        """Parse a comma-separated string (or iterable) of curve identifiers.

        Each token is matched in :meth:`select_curves` against either a
        curve's integer ``id`` (as a string) or its ``name``, so callers may
        pass either form interchangeably.
        """
        if not curve_ids:
            return set()
        if isinstance(curve_ids, str):
            return {t.strip() for t in curve_ids.split(",") if t.strip()}
        return {str(t).strip() for t in curve_ids if str(t).strip()}

    def select_curves(
        self,
        *,
        table_categories: Iterable[str] | None = None,
        curve_ids: Iterable[str] | None = None,
    ) -> list[Curve]:
        """Select catalog curves by ``table_category`` and/or curve id/name.

        Both filters are optional and combine with AND semantics: with no
        filters every catalog curve is returned; with both, a curve must be in
        one of ``table_categories`` *and* match one of ``curve_ids`` (by
        ``str(curve.id)`` or ``curve.name``).
        """
        cats = set(table_categories) if table_categories is not None else None
        ids = set(curve_ids) if curve_ids is not None else None

        out: list[Curve] = []
        for c in self.metadata.curvemap.values():
            if cats is not None and c.table_name() not in cats:
                continue
            if ids is not None and str(c.id) not in ids and c.name not in ids:
                continue
            out.append(c)
        return out

    def curve_requests(
        self,
        curves: Iterable[Curve],
        *,
        begin: dt.datetime,
        end: dt.datetime,
        issued_at_earliest: dt.datetime | None = None,
        raise_error: bool = False,
        foreign_units: Iterable[str] = FOREIGN_UNITS,
    ) -> list[CurveRequest]:
        """Build the fetch requests for a set of curves over ``[begin, end]``.

        Expands each curve into its variants: the base request, an additional
        ensembles request for ``INSTANCE`` curves, and an additional EUR-unit
        request for curves priced in a foreign currency (GBP/USD).
        """
        from energyquantified.metadata import CurveType

        foreign = tuple(foreign_units)
        requests: list[CurveRequest] = []
        for c in curves:
            base = CurveRequest(
                curve=c,
                begin=begin,
                end=end,
                issued_at_earliest=issued_at_earliest or begin,
                client=self,
                raise_error=raise_error,
            )
            requests.append(base)

            if c.curve_type == CurveType.INSTANCE:
                requests.append(base.copy(ensembles=True))

            if c.unit and any(cu in c.unit for cu in foreign):
                eur_unit = c.unit
                for cu in foreign:
                    eur_unit = eur_unit.replace(cu, "EUR")
                requests.append(base.copy(unit=eur_unit))

        return requests

    # ------------------------------------------------------------------
    # Instance listing
    # ------------------------------------------------------------------

    def list_instances(
        self,
        requests: CurveRequestsArg,
        *,
        begin: dt.datetime | str | None = None,
        end: dt.datetime | str | None = None,
        issued_at_earliest: dt.datetime | str | None = None,
        issued_at_latest: dt.datetime | str | None = None,
        raise_error: bool = True,
    ) -> Iterator[Instance]:
        now = dt.datetime.now(tz=dt.timezone.utc)

        for request in CurveRequest.iterate(
            requests,
            client=self,
            begin=begin,
            end=end,
            issued_at_earliest=issued_at_earliest,
            issued_at_latest=issued_at_latest,
            raise_error=raise_error,
        ):
            ial = (
                any_to_datetime(request.issued_at_latest, tz=dt.timezone.utc)
                if request.issued_at_latest else now
            )
            iae = (
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
                curve.name, endpoint, ial, iae, request.request_tags,
            )

            cursor = truncate_datetime(
                ial, interval=DEFAULT_ISSUE_INTERVAL, add_interval=True,
            )
            floor_earliest = (
                truncate_datetime(iae, interval=DEFAULT_ISSUE_INTERVAL)
                if iae is not None else None
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

                    if iae is not None and instance.issued_at < iae:
                        continue
                    if instance.issued_at > ial:
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

                next_cursor = truncate_datetime(oldest_in_batch, interval=DEFAULT_ISSUE_INTERVAL)
                if next_cursor >= cursor:
                    next_cursor = cursor - DEFAULT_ISSUE_INTERVAL
                cursor = next_cursor

    # ------------------------------------------------------------------
    # Fetch raw HTTP responses
    # ------------------------------------------------------------------

    def fetch_curves(
        self,
        requests: CurveRequestsArg,
        *,
        begin: dt.datetime | str | None = None,
        end: dt.datetime | str | None = None,
        issued_at_earliest: dt.datetime | str | None = None,
        issued_at_latest: dt.datetime | str | None = None,
        raise_error: bool = True,
        spark: "SparkSession | bool | None" = None,
        batch_size: int | None = None,
    ) -> Iterator[Any]:
        spark_session = self._resolve_spark(spark)
        http_requests = CurveRequest.http_requests(
            requests,
            client=self,
            begin=begin,
            end=end,
            issued_at_earliest=issued_at_earliest,
            issued_at_latest=issued_at_latest,
            raise_error=raise_error,
        )

        def _stamp(reqs):
            for r in reqs:
                cfg: SendConfig = r.send_config_or_default
                overrides = {}
                if cfg.spark_session is not spark_session:
                    overrides["spark_session"] = spark_session
                if cfg.raise_error != raise_error:
                    overrides["raise_error"] = raise_error
                if overrides:
                    r.send_config = cfg.copy(**overrides)
                yield r

        yield from self.send_many_batches(
            _stamp(http_requests),
            batch_size=batch_size,
        )

    # ------------------------------------------------------------------
    # Curate: fetch + transform → curated DataFrames + optional insert
    # ------------------------------------------------------------------

    def curate_curves(
        self,
        requests: CurveRequestsArg,
        *,
        begin: dt.datetime | str | None = None,
        end: dt.datetime | str | None = None,
        issued_at_earliest: dt.datetime | str | None = None,
        issued_at_latest: dt.datetime | str | None = None,
        raise_error: bool = True,
        insert_all: bool = False,
        return_data: bool = False,
        spark: "SparkSession | bool | None" = None,
        batch_size: int | None = None,
        insert_mode: Mode | str | None = None,
        _stats: dict[str, int] | None = None,
    ) -> Iterator[Any]:
        resolved_mode = self._resolve_insert_mode(insert_mode)
        spark_session = self._resolve_spark(spark)
        use_spark = spark_session is not None

        for batch in self.fetch_curves(
            requests,
            begin=begin,
            end=end,
            issued_at_earliest=issued_at_earliest,
            issued_at_latest=issued_at_latest,
            raise_error=raise_error,
            spark=spark_session,
            batch_size=batch_size,
        ):
            if use_spark:
                yield from self._curate_batch_spark(
                    batch,
                    insert_all=insert_all,
                    return_data=return_data,
                    insert_mode=resolved_mode,
                    stats=_stats,
                )
            else:
                yield from self._curate_batch_polars(
                    batch,
                    insert_all=insert_all,
                    return_data=return_data,
                    insert_mode=resolved_mode,
                    stats=_stats,
                )

    # ------------------------------------------------------------------
    # Spark-distributed ingestion (convenience wrapper)
    # ------------------------------------------------------------------

    def ingest_spark(
        self,
        requests: CurveRequestsArg,
        *,
        spark: "SparkSession | bool | None" = True,
        raise_error: bool = False,
        batch_size: int | None = None,
        insert_all: bool = False,
        insert_mode: Mode | str | None = None,
    ) -> dict[str, int | float]:
        """Distributed fetch → curate → insert pipeline.

        When ``spark=True`` (default), auto-detects the active SparkSession.
        When ``spark=False`` or ``spark=None``, uses the Polars path.
        A SparkSession instance can be passed directly.

        ``insert_mode`` controls the write mode for curated Delta table
        inserts.  Accepts a ``Mode`` enum value or a string
        (``"append"``, ``"overwrite"``, ``"upsert"``).
        Defaults to ``Mode.APPEND``.
        """
        t0 = time.perf_counter()
        stats: dict[str, int | float] = {
            "batches": 0,
            "curated_rows": 0,
            "inserts": 0,
            "insert_failures": 0,
        }

        # ``curate_curves(return_data=False)`` yields nothing, but iterating it
        # to exhaustion still drives the fetch → curate → insert side effects
        # and the ``_stats`` accumulator below.
        for _ in self.curate_curves(
            requests,
            raise_error=raise_error,
            insert_all=insert_all,
            return_data=False,
            spark=spark,
            batch_size=batch_size,
            insert_mode=insert_mode,
            _stats=stats,
        ):
            pass

        stats["elapsed"] = round(time.perf_counter() - t0, 2)
        logger.info(
            "ingest complete: %d batches, %d curated rows, %d inserts, "
            "%d failures, %.2fs elapsed",
            stats["batches"], stats["curated_rows"], stats["inserts"],
            stats["insert_failures"], stats["elapsed"],
        )
        return stats

    # ------------------------------------------------------------------
    # Internal: Spark curate path
    # ------------------------------------------------------------------

    def _curate_batch_spark(
        self,
        batch: Any,
        *,
        insert_all: bool,
        return_data: bool,
        insert_mode: Mode = Mode.APPEND,
        stats: dict[str, int] | None = None,
    ) -> Iterator[Any]:
        if stats is not None:
            stats["batches"] = stats.get("batches", 0) + 1

        if insert_all:
            base = batch.read_spark_frame()
        elif batch.new is None:
            return
        else:
            base = batch.new.read_spark_frame()

        try:
            curated = self.curate_responses_spark(base).cache()
            row_count = curated.count()
        except Exception:
            logger.exception("Spark curation failed for batch")
            if stats is not None:
                stats["insert_failures"] = stats.get("insert_failures", 0) + 1
            return

        if stats is not None:
            stats["curated_rows"] = stats.get("curated_rows", 0) + row_count

        curve_names = [
            _["curve_name"]
            for _ in curated.select("curve_name").distinct().collect()
        ]
        cm = self.metadata.curvemap
        groups: dict[str, list[str]] = {}
        for n in curve_names:
            c = cm.get(n)
            if c is not None:
                tb = c.table_name(prefix="curated_")
                groups.setdefault(tb, []).append(n)

        for tb, names in groups.items():
            # ``Column.isin`` keeps quoting out of our hands — curve names may
            # contain characters (e.g. apostrophes) that break an interpolated
            # ``curve_name in (...)`` SQL predicate.
            sub = curated.filter(curated["curve_name"].isin(names))
            if sub.limit(1).count() == 0:
                continue
            try:
                curves = [cm[n] for n in names if n in cm]
                if not curves:
                    continue
                curve_ids = tuple(c.id for c in curves)
                self.curation.table(curves[0]).insert(
                    sub,
                    mode=insert_mode,
                    match_by=["curve_id", "curve_name", "run_hash", "from_timestamp"],
                    where=_curve_id_predicate(curve_ids),
                )
                if stats is not None:
                    stats["inserts"] = stats.get("inserts", 0) + 1
            except Exception:
                logger.exception("Insert failed for table %s", tb)
                if stats is not None:
                    stats["insert_failures"] = stats.get("insert_failures", 0) + 1

        if return_data:
            yield curated

    # ------------------------------------------------------------------
    # Internal: Polars curate path
    # ------------------------------------------------------------------

    def _curate_batch_polars(
        self,
        batch: Any,
        *,
        insert_all: bool,
        return_data: bool,
        insert_mode: Mode = Mode.APPEND,
        stats: dict[str, int] | None = None,
    ) -> Iterator[polars.DataFrame]:
        if stats is not None:
            stats["batches"] = stats.get("batches", 0) + 1

        if insert_all:
            responses = batch.iter_responses()
        elif batch.new is None:
            return
        else:
            responses = Response.from_records(batch.new.read_records())

        curated_parts: list[polars.DataFrame] = []
        for response in responses:
            if not response.ok:
                continue
            try:
                df = self.curation.curate(response)
            except Exception:
                logger.exception("Curation failed for response %s", response)
                if stats is not None:
                    stats["insert_failures"] = stats.get("insert_failures", 0) + 1
                continue
            if df.height == 0:
                continue
            curated_parts.append(df)

        if not curated_parts:
            return

        curated = (
            curated_parts[0]
            if len(curated_parts) == 1
            else polars.concat(curated_parts, how="diagonal_relaxed")
        )

        if stats is not None:
            stats["curated_rows"] = stats.get("curated_rows", 0) + curated.height

        cm = self.metadata.curvemap
        groups: dict[str, list[str]] = {}
        for n in curated["curve_name"].unique().to_list():
            c = cm.get(n)
            if c is not None:
                tb = c.table_name(prefix="curated_")
                groups.setdefault(tb, []).append(n)

        for tb, names in groups.items():
            sub = curated.filter(polars.col("curve_name").is_in(names))
            if sub.height == 0:
                continue
            try:
                curves = [cm[n] for n in names if n in cm]
                if not curves:
                    continue
                curve_ids = tuple(c.id for c in curves)
                self.curation.table(curves[0]).insert(
                    sub,
                    mode=insert_mode,
                    schema_mode=Mode.APPEND,
                    match_by=["curve_id", "curve_name", "run_hash", "from_timestamp"],
                    wait=False,
                    where=_curve_id_predicate(curve_ids),
                )
                if stats is not None:
                    stats["inserts"] = stats.get("inserts", 0) + 1
            except Exception:
                logger.exception("Insert failed for table %s", tb)
                if stats is not None:
                    stats["insert_failures"] = stats.get("insert_failures", 0) + 1

        if return_data:
            yield curated

    # ------------------------------------------------------------------
    # Resolve spark argument
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_spark(spark: "SparkSession | bool | None") -> "SparkSession | None":
        if spark is None or spark is False:
            return None
        if spark is True:
            return _get_spark()
        return spark

    @staticmethod
    def _resolve_insert_mode(insert_mode: "Mode | str | None") -> Mode:
        if insert_mode is None:
            return Mode.APPEND
        if isinstance(insert_mode, Mode):
            return insert_mode
        return Mode[insert_mode.strip().upper()]

    # ------------------------------------------------------------------
    # Spark: curate a DataFrame of Responses via mapInArrow
    # ------------------------------------------------------------------

    def curate_responses_spark(
        self,
        df: "SparkDataFrame",
        *,
        barrier: bool = False,
    ) -> "SparkDataFrame":
        """Curate a Spark DataFrame whose rows match ``RESPONSE_SCHEMA``."""
        spark_curated_schema = CURATED_DATA_SCHEMA.to_spark_schema()

        spark = df.sparkSession
        bc_client = spark.sparkContext.broadcast(self)
        bc_curvemap = spark.sparkContext.broadcast(self.metadata.curvemap)

        def _curate_partition(
            batches: Iterable[pa.RecordBatch],
        ) -> Iterator[pa.RecordBatch]:
            from yggdrasil.io.response import Response
            from monteleq.api.schemas import CURATED_DATA_SCHEMA

            client = bc_client.value
            client.metadata._curves = bc_curvemap.value
            curation = client.curation

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
