"""CurationClient – response curation and Databricks read-back only."""
from __future__ import annotations

import ctypes
import datetime as dt
from typing import Optional, TYPE_CHECKING

import polars as pl
import xxhash
from yggdrasil.data.cast import any_to_datetime
from yggdrasil.data.enums.timezone import Timezone
from yggdrasil.http_ import HTTPResponse

from monteleq.api.curation_helpers import (
    GAS_DAY_TZ,
    iso_duration_to_seconds_expr,
    make_data,
    normalize_ohlc_frame,
    parse_unit_columns_expr,
    reorder_columns,
    timestamp_utc_expr,
)
from monteleq.api.schemas import CURATED_DATA_SCHEMA, FINAL_SCHEMA
from monteleq.model import Curve

if TYPE_CHECKING:
    from monteleq.api.client import APIClient

__all__ = ["CurationClient"]


def _xxh3_64_signed(value: str) -> int:
    """Return signed int64 xxh3_64 hash of a UTF-8 string."""
    return ctypes.c_int64(xxhash.xxh3_64_intdigest(value.encode("utf-8"))).value


def _canonical_hash_component(df: pl.DataFrame, col_name: str) -> pl.Expr:
    """
    Return a canonical UTF-8-safe string expression for hashing.

    Rules:
    - null -> ""
    - Datetime -> UTC ISO-8601 with fixed microsecond precision, trailing Z
    - Date -> YYYY-MM-DD
    - all else -> Utf8 cast
    """
    dtype = df.schema[col_name]
    expr = pl.col(col_name)

    if isinstance(dtype, pl.Datetime):
        if dtype.time_zone == "UTC":
            expr = expr.dt.strftime("%Y-%m-%dT%H:%M:%S.%6fZ")
        elif dtype.time_zone is not None:
            expr = expr.dt.convert_time_zone("UTC").dt.strftime("%Y-%m-%dT%H:%M:%S.%6fZ")
        else:
            expr = expr.dt.replace_time_zone("UTC").dt.strftime("%Y-%m-%dT%H:%M:%S.%6fZ")
    elif dtype == pl.Date:
        expr = expr.dt.strftime("%Y-%m-%d")
    else:
        expr = expr.cast(pl.Utf8, strict=False)

    return expr.fill_null("")


def _canonical_struct_json_expr(df: pl.DataFrame, cols: list[str]) -> pl.Expr:
    """
    Build a canonical JSON string from selected columns for stable hashing.

    Using JSON avoids delimiter-collision issues from ad-hoc string joins.
    """
    return pl.struct(
        [_canonical_hash_component(df, col).alias(col) for col in cols]
    ).struct.json_encode()


def _stable_xxh3_hash_expr(df: pl.DataFrame, cols: list[str]) -> pl.Expr:
    """Return signed Int64 xxh3_64 hash expression over canonical JSON payload."""
    return _canonical_struct_json_expr(df, cols).map_elements(
        _xxh3_64_signed,
        return_dtype=pl.Int64,
    )


class CurationClient:
    """
    Pure curation client – transforms raw API responses into the curated schema
    and provides Databricks Delta table read-back.

    Usage via ``APIClient``::

        # Curate a raw HTTP response from any sub-client
        df = client.curation.curate(resp)

        # Read directly from the Databricks curated table (no API call)
        df = client.curation.query_serie("Hydro NO Total >", begin="2024-01-01")

        # Access the Databricks Delta table object for a curve
        tbl = client.curation.table(curve)
    """

    def __init__(self, api: "APIClient") -> None:
        self._api = api

    # ------------------------------------------------------------------
    # Curvemap defaults helper
    # ------------------------------------------------------------------

    def _curvemap_defaults(self, curve_names: list[str]) -> pl.DataFrame:
        """
        Build a one-row-per-curve-name DataFrame of metadata defaults,
        to be left-joined onto the response for null-filling.
        """
        rows = []
        for name in curve_names:
            info = self._api.metadata.curvemap.get(name)
            if info is None:
                continue
            rows.append({
                "curve_name": name,
                "_d_unit": info.unit,
                "_d_denominator": info.denominator,
                "_d_resolution_frequency_iso": info.resolution.frequency or "",
                "_d_resolution_timezone": info.resolution.timezone,
            })

        if not rows:
            return pl.DataFrame()

        df = pl.DataFrame(
            rows,
            schema={
                "curve_name": pl.Utf8,
                "_d_unit": pl.Utf8,
                "_d_denominator": pl.Utf8,
                "_d_resolution_frequency_iso": pl.Utf8,
                "_d_resolution_timezone": pl.Utf8,
            },
        )

        return df.with_columns(
            _d_resolution_frequency=iso_duration_to_seconds_expr("_d_resolution_frequency_iso"),
        ).drop("_d_resolution_frequency_iso")

    # ------------------------------------------------------------------
    # Table helper
    # ------------------------------------------------------------------

    def table(self, curve: Curve, prefix: str = "curated_"):
        """Return the Databricks Delta table for a curated curve dataset (creates if absent)."""
        if not isinstance(curve, Curve):
            curve = self._api.metadata.curves(name=curve)[0]

        return self._api.sql.table(
            table_name=curve.table_name(prefix=prefix)
        ).ensure_created(CURATED_DATA_SCHEMA)

    # ------------------------------------------------------------------
    # Curation transform
    # ------------------------------------------------------------------

    def curate(
        self,
        response: HTTPResponse | pl.DataFrame | pl.LazyFrame,
        *,
        begin: Optional[dt.datetime | dt.date | str] = None,
        end: Optional[dt.datetime | dt.date | str] = None,
        **media_options,
    ) -> pl.DataFrame:
        """
        Transform a raw API response (or DataFrame) into the curated schema.

        Parameters
        ----------
        response :
            Raw HTTP response, or a pre-parsed DataFrame / LazyFrame.
        begin / end :
            Optional UTC-aware datetime boundaries to **filter** the curated
            data rows. Only data whose ``from_timestamp`` falls within
            ``[begin, end)`` is kept. Accepts ``datetime``, ``date``, or
            an ISO-format string. When ``None`` the corresponding bound
            is unbounded.

        Steps:
        1. Parse HTTP response → Polars DataFrame when needed.
        2. Unnest ``resolution``, ``unit``, ``instance``, ``curve``,
           ``curve_access``, and ``curve_resolution`` struct columns.
        3. Handle scenario timeseries (zip ``scenario_names`` × ``data_s``).
        4. Normalise data rows via ``make_data``.
        5. **Filter** rows by ``[begin, end)`` on ``data.from_timestamp``
           when either bound is supplied.
        6. Group by logical series identity and aggregate ``data`` into lists.
        """
        if isinstance(response, HTTPResponse):
            response = response.to_polars(parse=True, **media_options)

        if isinstance(response, pl.LazyFrame):
            response = response.collect()

        if response.shape[0] == 0:
            return reorder_columns(response, schema=FINAL_SCHEMA)

        # -- resolution --------------------------------------------------
        if "resolution" in response.columns:
            response = response.unnest("resolution", separator="_")
            response = response.with_columns(
                resolution_frequency=(
                    iso_duration_to_seconds_expr("resolution_frequency")
                    if "resolution_frequency" in response.columns
                    else pl.lit(0, dtype=pl.Int64)
                ),
                resolution_timezone=(
                    pl.when(pl.col("resolution_timezone") == GAS_DAY_TZ)
                    .then(pl.lit(GAS_DAY_TZ))
                    .otherwise(
                        Timezone.polars_normalize(pl.col("resolution_timezone"), return_value="iana")
                    )
                    if "resolution_timezone" in response.columns
                    else pl.lit(None, dtype=pl.Utf8)
                ),
            )
        elif "resolution_frequency" not in response.columns:
            response = response.with_columns(
                resolution_frequency=pl.lit(0, dtype=pl.Int64),
                resolution_timezone=pl.lit(None, dtype=pl.Utf8),
            )
        elif "resolution_timezone" not in response.columns:
            response = response.with_columns(
                resolution_timezone=pl.lit(None, dtype=pl.Utf8),
            )

        # -- unit --------------------------------------------------------
        if "unit" in response.columns:
            response = response.with_columns(*parse_unit_columns_expr("unit"))

        # -- instance ----------------------------------------------------
        if "instance" in response.columns and isinstance(response.schema["instance"], pl.Struct):
            response = response.unnest("instance", separator="_")
            response = response.with_columns(
                instance_issued=timestamp_utc_expr("instance_issued", parse_date_col=True),
                instance_created=timestamp_utc_expr("instance_created", parse_date_col=True),
                instance_modified=timestamp_utc_expr("instance_modified", parse_date_col=True),
            )
        elif "instance_issued" not in response.columns:
            response = response.with_columns(
                instance_issued=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
                instance_tag=pl.lit(None, dtype=pl.Utf8),
                instance_created=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
                instance_modified=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
            )

        # -- curve -------------------------------------------------------
        if "curve" in response.columns and isinstance(response.schema["curve"], pl.Struct):
            response = response.unnest("curve", separator="_").rename(
                {"curve_curve_type": "curve_type"},
                strict=False,
            )

        # -- curve_access ------------------------------------------------
        if "curve_access" in response.columns and isinstance(
            response.schema["curve_access"], pl.Struct
        ):
            response = response.unnest("curve_access", separator="_")
        elif "curve_access_by" not in response.columns:
            response = response.with_columns(
                curve_access_by=pl.lit(None, dtype=pl.Utf8),
                curve_access_package=pl.lit(None, dtype=pl.Utf8),
            )

        # -- curve_id (stable xxh3_64 of canonical curve identity) -------
        if "curve_id" not in response.columns and "curve_name" in response.columns:
            response = response.with_columns(
                curve_id=pl.col("curve_name").cast(pl.Utf8, strict=False).fill_null("").map_elements(
                    _xxh3_64_signed,
                    return_dtype=pl.Int64,
                )
            )

        # -- curve_resolution --------------------------------------------
        if "curve_resolution" in response.columns and isinstance(
            response.schema["curve_resolution"], pl.Struct
        ):
            response = response.unnest("curve_resolution", separator="_")
            response = response.with_columns(
                curve_resolution_frequency=(
                    iso_duration_to_seconds_expr("curve_resolution_frequency")
                    if "curve_resolution_frequency" in response.columns
                    else pl.lit(0, dtype=pl.Int64)
                ),
                curve_resolution_timezone=(
                    pl.when(pl.col("curve_resolution_timezone") == GAS_DAY_TZ)
                    .then(pl.lit(GAS_DAY_TZ))
                    .otherwise(
                        Timezone.polars_normalize(
                            pl.col("curve_resolution_timezone"),
                            return_value="iana",
                        )
                    )
                    if "curve_resolution_timezone" in response.columns
                    else pl.lit(None, dtype=pl.Utf8)
                ),
            )
        elif "curve_resolution_frequency" not in response.columns:
            response = response.with_columns(
                curve_resolution_frequency=pl.lit(0, dtype=pl.Int64),
                curve_resolution_timezone=pl.lit(None, dtype=pl.Utf8),
            )
        elif "curve_resolution_timezone" not in response.columns:
            response = response.with_columns(
                curve_resolution_timezone=pl.lit(None, dtype=pl.Utf8),
            )

        # -- data / OHLC -------------------------------------------------
        is_ohlc = "curve_type" in response.columns and (
            response.get_column("curve_type").cast(pl.Utf8, strict=False).eq("OHLC").any()
        )

        if is_ohlc:
            response = normalize_ohlc_frame(response)
        elif "data" in response.columns:
            schema_ = response.schema

            if isinstance(schema_["data"], pl.List):
                response = response.explode("data")

                if isinstance(schema_["data"].inner, pl.Struct):
                    response = response.unnest("data", separator="_")
                elif isinstance(schema_["data"].inner, pl.Null):
                    response = response.with_columns(
                        data_d=pl.lit(None, dtype=pl.Datetime("us", "UTC")),
                        data_v=pl.lit(None, dtype=pl.Float64),
                    )
            elif isinstance(schema_["data"], pl.Struct):
                response = response.unnest("data", separator="_")

        # -- scenario timeseries -----------------------------------------
        if "scenario_names" in response.columns and "data_s" in response.columns:
            schema_ = response.schema

            if isinstance(schema_["scenario_names"], pl.List) and isinstance(
                schema_["data_s"], pl.List
            ):
                inner_dtype = schema_["data_s"].inner

                response = (
                    response
                    .with_columns(
                        pl.struct(["scenario_names", "data_s"]).map_elements(
                            lambda row: [
                                {"scenario_name": name, "data_v": value}
                                for name, value in zip(
                                    row["scenario_names"] or [],
                                    row["data_s"] or [],
                                )
                            ],
                            return_dtype=pl.List(
                                pl.Struct({"scenario_name": pl.Utf8, "data_v": inner_dtype})
                            ),
                        ).alias("_scenario")
                    )
                    .explode("_scenario")
                    .unnest("_scenario")
                    .drop(["scenario_names", "data_s"])
                )

        # -- assemble data structs ---------------------------------------
        response = make_data(response)

        response = response.drop(
            [
                "data_d",
                "data_open",
                "data_high",
                "data_low",
                "data_close",
                "data_settlement",
                "data_volume",
                "data_open_interest",
                "data_capacity",
                "data_product_traded_at",
                "data_product_traded_at_ts",
                "data_product_delivery",
                "data_product_period",
                "data_product_front",
                "data_begin",
                "data_end",
                "data_v",
            ],
            strict=False,
        )

        # -- fill nulls from curvemap (partitioned by curve_name) --------
        if "curve_name" in response.columns:
            distinct_names = response["curve_name"].drop_nulls().unique().to_list()
            meta_df = self._curvemap_defaults(distinct_names)

            if meta_df.shape[0] > 0:
                response = response.join(meta_df, on="curve_name", how="left")

                fill_map = {
                    "unit": "_d_unit",
                    "denominator": "_d_denominator",
                    "resolution_frequency": "_d_resolution_frequency",
                    "resolution_timezone": "_d_resolution_timezone",
                }
                fill_exprs = []
                for col, meta_col in fill_map.items():
                    if meta_col not in response.columns:
                        continue
                    if col in response.columns:
                        fill_exprs.append(
                            pl.col(col).fill_null(pl.col(meta_col)).alias(col)
                        )
                    else:
                        fill_exprs.append(pl.col(meta_col).alias(col))

                if fill_exprs:
                    response = response.with_columns(fill_exprs)

                response = response.drop(
                    [c for c in response.columns if c.startswith("_d_")],
                    strict=False,
                )

        # -- compute run_hash (stable xxh3_64 of canonical series identity)
        _identity_cols = [
            c for c in (
                "curve_name",
                "scenario_name",
                "instance_created",
                "instance_modified",
                "instance_tag",
                "unit",
                "denominator",
            )
            if c in response.columns
        ]
        if _identity_cols:
            response = response.with_columns(
                run_hash=_stable_xxh3_hash_expr(response, _identity_cols)
            )

        # -- explode data struct into individual columns -----------------
        if "data" in response.columns:
            response = response.unnest("data")

        # -- drop rows without a timestamp (non-nullable) ----------------
        if "from_timestamp" in response.columns:
            response = response.filter(pl.col("from_timestamp").is_not_null())

        # -- optional time-range filter ----------------------------------
        if "from_timestamp" in response.columns:
            predicates: list[pl.Expr] = [
                pl.col("from_timestamp").is_not_null(),
                pl.col("to_timestamp").is_not_null()
            ]

            if (begin is not None or end is not None):
                if begin is not None:
                    begin_dt = any_to_datetime(begin, tz=dt.timezone.utc)
                    predicates.append(pl.col("from_timestamp") >= begin_dt)

                if end is not None:
                    end_dt = any_to_datetime(end, tz=dt.timezone.utc)
                    predicates.append(pl.col("from_timestamp") < end_dt)

            response = response.filter(*predicates)

        return reorder_columns(response, schema=FINAL_SCHEMA)

