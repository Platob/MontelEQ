from __future__ import annotations

import polars as pl

DATA_GROUP_COLUMN_NAMES = (
    "curve_name",
    "run_hash",
    "from_timestamp",
)

RESOLUTION_TIMEZONE_HELPER = "__effective_resolution_timezone"

# European Gas Day: a pseudo-timezone where midnight (00:00) corresponds to
# 06:00 CET (Europe/Paris).  Not a valid IANA name — requires special handling.
GAS_DAY_TZ = "Europe/Gas_Day"
GAS_DAY_BASE_TZ = "Europe/Paris"
GAS_DAY_HOUR_OFFSET = 6


_COMMON_DURATIONS: dict[str | None, int] = {
    None: 0, "": 0, "NONE": 0,
    "PT1S": 1, "PT5S": 5, "PT10S": 10, "PT15S": 15, "PT30S": 30,
    "PT1M": 60, "PT5M": 300, "PT10M": 600, "PT15M": 900, "PT30M": 1800,
    "PT1H": 3600, "PT2H": 7200, "PT3H": 10800, "PT4H": 14400,
    "PT6H": 21600, "PT12H": 43200,
    "P1D": 86400, "P1W": 7 * 86400,
    "P1M": 30 * 86400, "P3M": 90 * 86400, "P6M": 180 * 86400,
    "P1Y": 365 * 86400,
    "P1DT2H30M": 86400 + 7200 + 1800,
}


def _iso_batch_lookup(s: pl.Series) -> pl.Series:
    lut = _COMMON_DURATIONS
    values = s.to_list()
    result = []
    need_regex = False
    for v in values:
        if v is None:
            result.append(0)
            continue
        key = v.strip().upper()
        cached = lut.get(key)
        if cached is not None:
            result.append(cached)
        elif key.startswith("P"):
            need_regex = True
            result.append(None)
        else:
            result.append(0)

    if need_regex:
        import re
        _pat = re.compile(
            r"P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?"
            r"(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?"
        )
        for i, v in enumerate(result):
            if v is not None:
                continue
            m = _pat.match(values[i].strip().upper())
            if m:
                g = [int(x) if x else 0 for x in m.groups()]
                secs = (
                    g[0] * 365 * 24 * 3600 + g[1] * 30 * 24 * 3600
                    + g[2] * 7 * 24 * 3600 + g[3] * 24 * 3600
                    + g[4] * 3600 + g[5] * 60 + g[6]
                )
                lut[values[i].strip().upper()] = secs
                result[i] = secs
            else:
                result[i] = 0
    return pl.Series(s.name, result, dtype=pl.Int64)


def iso_duration_to_seconds_expr(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .map_batches(_iso_batch_lookup, return_dtype=pl.Int64)
    )


def normalize_datetime_string_expr(col: str) -> pl.Expr:
    s = pl.col(col).cast(pl.Utf8, strict=False).str.strip_chars()
    s = s.str.replace(r"Z$", "+00:00")
    s = s.str.replace(
        r"^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2})([+-]\d{2}:\d{2})$",
        r"${1}:00${2}",
    )
    s = s.str.replace(
        r"^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2})$",
        r"${1}:00",
    )
    return s


def non_empty_utf8_expr(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .replace("", None)
    )


def timestamp_utc_expr(
    col: str,
    parse_date_col: bool,
) -> pl.Expr:
    if not parse_date_col:
        return pl.col(col).cast(pl.Datetime("us", "UTC"), strict=False)

    s = normalize_datetime_string_expr(col)

    return (
        s.str.to_datetime(
            time_unit="us",
            strict=False,
            time_zone="UTC",
        )
        .cast(pl.Datetime("us", "UTC"), strict=False)
    )


def utc_offset_seconds_expr(
    col: str,
    parse_date_col: bool,
) -> pl.Expr:
    if not parse_date_col:
        return pl.lit(0, dtype=pl.Int64)

    s = normalize_datetime_string_expr(col)

    sign = (
        s.str.extract(r"([+-])\d{2}:\d{2}$", 1)
        .replace_strict({"+": 1, "-": -1}, default=None)
        .cast(pl.Int64, strict=False)
    )

    hours = s.str.extract(r"[+-](\d{2}):\d{2}$", 1).cast(pl.Int64, strict=False)
    minutes = s.str.extract(r"[+-]\d{2}:(\d{2})$", 1).cast(pl.Int64, strict=False)

    return (
        pl.when(s.str.ends_with("+00:00"))
        .then(pl.lit(0, dtype=pl.Int64))
        .when(s.str.contains(r"[+-]\d{2}:\d{2}$"))
        .then(sign * (hours.fill_null(0) * 3600 + minutes.fill_null(0) * 60))
        .otherwise(pl.lit(0, dtype=pl.Int64))
        .cast(pl.Int64)
    )


def resolve_effective_resolution_timezone(df: pl.DataFrame) -> pl.DataFrame:
    candidates: list[pl.Expr] = []

    if "resolution_timezone" in df.columns:
        candidates.append(non_empty_utf8_expr("resolution_timezone"))

    if "curve_resolution_timezone" in df.columns:
        candidates.append(non_empty_utf8_expr("curve_resolution_timezone"))

    if "curve_instance_issued_timezone" in df.columns:
        candidates.append(non_empty_utf8_expr("curve_instance_issued_timezone"))

    if not candidates:
        return df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias(RESOLUTION_TIMEZONE_HELPER)
        )

    return df.with_columns(
        pl.coalesce(candidates).alias(RESOLUTION_TIMEZONE_HELPER)
    )


def localize_by_distinct_timezones(
    df: pl.DataFrame,
    *,
    source_col: str,
    timezone_col: str,
    parsed_col: str,
    offset_col: str,
) -> pl.DataFrame:
    if source_col not in df.columns:
        return df.with_columns(
            pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias(parsed_col),
            pl.lit(None, dtype=pl.Int64).alias(offset_col),
        )

    normalized_col = f"__norm__{source_col}"
    naive_col = f"__naive__{source_col}"

    work = df.with_columns(
        normalize_datetime_string_expr(source_col).alias(normalized_col)
    )

    explicit_mask = pl.col(normalized_col).str.contains(r"([+-]\d{2}:\d{2})$")
    null_mask = pl.col(normalized_col).is_null()
    naive_mask = (~explicit_mask) & (~null_mask)

    out: list[pl.DataFrame] = []

    explicit_df = work.filter(explicit_mask)
    if explicit_df.height:
        out.append(
            explicit_df.with_columns(
                timestamp_utc_expr(source_col, parse_date_col=True).alias(parsed_col),
                utc_offset_seconds_expr(source_col, parse_date_col=True).alias(offset_col),
            )
        )

    null_df = work.filter(null_mask)
    if null_df.height:
        out.append(
            null_df.with_columns(
                pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias(parsed_col),
                pl.lit(None, dtype=pl.Int64).alias(offset_col),
            )
        )

    naive_df = work.filter(naive_mask)
    if naive_df.height:
        naive_df = naive_df.with_columns(
            pl.col(normalized_col).str.replace(r"([+-]\d{2}:\d{2})$", "").alias(naive_col)
        )

        tz_values = naive_df.get_column(timezone_col).unique().to_list()

        for tz in tz_values:
            if tz is None:
                bucket = naive_df.filter(pl.col(timezone_col).is_null())
            else:
                bucket = naive_df.filter(pl.col(timezone_col) == tz)

            if not bucket.height:
                continue

            if tz == GAS_DAY_TZ:
                # European Gas Day: midnight in gas-day notation = 06:00 CET.
                # Add 6 h to the naive value, then localise as Europe/Paris.
                parsed_naive = (
                    pl.col(naive_col)
                    .str.to_datetime(time_unit="us", strict=False, ambiguous="earliest")
                    + pl.duration(hours=GAS_DAY_HOUR_OFFSET)
                )
                localized = parsed_naive.dt.replace_time_zone(
                    GAS_DAY_BASE_TZ,
                    ambiguous="earliest",
                    non_existent="null",
                )
            else:
                tz_literal = "UTC" if tz in (None, "") else str(tz)

                localized = (
                    pl.col(naive_col)
                    .str.to_datetime(
                        time_unit="us",
                        strict=False,
                        ambiguous="earliest",
                    )
                    .dt.replace_time_zone(
                        tz_literal,
                        ambiguous="earliest",
                        non_existent="null",
                    )
                )

            out.append(
                bucket.with_columns(
                    localized.dt.convert_time_zone("UTC")
                    .cast(pl.Datetime("us", "UTC"), strict=False)
                    .alias(parsed_col),
                    localized.dt.base_utc_offset()
                    .dt.total_seconds()
                    .cast(pl.Int64)
                    .alias(offset_col),
                )
            )

    if not out:
        result = work.with_columns(
            pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias(parsed_col),
            pl.lit(None, dtype=pl.Int64).alias(offset_col),
        )
    else:
        result = pl.concat(out, how="diagonal_relaxed")

    return result.drop([normalized_col, naive_col], strict=False)


def data_struct_expr(
    *,
    from_ts: pl.Expr | None,
    from_offset: pl.Expr | None,
    to_ts: pl.Expr | None,
    to_offset: pl.Expr | None,
    frequency: int | pl.Expr,
    value_col: str,
    capacity_col: str | None = None,
    open_col: str | None = None,
    high_col: str | None = None,
    low_col: str | None = None,
    close_col: str | None = None,
    settlement_col: str | None = None,
    volume_col: str | None = None,
    open_interest_col: str | None = None,
    traded_at_col: str | None = None,
    front_col: str | None = None,
    period_col: str | None = None,
) -> pl.Expr:
    frequency_expr = (
        frequency
        if isinstance(frequency, pl.Expr)
        else pl.lit(frequency, dtype=pl.Int64)
    )

    freq_is_positive = frequency_expr > 0
    freq_duration = pl.duration(seconds=frequency_expr)

    if from_ts is not None and to_ts is not None:
        final_from_ts = from_ts
        final_from_offset = from_offset
        final_to_ts = pl.when(freq_is_positive).then(from_ts + freq_duration).otherwise(to_ts)
        final_to_offset = to_offset
    elif from_ts is not None:
        final_from_ts = from_ts
        final_from_offset = from_offset
        final_to_ts = pl.when(freq_is_positive).then(from_ts + freq_duration).otherwise(from_ts)
        final_to_offset = from_offset
    elif to_ts is not None:
        final_from_ts = pl.when(freq_is_positive).then(to_ts - freq_duration).otherwise(to_ts)
        final_from_offset = to_offset
        final_to_ts = to_ts
        final_to_offset = to_offset
    else:
        final_from_ts = pl.lit(None, dtype=pl.Datetime("us", "UTC"))
        final_from_offset = pl.lit(None, dtype=pl.Int64)
        final_to_ts = pl.lit(None, dtype=pl.Datetime("us", "UTC"))
        final_to_offset = pl.lit(None, dtype=pl.Int64)

    def optional_float(col: str | None) -> pl.Expr:
        if col is None:
            return pl.lit(None, dtype=pl.Float64)
        return pl.col(col).cast(pl.Float64, strict=False)

    def optional_int(col: str | None) -> pl.Expr:
        if col is None:
            return pl.lit(None, dtype=pl.Int64)
        return pl.col(col).cast(pl.Int64, strict=False)

    def optional_utf8(col: str | None) -> pl.Expr:
        if col is None:
            return pl.lit(None, dtype=pl.Utf8)
        return pl.col(col).cast(pl.Utf8, strict=False)

    def optional_ts(col: str | None) -> pl.Expr:
        if col is None:
            return pl.lit(None, dtype=pl.Datetime("us", "UTC"))
        return pl.col(col).cast(pl.Datetime("us", "UTC"), strict=False)

    return pl.struct(
        [
            final_from_ts.alias("from_timestamp"),
            final_from_offset.alias("from_utc_offset"),
            final_to_ts.alias("to_timestamp"),
            final_to_offset.alias("to_utc_offset"),
            pl.col(value_col).cast(pl.Float64, strict=False).alias("value"),
            optional_float(capacity_col).alias("capacity"),
            optional_float(open_col).alias("open"),
            optional_float(high_col).alias("high"),
            optional_float(low_col).alias("low"),
            optional_float(close_col).alias("close"),
            optional_float(settlement_col).alias("settlement"),
            optional_float(volume_col).alias("volume"),
            optional_float(open_interest_col).alias("open_interest"),
            optional_ts(traded_at_col).alias("traded_at"),
            optional_int(front_col).alias("front"),
            optional_utf8(period_col).alias("period"),
        ]
    ).alias("data")


def parse_unit_columns_expr(col: str = "unit") -> list[pl.Expr]:
    normalized = (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
    )

    return [
        normalized
        .str.extract(r"^\s*([^/]+)", 1)
        .str.strip_chars()
        .alias("unit"),
        normalized
        .str.extract(r"/\s*([^/]+)\s*$", 1)
        .str.strip_chars()
        .alias("denominator"),
    ]


def make_data(df: pl.DataFrame) -> pl.DataFrame:
    df = df.filter(pl.col("curve_name").is_not_null())

    has_value = "data_v" in df.columns
    has_begin = "data_begin" in df.columns
    has_end = "data_end" in df.columns
    has_point = "data_d" in df.columns

    if not has_value or not (has_point or has_begin or has_end):
        return df

    begin_col = "data_begin" if has_begin else ("data_d" if has_point else None)
    end_col = "data_end" if has_end else None

    begin_parse = begin_col is not None and isinstance(df.schema[begin_col], pl.String)
    end_parse = end_col is not None and isinstance(df.schema[end_col], pl.String)

    group_cols = [col for col in DATA_GROUP_COLUMN_NAMES if col in df.columns]
    sort_cols = [col for col in (*group_cols, begin_col, end_col) if col is not None and col in df.columns]

    if sort_cols:
        df = df.sort(sort_cols)

    df = resolve_effective_resolution_timezone(df)

    timezone_helper = RESOLUTION_TIMEZONE_HELPER
    begin_ts_col = "__begin_ts_utc"
    begin_offset_col = "__begin_utc_offset"
    end_ts_col = "__end_ts_utc"
    end_offset_col = "__end_utc_offset"

    if begin_col is not None and begin_parse:
        df = localize_by_distinct_timezones(
            df,
            source_col=begin_col,
            timezone_col=timezone_helper,
            parsed_col=begin_ts_col,
            offset_col=begin_offset_col,
        )
    elif begin_col is not None:
        df = df.with_columns(
            pl.col(begin_col).cast(pl.Datetime("us", "UTC"), strict=False).alias(begin_ts_col),
            pl.lit(0, dtype=pl.Int64).alias(begin_offset_col),
        )

    if end_col is not None and end_parse:
        df = localize_by_distinct_timezones(
            df,
            source_col=end_col,
            timezone_col=timezone_helper,
            parsed_col=end_ts_col,
            offset_col=end_offset_col,
        )
    elif end_col is not None:
        df = df.with_columns(
            pl.col(end_col).cast(pl.Datetime("us", "UTC"), strict=False).alias(end_ts_col),
            pl.lit(0, dtype=pl.Int64).alias(end_offset_col),
        )

    begin_ts = pl.col(begin_ts_col) if begin_col is not None else None
    end_ts = pl.col(end_ts_col) if end_col is not None else None

    resolution_frequency = (
        pl.col("resolution_frequency").fill_null(0).cast(pl.Int64)
        if "resolution_frequency" in df.columns
        else pl.lit(0, dtype=pl.Int64)
    )

    if begin_ts is not None and end_ts is not None:
        found_frequency = (end_ts - begin_ts).dt.total_seconds().cast(pl.Int64)
    elif begin_ts is not None:
        next_delta = (
            begin_ts.shift(-1).over(group_cols) - begin_ts
            if group_cols
            else begin_ts.shift(-1) - begin_ts
        )
        prev_delta = (
            begin_ts - begin_ts.shift(1).over(group_cols)
            if group_cols
            else begin_ts - begin_ts.shift(1)
        )

        found_frequency = pl.coalesce(
            next_delta.dt.total_seconds(),
            prev_delta.dt.total_seconds(),
        ).cast(pl.Int64)
    elif end_ts is not None:
        next_delta = (
            end_ts.shift(-1).over(group_cols) - end_ts
            if group_cols
            else end_ts.shift(-1) - end_ts
        )
        prev_delta = (
            end_ts - end_ts.shift(1).over(group_cols)
            if group_cols
            else end_ts - end_ts.shift(1)
        )

        found_frequency = pl.coalesce(
            next_delta.dt.total_seconds(),
            prev_delta.dt.total_seconds(),
        ).cast(pl.Int64)
    else:
        found_frequency = pl.lit(0, dtype=pl.Int64)

    valid_rows = pl.col("data_v").is_not_null()
    if begin_col is not None:
        valid_rows = valid_rows & pl.col(begin_col).is_not_null()
    elif end_col is not None:
        valid_rows = valid_rows & pl.col(end_col).is_not_null()

    effective_frequency = (
        pl.when(resolution_frequency == 0)
        .then(found_frequency.fill_null(0))
        .otherwise(resolution_frequency)
        .cast(pl.Int64)
    )

    result = (
        df
        .filter(valid_rows)
        .with_columns(
            resolution_frequency=effective_frequency,
            data=data_struct_expr(
                from_ts=pl.col(begin_ts_col) if begin_col is not None else None,
                from_offset=pl.col(begin_offset_col) if begin_col is not None else None,
                to_ts=pl.col(end_ts_col) if end_col is not None else None,
                to_offset=pl.col(end_offset_col) if end_col is not None else None,
                frequency=pl.col("resolution_frequency"),
                value_col="data_v",
                capacity_col="data_capacity" if "data_capacity" in df.columns else None,
                open_col="data_open" if "data_open" in df.columns else None,
                high_col="data_high" if "data_high" in df.columns else None,
                low_col="data_low" if "data_low" in df.columns else None,
                close_col="data_close" if "data_close" in df.columns else None,
                settlement_col="data_settlement" if "data_settlement" in df.columns else None,
                volume_col="data_volume" if "data_volume" in df.columns else None,
                open_interest_col="data_open_interest" if "data_open_interest" in df.columns else None,
                traded_at_col="data_product_traded_at_ts" if "data_product_traded_at_ts" in df.columns else None,
                front_col="data_product_front" if "data_product_front" in df.columns else None,
                period_col="data_product_period" if "data_product_period" in df.columns else None,
            ),
        )
    )

    drop_cols = [timezone_helper]
    if begin_col is not None:
        drop_cols.extend([begin_ts_col, begin_offset_col])
    if end_col is not None:
        drop_cols.extend([end_ts_col, end_offset_col])

    return result.drop(drop_cols, strict=False)


def _null_expr(dtype: pl.DataType) -> pl.Expr:
    return pl.lit(None, dtype=dtype)


def _struct_fields(dtype: pl.Struct) -> dict[str, pl.DataType]:
    fields = dtype.fields
    if isinstance(fields, dict):
        return fields

    result: dict[str, pl.DataType] = {}
    for field in fields:
        if hasattr(field, "name") and hasattr(field, "dtype"):
            result[field.name] = field.dtype
        else:
            result[field[0]] = field[1]
    return result


_reorder_expr_cache: dict[tuple, list[pl.Expr]] = {}


def reorder_columns(
    df: pl.DataFrame,
    *,
    schema: pl.Schema,
) -> pl.DataFrame:
    actual_columns = frozenset(df.columns)
    actual_struct_sig = tuple(
        (col, tuple(sorted(_struct_fields(df.schema[col]).keys())))
        for col in df.columns
        if col in df.schema and isinstance(df.schema[col], (pl.Struct, pl.List))
    )
    cache_key = (id(schema), actual_columns, actual_struct_sig)

    cached = _reorder_expr_cache.get(cache_key)
    if cached is not None:
        return df.select(cached).filter(pl.col("curve_name").is_not_null())

    new_columns = sorted(actual_columns - set(schema.names()))
    if new_columns:
        print(f"New columns detected: {new_columns}")

    exprs: list[pl.Expr] = []
    for col in schema.names():
        dtype = schema[col]
        if isinstance(dtype, pl.Struct):
            exprs.append(_build_struct_expr(col, dtype, df))
        elif isinstance(dtype, pl.List) and isinstance(dtype.inner, pl.Struct):
            exprs.append(_build_list_struct_expr(col, dtype, df))
        elif col in actual_columns:
            exprs.append(pl.col(col).cast(dtype, strict=False).alias(col))
        else:
            exprs.append(_null_expr(dtype).alias(col))

    _reorder_expr_cache[cache_key] = exprs
    return df.select(exprs).filter(pl.col("curve_name").is_not_null())


def _build_struct_expr(col: str, target_dtype: pl.Struct, df: pl.DataFrame) -> pl.Expr:
    target_fields = _struct_fields(target_dtype)

    if col not in df.columns or not isinstance(df.schema[col], pl.Struct):
        return _null_expr(target_dtype).alias(col)

    current_fields = _struct_fields(df.schema[col])

    return pl.struct(
        [
            (
                pl.col(col).struct.field(name).cast(dtype, strict=False)
                if name in current_fields
                else _null_expr(dtype)
            ).alias(name)
            for name, dtype in target_fields.items()
        ]
    ).alias(col)


def _build_list_struct_expr(col: str, target_dtype: pl.List, df: pl.DataFrame) -> pl.Expr:
    target_inner = target_dtype.inner
    if not isinstance(target_inner, pl.Struct):
        return pl.col(col).cast(target_dtype, strict=False).alias(col)

    if col not in df.columns or not isinstance(df.schema[col], pl.List):
        return _null_expr(target_dtype).alias(col)

    current_inner = df.schema[col].inner
    if not isinstance(current_inner, pl.Struct):
        return _null_expr(target_dtype).alias(col)

    target_fields = _struct_fields(target_inner)
    current_fields = _struct_fields(current_inner)

    return (
        pl.when(pl.col(col).is_null())
        .then(_null_expr(target_dtype))
        .otherwise(
            pl.col(col).list.eval(
                pl.struct(
                    [
                        (
                            pl.element().struct.field(name).cast(dtype, strict=False)
                            if name in current_fields
                            else _null_expr(dtype)
                        ).alias(name)
                        for name, dtype in target_fields.items()
                    ]
                )
            ).cast(target_dtype, strict=False)
        )
        .alias(col)
    )


def _ohlc_period_end_expr(
    delivery_col: str = "data_product_delivery",
    period_col: str = "data_product_period",
) -> pl.Expr:
    delivery = pl.col(delivery_col).cast(pl.Date, strict=False)
    period = (
        pl.col(period_col)
        .cast(pl.Utf8, strict=False)
        .str.to_lowercase()
        .str.strip_chars()
    )

    return (
        pl.when(delivery.is_null())
        .then(pl.lit(None, dtype=pl.Date))
        .when(period == "day")
        .then(delivery + pl.duration(days=1))
        .when(period == "week")
        .then(delivery + pl.duration(days=7))
        .when(period == "weekend")
        .then(delivery + pl.duration(days=2))
        .when(period == "month")
        .then(delivery.dt.offset_by("1mo"))
        .when(period == "quarter")
        .then(delivery.dt.offset_by("3mo"))
        .when(period == "year")
        .then(delivery.dt.offset_by("1y"))
        .when(period == "season")
        .then(delivery.dt.offset_by("6mo"))
        .otherwise(delivery)
    )


def normalize_ohlc_frame(df: pl.DataFrame) -> pl.DataFrame:
    if "curve_type" not in df.columns or "data" not in df.columns:
        return df

    curve_type = df.get_column("curve_type").cast(pl.Utf8, strict=False)
    if not curve_type.eq("OHLC").any():
        return df

    work = df

    data_dtype = work.schema.get("data")
    if isinstance(data_dtype, pl.List):
        work = work.explode("data")
        if isinstance(data_dtype.inner, pl.Struct):
            work = work.unnest("data", separator="_")
    elif isinstance(data_dtype, pl.Struct):
        work = work.unnest("data", separator="_")
    else:
        return work

    if "data_product" in work.columns and isinstance(work.schema["data_product"], pl.Struct):
        work = work.unnest("data_product", separator="_")

    value_candidates: list[pl.Expr] = []
    for col in ("data_settlement", "data_close", "data_open", "data_high", "data_low"):
        if col in work.columns:
            value_candidates.append(pl.col(col).cast(pl.Float64, strict=False))

    value_expr = (
        pl.coalesce(value_candidates).alias("data_v")
        if value_candidates
        else pl.lit(None, dtype=pl.Float64).alias("data_v")
    )

    traded_at_ts = (
        pl.col("data_product_traded_at")
        .cast(pl.Utf8, strict=False)
        .str.to_date(strict=False)
        .cast(pl.Datetime("us"))
        .dt.replace_time_zone("UTC")
        .cast(pl.Datetime("us", "UTC"))
        .alias("data_product_traded_at_ts")
        if "data_product_traded_at" in work.columns
        else pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("data_product_traded_at_ts")
    )

    instance_issued_expr = (
        pl.col("data_product_traded_at")
        .cast(pl.Utf8, strict=False)
        .str.to_date(strict=False)
        .cast(pl.Datetime("us"))
        .dt.replace_time_zone("UTC")
        .cast(pl.Datetime("us", "UTC"))
        .alias("instance_issued")
        if "data_product_traded_at" in work.columns
        else pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("instance_issued")
    )

    scenario_expr = (
        pl.when(pl.col("data_product_front").is_not_null())
        .then(pl.format("front_{}", pl.col("data_product_front").cast(pl.Int64, strict=False)))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("scenario_name")
        if "data_product_front" in work.columns
        else pl.lit(None, dtype=pl.Utf8).alias("scenario_name")
    )

    work = work.with_columns(
        traded_at_ts,
        instance_issued_expr,
        scenario_expr,
        pl.col("data_product_delivery").cast(pl.Utf8, strict=False).alias("data_begin")
        if "data_product_delivery" in work.columns
        else pl.lit(None, dtype=pl.Utf8).alias("data_begin"),
        _ohlc_period_end_expr().cast(pl.Utf8).alias("data_end")
        if "data_product_delivery" in work.columns and "data_product_period" in work.columns
        else pl.lit(None, dtype=pl.Utf8).alias("data_end"),
        value_expr,
    )

    if "resolution_frequency" not in work.columns:
        work = work.with_columns(
            resolution_frequency=pl.lit(0, dtype=pl.Int64),
        )

    if "resolution_timezone" not in work.columns:
        work = work.with_columns(
            resolution_timezone=pl.lit("UTC", dtype=pl.Utf8),
        )

    return work
