from __future__ import annotations

import pyarrow as pa
import polars as pl
from yggdrasil.data import field as schema_field, schema

__all__ = ["CURATED_DATA_SCHEMA", "FINAL_SCHEMA", "DATA_SCHEMA"]

# ---------------------------------------------------------------------------
# Arrow schema
# ---------------------------------------------------------------------------

_CURATED_DATA_SCHEMA_JSON_TAGS: dict[str, str] = {
    "domain": "energy",
    "entity": "curve.data",
    "layer": "silver",
    "namespace": "monteleq.api.client",
}

CURATED_DATA_SCHEMA = schema(
    fields=[],
    metadata={
        "comment": (
            "Curated EnergyQuantified dataset. "
            "One row represents one logical curve series grouped by "
            "(curve_name, scenario_name, instance_created, instance_modified, instance_tag), "
            "with interval observations stored in data and summary statistics materialized "
            "at row level."
        ),
    },
    tags=_CURATED_DATA_SCHEMA_JSON_TAGS,
)

# ---------------------------------------------------------------------------
# Curve identity
# ---------------------------------------------------------------------------

CURATED_DATA_SCHEMA["curve_id"] = schema_field(
    "curve_id",
    pa.int64(),
    nullable=False,
    metadata={"comment": "xxh3_64 hash of curve_name, matching Curve.id in the model"},
    tags={
        "entity": "curve",
        "group": "curve",
        "primary_key": "true",
        "partition_by": "true",
    },
)

CURATED_DATA_SCHEMA["curve_name"] = schema_field(
    "curve_name",
    pa.string(),
    nullable=False,
    metadata={"comment": "Unique EnergyQuantified curve name"},
    tags={"entity": "curve", "group": "curve"},
)

# ---------------------------------------------------------------------------
# Curve metadata
# ---------------------------------------------------------------------------

CURATED_DATA_SCHEMA["curve_access_by"] = schema_field(
    "curve_access_by",
    pa.string(),
    nullable=True,
    metadata={"comment": "Access scope or owner for the curve subscription"},
    tags={"entity": "curve", "group": "subscription"},
)

CURATED_DATA_SCHEMA["curve_access_package"] = schema_field(
    "curve_access_package",
    pa.string(),
    nullable=True,
    metadata={"comment": "Subscription package associated with the curve"},
    tags={"entity": "curve", "group": "subscription"},
)

CURATED_DATA_SCHEMA["curve_area"] = schema_field(
    "curve_area",
    pa.string(),
    nullable=True,
    metadata={"comment": "Primary curve area"},
    tags={"entity": "curve", "group": "curve"},
)

CURATED_DATA_SCHEMA["curve_area_sink"] = schema_field(
    "curve_area_sink",
    pa.string(),
    nullable=True,
    metadata={"comment": "Sink area associated with the curve when applicable"},
    tags={"entity": "curve", "group": "curve"},
)

CURATED_DATA_SCHEMA["curve_categories"] = schema_field(
    "curve_categories",
    pa.list_(pa.string()),
    nullable=True,
    metadata={"comment": "Ordered list of vendor curve categories"},
    tags={"entity": "curve", "group": "curve"},
)

CURATED_DATA_SCHEMA["curve_commodity"] = schema_field(
    "curve_commodity",
    pa.string(),
    nullable=True,
    metadata={"comment": "Commodity associated with the curve"},
    tags={"entity": "curve", "group": "curve"},
)

CURATED_DATA_SCHEMA["curve_type"] = schema_field(
    "curve_type",
    pa.string(),
    nullable=False,
    metadata={"comment": "EnergyQuantified curve type"},
    tags={"entity": "curve", "group": "curve"},
)

CURATED_DATA_SCHEMA["curve_data_type"] = schema_field(
    "curve_data_type",
    pa.string(),
    nullable=False,
    metadata={"comment": "Vendor data type for the curve"},
    tags={"entity": "curve", "group": "curve"},
)

CURATED_DATA_SCHEMA["curve_denominator"] = schema_field(
    "curve_denominator",
    pa.string(),
    nullable=True,
    metadata={"comment": "Denominator unit defined at curve metadata level"},
    tags={"entity": "curve", "group": "curve"},
)

CURATED_DATA_SCHEMA["curve_source"] = schema_field(
    "curve_source",
    pa.string(),
    nullable=True,
    metadata={"comment": "Vendor source system or provenance label for the curve"},
    tags={"entity": "curve", "group": "curve"},
)

CURATED_DATA_SCHEMA["curve_subscription"] = schema_field(
    "curve_subscription",
    pa.struct(
        [
            pa.field("access", pa.string(), nullable=True),
            pa.field("area", pa.string(), nullable=True),
            pa.field("label", pa.string(), nullable=True),
            pa.field("package", pa.string(), nullable=True),
            pa.field("type", pa.string(), nullable=True),
        ]
    ),
    nullable=True,
    metadata={"comment": "Structured curve subscription metadata from EnergyQuantified"},
    tags={"entity": "curve", "group": "subscription"},
)

CURATED_DATA_SCHEMA["curve_place"] = schema_field(
    "curve_place",
    pa.struct(
        [
            pa.field("type", pa.string(), nullable=True),
            pa.field("key", pa.string(), nullable=True),
            pa.field("name", pa.string(), nullable=True),
            pa.field("unit", pa.string(), nullable=True),
            pa.field("area", pa.string(), nullable=True),
            pa.field("areas", pa.list_(pa.string()), nullable=True),
            pa.field("location", pa.list_(pa.float64()), nullable=True),
            pa.field("fuels", pa.list_(pa.string()), nullable=True),
            pa.field("remit_units", pa.list_(pa.string()), nullable=True),
        ]
    ),
    nullable=True,
    metadata={"comment": "Structured place metadata attached to the curve"},
    tags={"entity": "curve", "group": "location"},
)

CURATED_DATA_SCHEMA["curve_instance_issued_timezone"] = schema_field(
    "curve_instance_issued_timezone",
    pa.string(),
    nullable=True,
    metadata={"comment": "Timezone associated with instance issued timestamps at curve metadata level"},
    tags={"entity": "curve", "group": "timing"},
)

CURATED_DATA_SCHEMA["curve_unit"] = schema_field(
    "curve_unit",
    pa.string(),
    nullable=True,
    metadata={"comment": "Unit defined at curve metadata level"},
    tags={"entity": "curve", "group": "measure"},
)

CURATED_DATA_SCHEMA["curve_resolution_frequency"] = schema_field(
    "curve_resolution_frequency",
    pa.int64(),
    nullable=True,
    metadata={
        "comment": "Curve metadata resolution frequency normalized to seconds",
        "unit": "s",
    },
    tags={"entity": "curve", "group": "timing"},
)

CURATED_DATA_SCHEMA["curve_resolution_timezone"] = schema_field(
    "curve_resolution_timezone",
    pa.string(),
    nullable=True,
    metadata={"comment": "Curve metadata resolution timezone normalized to IANA name when available"},
    tags={"entity": "curve", "group": "timing"},
)

# ---------------------------------------------------------------------------
# Series metadata
# ---------------------------------------------------------------------------

CURATED_DATA_SCHEMA["resolution_frequency"] = schema_field(
    "resolution_frequency",
    pa.int64(),
    nullable=True,
    metadata={
        "comment": "Effective series resolution frequency normalized to seconds",
        "unit": "s",
    },
    tags={"entity": "series", "group": "timing"},
)

CURATED_DATA_SCHEMA["resolution_timezone"] = schema_field(
    "resolution_timezone",
    pa.string(),
    nullable=True,
    metadata={"comment": "Effective series resolution timezone normalized to IANA name when available"},
    tags={"entity": "series", "group": "timing"},
)

CURATED_DATA_SCHEMA["unit"] = schema_field(
    "unit",
    pa.string(),
    nullable=True,
    metadata={"comment": "Numerator unit for the curated data series"},
    tags={"entity": "series", "group": "measure"},
)

CURATED_DATA_SCHEMA["denominator"] = schema_field(
    "denominator",
    pa.string(),
    nullable=True,
    metadata={"comment": "Denominator unit for the curated data series when applicable"},
    tags={"entity": "series", "group": "measure"},
)

CURATED_DATA_SCHEMA["scenario_name"] = schema_field(
    "scenario_name",
    pa.string(),
    nullable=True,
    metadata={"comment": "Scenario name for scenario timeseries or ensemble-style data"},
    tags={"entity": "series", "group": "scenario"},
)

# ---------------------------------------------------------------------------
# Instance metadata
# ---------------------------------------------------------------------------

CURATED_DATA_SCHEMA["instance_issued"] = schema_field(
    "instance_issued",
    pa.timestamp("us", "UTC"),
    nullable=True,
    metadata={
        "comment": "UTC timestamp when the instance was issued",
        "unit": "us",
        "tz": "UTC",
    },
    tags={"entity": "instance", "group": "instance"},
)

CURATED_DATA_SCHEMA["instance_tag"] = schema_field(
    "instance_tag",
    pa.string(),
    nullable=True,
    metadata={"comment": "Vendor instance tag"},
    tags={"entity": "instance", "group": "instance"},
)

CURATED_DATA_SCHEMA["instance_created"] = schema_field(
    "instance_created",
    pa.timestamp("us", "UTC"),
    nullable=True,
    metadata={
        "comment": "UTC timestamp when the instance was created",
        "unit": "us",
        "tz": "UTC",
    },
    tags={"entity": "instance", "group": "instance"},
)

CURATED_DATA_SCHEMA["instance_modified"] = schema_field(
    "instance_modified",
    pa.timestamp("us", "UTC"),
    nullable=True,
    metadata={
        "comment": "UTC timestamp when the instance was last modified",
        "unit": "us",
        "tz": "UTC",
    },
    tags={"entity": "instance", "group": "instance"},
)

# ---------------------------------------------------------------------------
# Series identity + payload
# ---------------------------------------------------------------------------

CURATED_DATA_SCHEMA["run_hash"] = schema_field(
    "run_hash",
    pa.int64(),
    nullable=False,
    metadata={
        "comment": (
            "xxh3_64 hash of the series identity keys "
            "(curve_name, scenario_name, instance_created, instance_modified, instance_tag). "
            "Combined with from_timestamp forms the full primary key."
        ),
    },
    tags={
        "entity": "series",
        "group": "key",
        "primary_key": "true",
    },
)

CURATED_DATA_SCHEMA["from_timestamp"] = schema_field(
    "from_timestamp",
    pa.timestamp("us", "UTC"),
    nullable=False,
    metadata={
        "comment": "UTC start timestamp of the interval observation",
        "unit": "us",
        "tz": "UTC",
    },
    tags={
        "entity": "series",
        "group": "payload",
        "primary_key": "true",
    },
)

CURATED_DATA_SCHEMA["from_utc_offset"] = schema_field(
    "from_utc_offset",
    pa.int64(),
    nullable=False,
    metadata={
        "comment": "UTC offset in seconds at the interval start",
        "unit": "s",
    },
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["to_timestamp"] = schema_field(
    "to_timestamp",
    pa.timestamp("us", "UTC"),
    nullable=False,
    metadata={
        "comment": "UTC end timestamp of the interval observation",
        "unit": "us",
        "tz": "UTC",
    },
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["to_utc_offset"] = schema_field(
    "to_utc_offset",
    pa.int64(),
    nullable=False,
    metadata={
        "comment": "UTC offset in seconds at the interval end",
        "unit": "s",
    },
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["value"] = schema_field(
    "value",
    pa.float64(),
    nullable=True,
    metadata={"comment": "Observed numeric value for the interval"},
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["capacity"] = schema_field(
    "capacity",
    pa.float64(),
    nullable=True,
    metadata={"comment": "Capacity value when available"},
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["open"] = schema_field(
    "open",
    pa.float64(),
    nullable=True,
    metadata={"comment": "OHLC open price"},
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["high"] = schema_field(
    "high",
    pa.float64(),
    nullable=True,
    metadata={"comment": "OHLC high price"},
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["low"] = schema_field(
    "low",
    pa.float64(),
    nullable=True,
    metadata={"comment": "OHLC low price"},
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["close"] = schema_field(
    "close",
    pa.float64(),
    nullable=True,
    metadata={"comment": "OHLC close price"},
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["settlement"] = schema_field(
    "settlement",
    pa.float64(),
    nullable=True,
    metadata={"comment": "OHLC settlement price"},
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["volume"] = schema_field(
    "volume",
    pa.float64(),
    nullable=True,
    metadata={"comment": "OHLC traded volume"},
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["open_interest"] = schema_field(
    "open_interest",
    pa.float64(),
    nullable=True,
    metadata={"comment": "OHLC open interest"},
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["traded_at"] = schema_field(
    "traded_at",
    pa.timestamp("us", "UTC"),
    nullable=True,
    metadata={
        "comment": "UTC timestamp when the OHLC observation was traded",
        "unit": "us",
        "tz": "UTC",
    },
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["front"] = schema_field(
    "front",
    pa.int64(),
    nullable=True,
    metadata={"comment": "Front number for OHLC forward curves"},
    tags={"entity": "series", "group": "payload"},
)

CURATED_DATA_SCHEMA["period"] = schema_field(
    "period",
    pa.string(),
    nullable=True,
    metadata={"comment": "Delivery period label for OHLC forward curves"},
    tags={"entity": "series", "group": "payload"},
)

# ---------------------------------------------------------------------------
# Polars projections
# ---------------------------------------------------------------------------

FINAL_SCHEMA: pl.Schema = CURATED_DATA_SCHEMA.to_polars_schema()

DATA_SCHEMA: pl.Schema = pl.Schema(
    {
        name: FINAL_SCHEMA[name]
        for name in (
            "curve_id",
            "curve_name",
            "curve_access_by",
            "curve_access_package",
            "curve_area",
            "curve_area_sink",
            "curve_categories",
            "curve_commodity",
            "curve_type",
            "curve_data_type",
            "curve_denominator",
            "curve_source",
            "curve_subscription",
            "curve_place",
            "curve_instance_issued_timezone",
            "curve_unit",
            "curve_resolution_frequency",
            "curve_resolution_timezone",
            "resolution_frequency",
            "resolution_timezone",
            "unit",
            "denominator",
            "scenario_name",
            "instance_issued",
            "instance_tag",
            "instance_created",
            "instance_modified",
            "run_hash",
            "from_timestamp",
            "from_utc_offset",
            "to_timestamp",
            "to_utc_offset",
            "value",
            "capacity",
            "open",
            "high",
            "low",
            "close",
            "settlement",
            "volume",
            "open_interest",
            "traded_at",
            "front",
            "period",
        )
    }
)