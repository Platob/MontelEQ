"""
monteleq.model
==============

Frozen dataclass models for EnergyQuantified curve metadata.
"""
from __future__ import annotations

import ctypes
import datetime as dt
import re
from dataclasses import dataclass, field
from typing import Any, Mapping

import xxhash

__all__ = [
    "Resolution",
    "Access",
    "Subscription",
    "Place",
    "Curve",
    "Instance",
    "DEFAULT_ISSUE_INTERVAL",
]

from energyquantified.metadata import CurveType, DataType

DEFAULT_ISSUE_INTERVAL = dt.timedelta(hours=4)
_UNSAFE_CHARS = re.compile(r"[^a-z0-9]+")
_TABLE_NAME_CACHE: dict[tuple, str] = {}


def _safe_name(value: str) -> str:
    return _UNSAFE_CHARS.sub("_", value.lower()).strip("_")


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _parse_str_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple)):
        return tuple(str(v) for v in value if v is not None)
    raise TypeError(f"Expected list, tuple, str, or None, got {type(value).__name__}")


def _parse_float_tuple(value: Any) -> tuple[float, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(float(v) for v in value if v is not None)
    raise TypeError(f"Expected list, tuple, or None, got {type(value).__name__}")


def _xxh3_id(name: str) -> int:
    return ctypes.c_int64(xxhash.xxh3_64_intdigest(name.encode("utf-8"))).value


def _coerce_data_type(value: Any) -> DataType:
    if value is None:
        return DataType.NORMAL
    if isinstance(value, DataType):
        return value
    s = str(value).strip().upper()
    if not s or s == "NONE":
        return DataType.NORMAL
    try:
        return DataType[s]
    except KeyError:
        raise ValueError(f"Unknown DataType: {value!r}")


def _coerce_curve_type(value: Any) -> CurveType:
    if value is None:
        return CurveType.TIMESERIES
    if isinstance(value, CurveType):
        return value
    s = str(value).strip().upper()
    if not s or s == "NONE":
        return CurveType.TIMESERIES
    try:
        return CurveType[s]
    except KeyError:
        raise ValueError(f"Unknown CurveType: {value!r}")


@dataclass(frozen=True, slots=True)
class Resolution:
    frequency: str | None = None
    timezone: str | None = None

    def __post_init__(self) -> None:
        if self.frequency is not None:
            cleaned = self.frequency.strip()
            if not cleaned or cleaned.lower() == "none":
                object.__setattr__(self, "frequency", None)
            else:
                object.__setattr__(self, "frequency", cleaned)

    @classmethod
    def parse(cls, obj: Any) -> Resolution:
        if obj is None:
            return cls()
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, Mapping):
            return cls.parse_mapping(obj)
        raise TypeError(f"Expected {cls.__name__}, mapping, or None, got {type(obj).__name__}")

    @classmethod
    def parse_mapping(cls, obj: Mapping[str, Any]) -> Resolution:
        return cls(
            frequency=_str_or_none(obj.get("frequency")),
            timezone=_str_or_none(obj.get("timezone")),
        )


@dataclass(frozen=True, slots=True)
class Access:
    by: str | None = None
    package: str | None = None

    @classmethod
    def parse(cls, obj: Any) -> Access:
        if obj is None:
            return cls()
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, Mapping):
            return cls.parse_mapping(obj)
        raise TypeError(f"Expected {cls.__name__}, mapping, or None, got {type(obj).__name__}")

    @classmethod
    def parse_mapping(cls, obj: Mapping[str, Any]) -> Access:
        return cls(
            by=_str_or_none(obj.get("by")),
            package=_str_or_none(obj.get("package")),
        )


@dataclass(frozen=True, slots=True)
class Subscription:
    access: str | None = None
    area: str | None = None
    label: str | None = None
    package: str | None = None
    type: str | None = None

    @classmethod
    def parse(cls, obj: Any) -> Subscription:
        if obj is None:
            return cls()
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, Mapping):
            return cls.parse_mapping(obj)
        raise TypeError(f"Expected {cls.__name__}, mapping, or None, got {type(obj).__name__}")

    @classmethod
    def parse_mapping(cls, obj: Mapping[str, Any]) -> Subscription:
        return cls(
            access=_str_or_none(obj.get("access")),
            area=_str_or_none(obj.get("area")),
            label=_str_or_none(obj.get("label")),
            package=_str_or_none(obj.get("package")),
            type=_str_or_none(obj.get("type")),
        )


@dataclass(frozen=True, slots=True)
class Place:
    type: str | None = None
    key: str | None = None
    name: str | None = None
    unit: str | None = None
    area: str | None = None
    areas: tuple[str, ...] = ()
    location: tuple[float, ...] = ()
    fuels: tuple[str, ...] = ()
    remit_units: tuple[str, ...] = ()

    @classmethod
    def parse(cls, obj: Any) -> Place:
        if obj is None:
            return cls()
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, Mapping):
            return cls.parse_mapping(obj)
        raise TypeError(f"Expected {cls.__name__}, mapping, or None, got {type(obj).__name__}")

    @classmethod
    def parse_mapping(cls, obj: Mapping[str, Any]) -> Place:
        return cls(
            type=_str_or_none(obj.get("type")),
            key=_str_or_none(obj.get("key")),
            name=_str_or_none(obj.get("name")),
            unit=_str_or_none(obj.get("unit")),
            area=_str_or_none(obj.get("area")),
            areas=_parse_str_tuple(obj.get("areas")),
            location=_parse_float_tuple(obj.get("location")),
            fuels=_parse_str_tuple(obj.get("fuels")),
            remit_units=_parse_str_tuple(obj.get("remit_units")),
        )


@dataclass(frozen=True, slots=True)
class Curve:
    id: int = 0
    name: str = ""
    area: str | None = None
    area_sink: str | None = None
    categories: tuple[str, ...] = ()
    resolution: Resolution = field(default_factory=Resolution)
    unit: str | None = None
    denominator: str | None = None
    source: str | None = None
    data_type: DataType = DataType.NORMAL
    curve_type: CurveType = CurveType.TIMESERIES
    access: Access = field(default_factory=Access)
    subscription: Subscription = field(default_factory=Subscription)
    commodity: str | None = None
    instance_issued_timezone: str | None = None
    place: Place = field(default_factory=Place)

    def __post_init__(self) -> None:
        if self.id == 0 and self.name:
            object.__setattr__(self, "id", _xxh3_id(self.name))

    def to_metadata_row(self, *, now: dt.datetime) -> dict:
        return {
            "curve_id": self.id,
            "curve_name": self.name,
            "curve_type": self.curve_type.name,
            "curve_data_type": self.data_type.name,
            "curve_area": self.area,
            "curve_area_sink": self.area_sink,
            "curve_commodity": self.commodity,
            "curve_source": self.source,
            "curve_unit": self.unit,
            "curve_denominator": self.denominator,
            "curve_categories": list(self.categories),
            "curve_resolution_frequency": self.resolution.frequency,
            "curve_resolution_timezone": self.resolution.timezone,
            "curve_access_by": self.access.by,
            "curve_access_package": self.access.package,
            "curve_instance_issued_timezone": self.instance_issued_timezone,
            "table_category": self.table_name(),
            "updated_at": now,
        }

    def table_name(self, prefix: str = "") -> str:
        key = (self.data_type.name, self.curve_type.name, self.categories[:2], prefix)
        cached = _TABLE_NAME_CACHE.get(key)
        if cached is not None:
            return cached

        data_type = _safe_name(self.data_type.name) or "none"
        curve_type = _safe_name(self.curve_type.name) or "none"

        if self.categories:
            categories = "_" + "_".join(
                _safe_name(c) for c in self.categories[:2] if _safe_name(c)
            )
        else:
            categories = ""

        safe_prefix = _safe_name(prefix)
        safe_prefix = f"{safe_prefix}_" if safe_prefix else ""

        result = f"{safe_prefix}{data_type}_{curve_type}{categories}"
        _TABLE_NAME_CACHE[key] = result
        return result

    @property
    def is_instance(self) -> bool:
        return self.curve_type == CurveType.INSTANCE

    @property
    def is_period_instance(self) -> bool:
        return self.curve_type == CurveType.INSTANCE_PERIOD

    @property
    def tags(self) -> dict[str, str]:
        tags: dict[str, str] = {}
        if self.name:
            tags["name"] = self.name
        if self.area:
            tags["area"] = self.area
        if self.unit:
            tags["unit"] = self.unit
        if self.commodity:
            tags["commodity"] = self.commodity
        tags["data_type"] = self.data_type.name
        tags["curve_type"] = self.curve_type.name
        if self.categories:
            tags["categories"] = ",".join(self.categories)
        return tags

    @classmethod
    def parse(cls, obj: Any) -> Curve:
        if obj is None:
            raise ValueError("Cannot parse None as Curve")
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, Mapping):
            return cls.parse_mapping(obj)
        raise TypeError(f"Expected {cls.__name__}, mapping, or None, got {type(obj).__name__}")

    @classmethod
    def parse_mapping(cls, obj: Mapping[str, Any]) -> Curve:
        return cls(
            name=_str_or_none(obj.get("name")) or "",
            area=_str_or_none(obj.get("area")),
            area_sink=_str_or_none(obj.get("area_sink")),
            categories=_parse_str_tuple(obj.get("categories")),
            resolution=Resolution.parse(obj.get("resolution")),
            unit=_str_or_none(obj.get("unit")),
            denominator=_str_or_none(obj.get("denominator")),
            source=_str_or_none(obj.get("source")),
            data_type=_coerce_data_type(obj.get("data_type")),
            curve_type=_coerce_curve_type(obj.get("curve_type")),
            access=Access.parse(obj.get("access")),
            subscription=Subscription.parse(obj.get("subscription")),
            commodity=_str_or_none(obj.get("commodity")),
            instance_issued_timezone=_str_or_none(obj.get("instance_issued_timezone")),
            place=Place.parse(obj.get("place")),
        )


def _coerce_datetime(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc)
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return dt.datetime.fromisoformat(s)
    raise TypeError(f"Expected datetime, str, or None, got {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class Instance:
    curve: Curve
    issued_at: dt.datetime | None = None
    created_at: dt.datetime | None = None
    modified_at: dt.datetime | None = None
    tag: str | None = None

    @property
    def tags(self) -> list[str]:
        if not self.tag:
            return []
        return [self.tag]

    def __post_init__(self) -> None:
        if self.issued_at is not None:
            object.__setattr__(self, "issued_at", _coerce_datetime(self.issued_at))
        if self.created_at is not None:
            object.__setattr__(self, "created_at", _coerce_datetime(self.created_at))
        if self.modified_at is not None:
            object.__setattr__(self, "modified_at", _coerce_datetime(self.modified_at))