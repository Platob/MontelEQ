# model.py
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass, field
from typing import Any, Mapping

__all__ = [
    "Resolution",
    "Access",
    "Subscription",
    "Place",
    "Curve",
    "Instance",
    "DEFAULT_ISSUE_INTERVAL"
]

from energyquantified.metadata import CurveType, DataType
from yggdrasil.data.cast import any_to_datetime
from yggdrasil.io import BytesIO

DEFAULT_ISSUE_INTERVAL = dt.timedelta(hours=4)
_UNSAFE_CHARS = re.compile(r"[^a-z0-9]+")

def _safe_name(value: str) -> str:
    """Lowercase and replace runs of non-alphanumerics with a single underscore."""
    return _UNSAFE_CHARS.sub("_", value.lower()).strip("_")

@dataclass(frozen=True, slots=True)
class Resolution:
    frequency: str | None = None
    timezone: str | None = None

    def __post_init__(self):
        if self.frequency:
            object.__setattr__(
                self, "frequency",
                None if self.frequency.lower() in ["none", ""] else self.frequency
            )

    @classmethod
    def parse(cls, obj: Any) -> "Resolution":
        if obj is None:
            return cls()
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, Mapping):
            return cls.parse_mapping(obj)
        raise TypeError(f"Expected {cls.__name__}, mapping, or None, got {type(obj).__name__}")

    @classmethod
    def parse_mapping(cls, obj: Mapping[str, Any]) -> "Resolution":
        return cls(
            frequency=_str_or_none(obj.get("frequency")),
            timezone=_str_or_none(obj.get("timezone")),
        )


@dataclass(frozen=True, slots=True)
class Access:
    by: str | None = None
    package: str | None = None

    @classmethod
    def parse(cls, obj: Any) -> "Access":
        if obj is None:
            return cls()
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, Mapping):
            return cls.parse_mapping(obj)
        raise TypeError(f"Expected {cls.__name__}, mapping, or None, got {type(obj).__name__}")

    @classmethod
    def parse_mapping(cls, obj: Mapping[str, Any]) -> "Access":
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
    def parse(cls, obj: Any) -> "Subscription":
        if obj is None:
            return cls()
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, Mapping):
            return cls.parse_mapping(obj)
        raise TypeError(f"Expected {cls.__name__}, mapping, or None, got {type(obj).__name__}")

    @classmethod
    def parse_mapping(cls, obj: Mapping[str, Any]) -> "Subscription":
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
    def parse(cls, obj: Any) -> "Place":
        if obj is None:
            return cls()
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, Mapping):
            return cls.parse_mapping(obj)
        raise TypeError(f"Expected {cls.__name__}, mapping, or None, got {type(obj).__name__}")

    @classmethod
    def parse_mapping(cls, obj: Mapping[str, Any]) -> "Place":
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
    commodity: str = "None"
    instance_issued_timezone: str | None = None
    place: Place = field(default_factory=Place)

    def __post_init__(self):
        if self.id == 0:
            buff = BytesIO()
            buff.write(self.name)
            object.__setattr__(self, "id", buff.xxh3_int64())

    def table_name(self, prefix: str = "") -> str:
        curve_type = _safe_name(self.curve_type.name) or "none"
        data_type = _safe_name(self.data_type.name) or "none"

        if self.categories:
            categories = "_" + "_".join(_safe_name(c) for c in self.categories[:2] if _safe_name(c))
        else:
            categories = ""

        safe_prefix = _safe_name(prefix)
        safe_prefix = f"{safe_prefix}_" if safe_prefix else ""

        return f"{safe_prefix}{data_type}_{curve_type}{categories}"

    @property
    def is_instance(self) -> bool:
        return self.curve_type == CurveType.INSTANCE

    @property
    def is_period_instance(self) -> bool:
        return self.curve_type == CurveType.INSTANCE_PERIOD

    @property
    def tags(self) -> dict[str, str]:
        tags: dict[str, str] = {
            "curve_id": str(self.id),
            "curve_name": self.name,
            "curve_commodity": self.commodity,
            "curve_data_type": self.data_type.name,
            "curve_type": self.curve_type.name,
        }

        if self.categories:
            tags["categories"] = ",".join(self.categories)

        return tags

    @classmethod
    def parse(cls, obj: Any) -> "Curve":
        if obj is None:
            raise ValueError("Cannot parse None as Curve")

        if isinstance(obj, cls):
            return obj
        if isinstance(obj, Mapping):
            return cls.parse_mapping(obj)
        raise TypeError(f"Expected {cls.__name__}, mapping, or None, got {type(obj).__name__}")

    @classmethod
    def parse_mapping(cls, obj: Mapping[str, Any]) -> "Curve":
        return cls(
            name=_str_or_none(obj.get("name")),
            area=_str_or_none(obj.get("area")),
            area_sink=_str_or_none(obj.get("area_sink")),
            categories=_parse_str_tuple(obj.get("categories")),
            resolution=Resolution.parse(obj.get("resolution")),
            unit=_str_or_none(obj.get("unit")),
            denominator=_str_or_none(obj.get("denominator")),
            source=_str_or_none(obj.get("source")),
            data_type=DataType[_str_or_none(obj.get("data_type"))],
            curve_type=CurveType[_str_or_none(obj.get("curve_type"))],
            access=Access.parse(obj.get("access")),
            subscription=Subscription.parse(obj.get("subscription")),
            commodity=_str_or_none(obj.get("commodity")),
            instance_issued_timezone=_str_or_none(obj.get("instance_issued_timezone")),
            place=Place.parse(obj.get("place")),
        )


@dataclass(frozen=True, slots=True)
class Instance:
    curve: Curve
    issued_at: dt.datetime
    created_at: dt.datetime | None
    modified_at: dt.datetime | None
    tag: str

    @property
    def tags(self):
        if not self.tag:
            return []
        return [self.tag]

    def __post_init__(self):
        if self.issued_at:
            object.__setattr__(
                self, "issued_at",
                any_to_datetime(self.issued_at, tz=dt.timezone.utc)
            )

        if self.created_at:
            object.__setattr__(
                self, "created_at",
                any_to_datetime(self.created_at, tz=dt.timezone.utc)
            )

        if self.modified_at:
            object.__setattr__(
                self, "modified_at",
                any_to_datetime(self.modified_at, tz=dt.timezone.utc)
            )


def _str_or_none(value: Any) -> str | None:
    return None if value is None else str(value)


def _parse_str_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(str(v) for v in value)
    raise TypeError(f"Expected list, tuple, or None, got {type(value).__name__}")


def _parse_float_tuple(value: Any) -> tuple[float, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(float(v) for v in value)
    raise TypeError(f"Expected list, tuple, or None, got {type(value).__name__}")