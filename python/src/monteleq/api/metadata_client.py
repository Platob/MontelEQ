"""MetadataClient – curve-catalog queries and in-memory lookup."""
from __future__ import annotations

import re
import polars as pl
import datetime as dt
from fnmatch import translate
from typing import Optional, TYPE_CHECKING, Iterable

from energyquantified.metadata import CurveType, DataType

from yggdrasil.io.send_config import CacheConfig, SendConfig

from monteleq.model import Curve, _xxh3_id, _safe_name

if TYPE_CHECKING:
    from yggdrasil.io.request import PreparedRequest
    from yggdrasil.io.response import Response
    from monteleq.api._base_client import BaseClient

__all__ = ["MetadataClient"]


class MetadataClient:
    """
    Client for EnergyQuantified curve-catalog operations.

    Usage via ``APIClient``::

        client.metadata.curves(curve_type="TIMESERIES")
        client.metadata.curvemap["Hydro NO Total >"]
    """

    def __init__(self, base: "BaseClient") -> None:
        self._base = base
        self._df: pl.DataFrame | None = None
        self._curves: dict[str, Curve] | None = None
        self._name_keys: tuple[str, ...] | None = None
        self._name_lc_values: pl.Series | None = None

    # ------------------------------------------------------------------
    # Raw catalog HTTP
    # ------------------------------------------------------------------
    def iter_curves(self, curves: str | Iterable[Curve | str]) -> Iterable[Curve]:
        if isinstance(curves, str):
            return self.curves(name=curves)
        elif isinstance(curves, Curve):
            return [curves]
        return curves

    def request(self) -> PreparedRequest:
        return self._base.prepare_request(
            method="GET",
            url="metadata/curves/",
            headers={"Accept": "application/json", "Accept-Encoding": "gzip"},
            tags={"endpoint": "metadata_curves"},
            send_config=SendConfig(
                local_cache=CacheConfig(received_ttl=dt.timedelta(days=7)),
                remote_cache=CacheConfig(
                    tabular=self._base.check_cache_param(
                        cache=None, table_name="raw_metadata_curves",
                    ),
                ),
            ),
        )

    def fetch(self) -> Response:
        return self.request().send()

    def fetch_df(self) -> pl.DataFrame:
        if self._df is None:
            self._df = pl.from_arrow(self.fetch().to_arrow_table())
        return self._df

    # ------------------------------------------------------------------
    # In-memory curve map
    # ------------------------------------------------------------------

    @property
    def curvemap(self) -> dict[str, Curve]:
        if self._curves is None:
            with self._base._lock:
                if self._curves is None:
                    self._curves = {
                        row["name"]: Curve.parse_mapping(row)
                        for row in self.fetch_df().iter_rows(named=True)
                    }
        return self._curves

    def metadata_df(self, *, now: dt.datetime | None = None) -> pl.DataFrame:
        from monteleq.api.schemas import CURVE_METADATA_SCHEMA

        if now is None:
            now = dt.datetime.now(dt.timezone.utc)

        df = self.fetch_df()

        return df.select(
            pl.col("name").map_elements(_xxh3_id, return_dtype=pl.Int64).alias("curve_id"),
            pl.col("name").alias("curve_name"),
            pl.col("curve_type"),
            pl.col("data_type").alias("curve_data_type"),
            pl.col("area").alias("curve_area"),
            pl.col("area_sink").alias("curve_area_sink"),
            pl.col("commodity").alias("curve_commodity"),
            pl.col("source").alias("curve_source"),
            pl.col("unit").alias("curve_unit"),
            pl.col("denominator").alias("curve_denominator"),
            pl.col("categories").alias("curve_categories"),
            pl.col("resolution").struct.field("frequency").alias("curve_resolution_frequency"),
            pl.col("resolution").struct.field("timezone").alias("curve_resolution_timezone"),
            pl.lit(None).alias("curve_access_by"),
            pl.lit(None).alias("curve_access_package"),
            pl.col("instance_issued_timezone").alias("curve_instance_issued_timezone"),
            pl.struct("data_type", "curve_type", "categories")
            .map_elements(_table_category, return_dtype=pl.Utf8)
            .alias("table_category"),
            pl.lit(now).alias("updated_at"),
        ).cast(CURVE_METADATA_SCHEMA.to_polars_schema())

    def _name_index(self) -> tuple[tuple[str, ...], pl.Series]:
        """Lazily-built (original_keys, lowercased_values_series).

        The lowercased side is a polars Utf8 Series so downstream matching
        can stay in Rust (``str.contains``) rather than looping in Python.
        """
        if self._name_keys is None:
            cm = self.curvemap
            keys = tuple(cm.keys())
            # Build the lowercase column once in Rust. `strict=False` is not
            # needed here — all keys are guaranteed non-null strings.
            self._name_keys = keys
            self._name_lc_values = pl.Series("name_lc", keys, dtype=pl.Utf8).str.to_lowercase()
        return self._name_keys, self._name_lc_values  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Listing helpers
    # ------------------------------------------------------------------

    def curve_data_types(self) -> set[tuple[CurveType, DataType]]:
        return {(c.curve_type, c.data_type) for c in self.curvemap.values()}

    def volcano_curves(self) -> list[Curve]:
        df = pl.DataFrame(self.curves(), infer_schema_length=100000)
        cat = ["Exchange Net Transfer Capacity", "Price Spot", "Consumption",
               "Hydro Pumped-storage Pumping", "Exchange Day-Ahead Schedule"]

        df_norm = df.with_columns(
            pl.col("data_type").map_elements(lambda x: x.name, return_dtype=pl.String).alias("data_type"),
            pl.col("curve_type").map_elements(lambda x: x.name, return_dtype=pl.String).alias("curve_type"),
        )

        curves = pl.concat(
            [
                df_norm.filter(
                    (pl.col("data_type") == "REMIT")
                    & (pl.col("curve_type") == "INSTANCE_PERIOD")
                    & pl.col("categories").list.eval(pl.element().str.contains("Capacity Available")).list.any()
                ),
                df_norm.filter(
                    (pl.col("data_type") == "FORECAST")
                    & (pl.col("curve_type") == "INSTANCE")
                    & pl.col("categories").list.eval(pl.element().is_in(cat)).list.any()
                ),
                df_norm.filter(
                    (pl.col("data_type") == "REMIT")
                    & (pl.col("curve_type") == "TIMESERIES")
                    & pl.col("categories").list.eval(pl.element().is_in(cat)).list.any()
                ),
                df_norm.filter(
                    (pl.col("data_type") == "ACTUAL")
                    & (pl.col("curve_type") == "TIMESERIES")
                    & pl.col("categories").list.eval(pl.element().is_in(cat)).list.any()
                ),
                df_norm.filter(
                    (pl.col("curve_type") == "TIMESERIES")
                    & pl.col("categories").list.eval(pl.element().str.contains("Production")).list.any()
                ),
                df_norm.filter(
                    (pl.col("curve_type") == "TIMESERIES")
                    & pl.col("categories").list.eval(pl.element().str.contains("Consumption")).list.any()
                ),
            ],
            how="vertical",
        ).unique(subset=["id"])

        return [self.curvemap[name] for name in curves["name"].unique().to_list()]

    def curves(
        self,
        name: Optional[str | Iterable[str]] = None,
        curve_type: Optional[str | CurveType | list[str] | list[CurveType]] = None,
        data_type: Optional[str | DataType | list[str] | list[DataType]] = None,
        area: Optional[str | list[str]] = None,
        area_sink: Optional[str | list[str]] = None,
        commodity: Optional[str | list[str]] = None,
        source: Optional[str | list[str]] = None,
        categories: Optional[str | list[str]] = None,
        frequency: Optional[str | list[str]] = None,
        timezone: Optional[str | list[str]] = None,
        unit: Optional[str | list[str]] = None,
        denominator: Optional[str | list[str]] = None,
        instance_issued_timezone: Optional[str | list[str]] = None,
        place_key: Optional[str | list[str]] = None,
        place_area: Optional[str | list[str]] = None,
        place_fuel: Optional[str | list[str]] = None,
        access_by: Optional[str | list[str]] = None,
        access_package: Optional[str | list[str]] = None,
    ) -> list[Curve]:
        """Return curves, optionally filtered by any combination of attributes."""
        cm = self.curvemap

        # ---- 1) NAME PREFILTER ------------------------------------------------
        if name is not None:
            if isinstance(name, str):
                patterns = (name,)
            else:
                patterns = tuple(name)

            if (
                isinstance(name, str)
                and "*" not in name
                and "?" not in name
                and name in cm
            ):
                return [cm[name]]

            keys, lc_values = self._name_index()
            selected_idx = _name_match_indices(patterns, lc_values)
            candidates = [cm[keys[i]] for i in selected_idx]
        else:
            candidates = list(cm.values())

        # ---- 2) BUILD PREDICATE LIST -----------------------------------------
        # Collect all active filters, then apply in a single pass over
        # candidates instead of N sequential list comprehensions.
        predicates: list = []

        if curve_type is not None:
            if isinstance(curve_type, (str, CurveType)):
                curve_type = (curve_type,)
            ct_set = frozenset(
                ct if isinstance(ct, CurveType) else CurveType[ct] for ct in curve_type
            )
            predicates.append(lambda c, s=ct_set: c.curve_type in s)

        if data_type is not None:
            if isinstance(data_type, (str, DataType)):
                data_type = (data_type,)
            dt_set = frozenset(
                dt_ if isinstance(dt_, DataType) else DataType[dt_] for dt_ in data_type
            )
            predicates.append(lambda c, s=dt_set: c.data_type in s)

        if area is not None:
            s = frozenset((area,) if isinstance(area, str) else area)
            predicates.append(lambda c, s=s: c.area in s)

        if area_sink is not None:
            s = frozenset((area_sink,) if isinstance(area_sink, str) else area_sink)
            predicates.append(lambda c, s=s: c.area_sink in s)

        if commodity is not None:
            s = frozenset((commodity,) if isinstance(commodity, str) else commodity)
            predicates.append(lambda c, s=s: c.commodity in s)

        if source is not None:
            s = frozenset((source,) if isinstance(source, str) else source)
            predicates.append(lambda c, s=s: c.source in s)

        if categories is not None:
            s = frozenset((categories,) if isinstance(categories, str) else categories)
            predicates.append(lambda c, s=s: s.issubset(c.categories))

        if frequency is not None:
            s = frozenset((frequency,) if isinstance(frequency, str) else frequency)
            predicates.append(lambda c, s=s: (c.resolution.frequency or "") in s)

        if timezone is not None:
            s = frozenset((timezone,) if isinstance(timezone, str) else timezone)
            predicates.append(lambda c, s=s: (c.resolution.timezone or "") in s)

        if unit is not None:
            s = frozenset((unit,) if isinstance(unit, str) else unit)
            predicates.append(lambda c, s=s: c.unit in s)

        if denominator is not None:
            s = frozenset((denominator,) if isinstance(denominator, str) else denominator)
            predicates.append(lambda c, s=s: c.denominator in s)

        if instance_issued_timezone is not None:
            s = frozenset(
                (instance_issued_timezone,)
                if isinstance(instance_issued_timezone, str)
                else instance_issued_timezone
            )
            predicates.append(lambda c, s=s: c.instance_issued_timezone in s)

        if place_key is not None:
            s = frozenset((place_key,) if isinstance(place_key, str) else place_key)
            predicates.append(lambda c, s=s: c.place.key in s)

        if place_area is not None:
            s = frozenset((place_area,) if isinstance(place_area, str) else place_area)
            predicates.append(lambda c, s=s: c.place.area in s or not s.isdisjoint(c.place.areas))

        if place_fuel is not None:
            s = frozenset((place_fuel,) if isinstance(place_fuel, str) else place_fuel)
            predicates.append(lambda c, s=s: not s.isdisjoint(c.place.fuels))

        if access_by is not None:
            s = frozenset((access_by,) if isinstance(access_by, str) else access_by)
            predicates.append(lambda c, s=s: c.access.by in s)

        if access_package is not None:
            s = frozenset((access_package,) if isinstance(access_package, str) else access_package)
            predicates.append(lambda c, s=s: c.access.package in s)

        # ---- 3) SINGLE-PASS FILTER -------------------------------------------
        if not predicates:
            return candidates

        return [c for c in candidates if all(p(c) for p in predicates)]

    # ------------------------------------------------------------------
    # Internal lookup
    # ------------------------------------------------------------------

    def _curve_infos(self, curve: Curve | str) -> Curve:
        if isinstance(curve, Curve):
            return curve
        infos = self.curvemap.get(curve)
        if not infos:
            raise ValueError(f"Curve {curve!r} not found")
        return infos


# ----------------------------------------------------------------------
# Module-level helpers
# ----------------------------------------------------------------------

def _table_category(row: dict) -> str:
    dt_name = _safe_name(row["data_type"] or "") or "none"
    ct_name = _safe_name(row["curve_type"] or "") or "none"
    cats = row["categories"] or []
    parts = [_safe_name(c) for c in cats[:2] if _safe_name(c)]
    cat_suffix = "_" + "_".join(parts) if parts else ""
    return f"{dt_name}_{ct_name}{cat_suffix}"


def _compile_name_patterns(patterns: tuple[str, ...]) -> re.Pattern[str]:
    """
    Compile a tuple of name patterns into a single combined regex.

    Preserves original semantics:
      - case-insensitive (patterns are passed in lowercased)
      - `*` / `?` => glob (via fnmatch.translate, whole-string anchored)
      - otherwise => substring contains
    """
    parts: list[str] = []
    for p in patterns:
        if "*" in p or "?" in p:
            # fnmatch.translate adds (?s:...)\Z anchors — whole-string match,
            # consistent with fnmatchcase.
            parts.append(translate(p))
        else:
            parts.append(f".*{re.escape(p)}.*")
    combined = "|".join(f"(?:{part})" for part in parts)
    return re.compile(combined)


# Per-process cache of compiled name-pattern regexes. Patterns are typically
# reused across calls (UI dropdowns, scripted scans), so caching the compile
# step removes the dominant cost when the candidate set is large.
from functools import lru_cache


@lru_cache(maxsize=512)
def _compile_name_patterns_cached(patterns: tuple[str, ...]) -> re.Pattern[str]:
    return _compile_name_patterns(patterns)


def _glob_to_rust_regex(glob: str) -> str:
    """Convert a shell glob pattern to a Rust-compatible regex.

    ``fnmatch.translate`` emits Python-specific constructs (``(?s:...)``,
    ``(?>...)``, ``\\Z``) that the Rust ``regex`` crate rejects.  Instead
    of stripping them we translate the glob directly: ``*`` → ``.*``,
    ``?`` → ``.``, everything else escaped.
    """
    parts: list[str] = []
    for ch in glob:
        if ch == "*":
            parts.append(".*")
        elif ch == "?":
            parts.append(".")
        else:
            parts.append(re.escape(ch))
    return "^" + "".join(parts) + "$"


def _build_polars_pattern(patterns: tuple[str, ...]) -> str:
    """Build a combined regex string compatible with polars' Rust regex engine."""
    parts: list[str] = []
    for p in patterns:
        if "*" in p or "?" in p:
            parts.append(_glob_to_rust_regex(p))
        else:
            parts.append(re.escape(p))
    return "|".join(f"(?:{part})" for part in parts)


@lru_cache(maxsize=512)
def _build_polars_pattern_cached(patterns: tuple[str, ...]) -> str:
    return _build_polars_pattern(patterns)


def _name_match_indices(
    patterns: tuple[str, ...],
    lc_values: pl.Series,
) -> list[int]:
    """Vectorized name match. Returns positional indices into ``lc_values``
    whose lowercased value matches any of the (case-insensitive) patterns.

    Matching is delegated to polars' ``str.contains`` (Rust regex engine),
    which is ~1-2 orders of magnitude faster than a Python list comprehension
    over ``re.Pattern.match`` for catalogs in the 10k-100k range.
    """
    lc_patterns = tuple(p.lower() for p in patterns)
    pat = _build_polars_pattern_cached(lc_patterns)
    mask = lc_values.str.contains(pat, literal=False)
    # arg_true returns a UInt32 Series of indices where mask is True.
    return mask.arg_true().to_list()