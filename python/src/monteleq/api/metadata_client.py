"""MetadataClient – curve-catalog queries and in-memory lookup."""
from __future__ import annotations

import re
import polars as pl
import datetime as dt
from fnmatch import translate
from typing import Optional, TYPE_CHECKING, Iterable

from energyquantified.metadata import CurveType, DataType

from monteleq.model import Curve

if TYPE_CHECKING:
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
        self._curves: dict[str, Curve] | None = None
        # Cached name index for vectorized matching. `_name_keys` is the
        # original-case key list (positionally aligned with `_name_lc_values`).
        # `_name_lc_values` is a polars Utf8 Series of lowercased names — the
        # lowercasing is done once in Rust and held for the lifetime of the
        # curvemap. Built lazily on first use of the substring/glob path.
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

    def request(self):
        return self._base.prepare_request(
            method="GET",
            url="metadata/curves/",
            headers={"Accept": "application/json", "Accept-Encoding": "gzip"},
            tags={"endpoint": "metadata_curves"},
        )

    def fetch(self):
        if self._base._databricks:
            cache = self._base.check_cache_param(cache=None, table_name="raw_metadata_curves")
        else:
            cache = None
        return self._base.send(self.request(), remote_cache=cache, local_cache=dt.timedelta(days=7))

    # ------------------------------------------------------------------
    # In-memory curve map
    # ------------------------------------------------------------------

    @property
    def curvemap(self) -> dict[str, Curve]:
        if self._curves is None:
            with self._base._lock:
                if self._curves is None:
                    self._curves = {
                        infos["name"]: Curve.parse_mapping(infos)
                        for infos in self.fetch().json()
                    }
        return self._curves

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

    def volcano_curves(self):
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
        # Run name first when present: it's the most selective filter and the
        # only one that can shrink the candidate set below O(N) via dict lookup.
        if name is not None:
            if isinstance(name, str):
                patterns = (name,)
            else:
                patterns = tuple(name)

            # Single-pattern, exact key, no wildcards: O(1) short-circuit.
            # Matches the original `if name and isinstance(name, str)` branch
            # which returned [found] without applying other filters.
            if (
                isinstance(name, str)
                and "*" not in name
                and "?" not in name
                and name in cm
            ):
                return [cm[name]]

            # Build candidate index list via vectorized substring/glob match.
            keys, lc_values = self._name_index()
            selected_idx = _name_match_indices(patterns, lc_values)
            result = [cm[keys[i]] for i in selected_idx]
        else:
            result = list(cm.values())

        # ---- 2) NORMALIZE FILTER ARGS ONCE ------------------------------------
        # Pre-convert enum-string filters to sets for O(1) membership.

        if curve_type is not None:
            if isinstance(curve_type, (str, CurveType)):
                curve_type = (curve_type,)
            ct_set = frozenset(
                ct if isinstance(ct, CurveType) else CurveType[ct] for ct in curve_type
            )
            result = [c for c in result if c.curve_type in ct_set]

        if data_type is not None:
            if isinstance(data_type, (str, DataType)):
                data_type = (data_type,)
            dt_set = frozenset(
                dt_ if isinstance(dt_, DataType) else DataType[dt_] for dt_ in data_type
            )
            result = [c for c in result if c.data_type in dt_set]

        if area is not None:
            area_set = frozenset((area,) if isinstance(area, str) else area)
            result = [c for c in result if c.area in area_set]

        if area_sink is not None:
            asink_set = frozenset((area_sink,) if isinstance(area_sink, str) else area_sink)
            result = [c for c in result if c.area_sink in asink_set]

        if commodity is not None:
            com_set = frozenset((commodity,) if isinstance(commodity, str) else commodity)
            result = [c for c in result if c.commodity in com_set]

        if source is not None:
            src_set = frozenset((source,) if isinstance(source, str) else source)
            result = [c for c in result if c.source in src_set]

        if categories is not None:
            cats = frozenset((categories,) if isinstance(categories, str) else categories)
            # issubset against the curve's categories — wrap once per curve.
            result = [c for c in result if cats.issubset(c.categories)]

        if frequency is not None:
            freq_set = frozenset((frequency,) if isinstance(frequency, str) else frequency)
            result = [c for c in result if (c.resolution.frequency or "") in freq_set]

        if timezone is not None:
            tz_set = frozenset((timezone,) if isinstance(timezone, str) else timezone)
            result = [c for c in result if (c.resolution.timezone or "") in tz_set]

        if unit is not None:
            unit_set = frozenset((unit,) if isinstance(unit, str) else unit)
            result = [c for c in result if c.unit in unit_set]

        if denominator is not None:
            den_set = frozenset((denominator,) if isinstance(denominator, str) else denominator)
            result = [c for c in result if c.denominator in den_set]

        if instance_issued_timezone is not None:
            itz_set = frozenset(
                (instance_issued_timezone,)
                if isinstance(instance_issued_timezone, str)
                else instance_issued_timezone
            )
            result = [c for c in result if c.instance_issued_timezone in itz_set]

        if place_key is not None:
            pk_set = frozenset((place_key,) if isinstance(place_key, str) else place_key)
            result = [c for c in result if c.place.key in pk_set]

        if place_area is not None:
            pa_set = frozenset((place_area,) if isinstance(place_area, str) else place_area)
            result = [
                c for c in result
                if c.place.area in pa_set or not pa_set.isdisjoint(c.place.areas)
            ]

        if place_fuel is not None:
            fuel_set = frozenset((place_fuel,) if isinstance(place_fuel, str) else place_fuel)
            result = [c for c in result if not fuel_set.isdisjoint(c.place.fuels)]

        if access_by is not None:
            by_set = frozenset((access_by,) if isinstance(access_by, str) else access_by)
            result = [c for c in result if c.access.by in by_set]

        if access_package is not None:
            pkg_set = frozenset((access_package,) if isinstance(access_package, str) else access_package)
            result = [c for c in result if c.access.package in pkg_set]

        return result

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


def _build_polars_pattern(patterns: tuple[str, ...]) -> str:
    """Build a combined regex string compatible with polars' Rust regex engine.

    Differs from ``_compile_name_patterns`` in two ways the Rust ``regex``
    crate cares about:
      - ``\\Z`` (Python re) → ``$`` with multi-line off (default), i.e.
        end-of-string. The crate uses ``\\z`` lowercase; we use ``$`` because
        it's universally supported.
      - We do not need the ``(?s:...)`` dotall group ``fnmatch.translate``
        emits, because our glob translations don't use ``.`` to match
        newlines in curve names (names don't contain newlines). We strip
        the wrapper to keep the regex simple and portable.
    """
    parts: list[str] = []
    for p in patterns:
        if "*" in p or "?" in p:
            # fnmatch.translate output across CPython versions:
            #   3.8:  "(?s:...)\\Z"
            #   3.12: "(?s:...)\\Z"
            # We unwrap the (?s:...) group and replace \Z with $ for the
            # Rust regex crate. Dotall is irrelevant for names.
            t = translate(p)
            # Strip leading (?s: and trailing )\Z if present.
            if t.startswith("(?s:") and t.endswith(r")\Z"):
                t = t[4:-3]
            elif t.endswith(r"\Z"):
                t = t[:-2]
            parts.append(f"^{t}$")
        else:
            # Substring contains, anchored nowhere.
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