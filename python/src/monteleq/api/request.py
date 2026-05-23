import datetime as dt
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterable, Union, Iterator
from zoneinfo import ZoneInfo

from energyquantified.events import CurveUpdateEvent, EventType
from energyquantified.events.events import _Event
from energyquantified.metadata import CurveType, Instance as EQInstance
from yggdrasil.data.cast import any_to_datetime, iter_datetime_ranges, truncate_datetime
from yggdrasil.io import URL
from yggdrasil.io.request import PreparedRequest

from monteleq.model import Curve, DEFAULT_ISSUE_INTERVAL

if TYPE_CHECKING:
    from yggdrasil.io.send_config import CacheConfig
    from .client import APIClient

__all__ = [
    "CurveRequest", "CurveRequestsArg", "CurveRequestArg"
]

CurveRequestArg = Union[
    str, Curve, "CurveRequest",
]
CurveRequestsArg = Union[
    CurveRequestArg, Iterable[CurveRequestArg]
]
REQUEST_HEADERS = {"Accept": "application/json", "Accept-Encoding": "gzip"}
DATETIME_SPAN = dt.timedelta(hours=4)
DATETIME_WINDOW_COUNT = 24
MAX_DATETIME_SPAN = DATETIME_SPAN * DATETIME_WINDOW_COUNT
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


def utc_now_ceil_hour() -> dt.datetime:
    return (
        dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
        + dt.timedelta(hours=1)
    )


def is_timezone_relevant_frequency(frequency: str | None) -> bool:
    if not frequency:
        return True

    freq = str(frequency).strip().upper()
    if not freq.startswith("P"):
        return True

    if "T" in freq:
        return True

    return False


def _normalize_timezone_param(timezone: str | None) -> str | None:
    if timezone is None:
        return None

    value = str(timezone).strip()
    if not value:
        return None

    if value.upper() in {"UTC", "Z", "ETC/UTC"}:
        return "UTC"

    try:
        return ZoneInfo(value).key
    except Exception:
        return value


def _effective_timezone(
    curve: Curve,
    frequency: str | None,
    timezone: str | None,
) -> str | None:
    if curve.curve_type in [CurveType.OHLC]:
        return None

    if not is_timezone_relevant_frequency(frequency):
        return None
    return _normalize_timezone_param(timezone or "UTC")


@dataclass
class CurveRequest:
    """Domain-level description of a curve fetch.

    This is *not* a :class:`PreparedRequest` — it's the inputs needed to
    build one. Call :meth:`to_request` to materialize an HTTP request
    suitable for ``Session.send`` / ``send_many``.
    """

    curve: Curve = field(default_factory=lambda: Curve(name=""))
    begin: dt.datetime | None = None
    end: dt.datetime | None = None
    issued_at: dt.datetime | None = None
    issued_at_earliest: dt.datetime | None = None
    issued_at_latest: dt.datetime | None = None
    request_tags: list[str] = field(default_factory=list)
    limit: int | None = None
    exclude_tags: list[str] = field(default_factory=list)
    ensembles: bool = False
    timezone: str | None = None
    unit: str | None = None
    frequency: str | None = None
    event_type: EventType | None = None
    raise_error: bool = True
    tags: dict[str, str] = field(default_factory=dict)
    client: "APIClient | None" = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.curve, Curve):
            raise TypeError(f"curve must be a Curve instance, got {type(self.curve).__name__}")

        now = dt.datetime.now(dt.timezone.utc)

        self.end = any_to_datetime(self.end or now, tz=dt.timezone.utc)
        self.begin = any_to_datetime(
            self.begin or self.end - dt.timedelta(days=14),
            tz=dt.timezone.utc,
        )

        if self.issued_at:
            self.issued_at = any_to_datetime(self.issued_at, tz=dt.timezone.utc)

        self.issued_at_latest = any_to_datetime(
            self.issued_at_latest or now, tz=dt.timezone.utc
        )
        self.issued_at_earliest = any_to_datetime(
            self.issued_at_earliest or self.issued_at_latest - dt.timedelta(days=7),
            tz=dt.timezone.utc,
        )

        if self.request_tags:
            if isinstance(self.request_tags, str):
                self.request_tags = [self.request_tags]
            else:
                self.request_tags = [str(t) for t in self.request_tags if t]
        else:
            self.request_tags = []

        if self.curve.curve_type == CurveType.INSTANCE_PERIOD:
            self.limit = max(1, min(int(self.limit or 20), 20))
        elif self.curve.curve_type == CurveType.INSTANCE:
            self.limit = max(
                1, min(int(self.limit or 25), 10 if self.ensembles else 25)
            )
        else:
            self.limit = None

        if not self.frequency:
            self.frequency = self.curve.resolution.frequency

        if not self.unit:
            self.unit = self.curve.unit

        self.timezone = _effective_timezone(self.curve, self.frequency, self.timezone)

    # ------------------------------------------------------------------
    # PreparedRequest materialization
    # ------------------------------------------------------------------

    def _url_path(self) -> str:
        url_path = f"{self.endpoint}/{URL.path_encode(self.curve.name, safe='')}/"

        if self.curve.is_instance and self.issued_at:
            safe_issued = URL.path_encode(self.issued_at.isoformat(" "), safe="")
            url_path += f"get/{safe_issued}/"

            if len(self.request_tags) == 1:
                safe_tag = URL.path_encode(self.request_tags[0].lower(), safe="")
                url_path += f"{safe_tag}/"

        return url_path

    def _merged_tags(self) -> dict[str, str]:
        merged: dict[str, str] = dict(self.curve.tags) if self.curve.tags else {}
        if self.tags:
            merged.update(self.tags)
        return merged

    def _cache_configs(self) -> tuple["CacheConfig | None", "CacheConfig | None"]:
        if self.client is None:
            return None, None

        now = dt.datetime.now(dt.timezone.utc)
        upsert = (
            self.event_type == EventType.CURVE_UPDATE
            or (self.end > now if self.end else False)
        )
        return self.client.cache_configs(curve=self.curve, upsert=upsert)

    def to_request(self) -> PreparedRequest:
        """Materialize this domain object as a :class:`PreparedRequest`.

        Builds the URL, headers, tags, and cache configs from the
        current domain state. Re-callable — useful after mutating
        ``begin/end/issued_at`` etc. via :meth:`copy`.
        """
        base_url = (
            URL.from_("https://app.energyquantified.com/api/")
            if self.client is None
            else self.client.base_url
        )
        url = (base_url / self._url_path()).with_query_items(self.parameters())

        local_cache, remote_cache = self._cache_configs()

        request = PreparedRequest(
            method="GET",
            url=url,
            headers=dict(REQUEST_HEADERS),
            tags=self._merged_tags(),
            buffer=None,
            sent_at=None,
            local_cache_config=local_cache,
            remote_cache_config=remote_cache,
        )
        if self.client is not None:
            request.attach_session(self.client)
        return request

    @classmethod
    def deduplicate(cls, requests: Iterable["CurveRequest"]) -> Iterator["PreparedRequest"]:
        keys = set()
        for request in requests:
            if not isinstance(request, PreparedRequest):
                request = request.to_request()

            key = request.public_hash
            if key not in keys:
                keys.add(key)
                yield request

    # ------------------------------------------------------------------
    # Copy
    # ------------------------------------------------------------------

    def copy(
        self,
        *,
        curve: Curve | None = ...,
        begin: dt.datetime | None = ...,
        end: dt.datetime | None = ...,
        issued_at: dt.datetime | None = ...,
        issued_at_earliest: dt.datetime | None = ...,
        issued_at_latest: dt.datetime | None = ...,
        request_tags: list[str] | str | None = ...,
        limit: int | None = ...,
        exclude_tags: list[str] | str | None = ...,
        ensembles: bool = ...,
        timezone: str | None = ...,
        unit: str | None = ...,
        frequency: str | None = ...,
        event_type: EventType | None = ...,
        raise_error: bool = ...,
        tags: dict[str, str] | None = ...,
        client: "APIClient" = ...,
        **kwargs: Any,
    ) -> "CurveRequest":
        return type(self)(
            curve=self.curve if curve is ... else curve,
            begin=self.begin if begin is ... else begin,
            end=self.end if end is ... else end,
            issued_at=self.issued_at if issued_at is ... else issued_at,
            issued_at_earliest=(
                self.issued_at_earliest if issued_at_earliest is ... else issued_at_earliest
            ),
            issued_at_latest=(
                self.issued_at_latest if issued_at_latest is ... else issued_at_latest
            ),
            request_tags=list(self.request_tags) if request_tags is ... else request_tags,
            limit=self.limit if limit is ... else limit,
            exclude_tags=list(self.exclude_tags) if exclude_tags is ... else exclude_tags,
            ensembles=self.ensembles if ensembles is ... else ensembles,
            timezone=self.timezone if timezone is ... else timezone,
            unit=self.unit if unit is ... else unit,
            frequency=self.frequency if frequency is ... else frequency,
            event_type=self.event_type if event_type is ... else event_type,
            raise_error=self.raise_error if raise_error is ... else raise_error,
            tags=dict(self.tags) if tags is ... else (tags or {}),
            client=self.client if client is ... else client,
        )

    # ------------------------------------------------------------------
    # Iteration / fan-out
    # ------------------------------------------------------------------

    @classmethod
    def iterate(
        cls,
        obj: Any,
        client: "APIClient",
        raise_error: bool = True,
        **options,
    ) -> Iterator["CurveRequest"]:
        if isinstance(obj, cls):
            obj.client = client
            yield obj
        elif isinstance(obj, _Event):
            if isinstance(obj, CurveUpdateEvent):
                instance: EQInstance | None = obj.instance

                if instance:
                    issued_at = instance.issued
                    request_tags = [instance.tag] if instance.tag else []
                else:
                    issued_at = None
                    request_tags = []

                for curve in client.metadata.curves(name=obj.curve.name):
                    yield cls(
                        curve=curve,
                        begin=obj.begin,
                        end=obj.end,
                        issued_at=issued_at,
                        issued_at_earliest=None,
                        issued_at_latest=None,
                        request_tags=request_tags,
                        event_type=obj.event_type,
                        client=client,
                        raise_error=raise_error,
                        **options,
                    )
        elif isinstance(obj, str):
            for curve in client.metadata.curves(name=obj):
                yield cls(curve=curve, client=client, raise_error=raise_error, **options)
        elif isinstance(obj, Curve):
            yield cls(curve=obj, client=client, raise_error=raise_error, **options)
        elif isinstance(obj, Iterable):
            for item in obj:
                yield from cls.iterate(item, client=client, raise_error=raise_error, **options)
        elif isinstance(obj, PreparedRequest):
            yield obj
        else:
            raise ValueError(f"Cannot iterate curve requests based on {type(obj)}")

    # ------------------------------------------------------------------
    # Properties / parameters
    # ------------------------------------------------------------------

    @property
    def endpoint(self):
        if self.curve.curve_type == CurveType.INSTANCE:
            return "ensembles" if self.ensembles else "instances"
        elif self.curve.curve_type == CurveType.INSTANCE_PERIOD:
            return "period-instances"
        elif self.curve.curve_type == CurveType.OHLC:
            return "ohlc"
        elif self.curve.curve_type == CurveType.PERIOD:
            return "periods"
        elif self.curve.curve_type in [
            CurveType.TIMESERIES,
            CurveType.SCENARIO_TIMESERIES,
        ]:
            return "timeseries"
        else:
            raise ValueError(f"Invalid curve type {self.curve.curve_type}")

    @property
    def curve_type(self):
        return self.curve.curve_type

    @property
    def fetch_interval(self) -> str:
        freq = self.frequency or "PT1H"
        return "P1M" if freq.startswith("PT") else "P1Y"

    def parameters(
        self,
        with_issued_range: bool = True,
        with_tags: bool = True,
        with_limit: bool = True,
    ):
        # When the URL path already encodes issued_at (and possibly the
        # single tag), drop the corresponding query params.
        if self.curve.is_instance and self.issued_at:
            with_issued_range = False
            with_limit = False
            if len(self.request_tags) == 1:
                with_tags = False

        params: dict[str, Any] = {}

        if self.curve_type in [CurveType.OHLC]:
            if self.begin:
                params["begin"] = self.begin.strftime("%Y-%m-%d")
            if self.end:
                params["end"] = self.end.strftime("%Y-%m-%d")
        else:
            if self.begin:
                params["begin"] = self.begin
            if self.end:
                params["end"] = self.end

        if self.unit:
            params["unit"] = self.unit
        if self.frequency:
            params["frequency"] = self.frequency
        if self.timezone:
            params["timezone"] = self.timezone

        if self.curve.is_instance or self.curve.is_period_instance:
            if with_issued_range and self.curve.is_instance:
                if self.issued_at_earliest:
                    params["issued-at-earliest"] = self.issued_at_earliest
                if self.issued_at_latest:
                    params["issued-at-latest"] = self.issued_at_latest
            if with_tags and self.request_tags:
                params["tags"] = self.request_tags
            if with_limit and self.limit:
                params["limit"] = self.limit

        return params

    # ------------------------------------------------------------------
    # http_requests fan-out — yields CurveRequest instances; callers
    # pipe through .to_request() to get PreparedRequests for send_many.
    # ------------------------------------------------------------------

    @classmethod
    def http_requests(
        cls,
        obj: Any,
        client: "APIClient",
        raise_error: bool = True,
        **options,
    ) -> Iterator[PreparedRequest]:
        yield from cls.deduplicate(
            cls._http_requests(
                obj=obj, client=client,
                raise_error=raise_error,
                **options
            )
        )

    @classmethod
    def _http_requests(
        cls,
        obj: Any,
        client: "APIClient",
        raise_error: bool = True,
        **options,
    ) -> Iterator["CurveRequest"]:
        for curve_request in cls.iterate(
            obj=obj, client=client,
            raise_error=raise_error,
            **options
        ):
            if isinstance(curve_request, PreparedRequest):
                yield curve_request
                continue

            for start, end in iter_datetime_ranges(
                curve_request.begin,
                curve_request.end,
                interval=curve_request.fetch_interval,
            ):
                refined: CurveRequest = curve_request.copy(
                    begin=start, end=end,
                    client=client,
                    raise_error=raise_error,
                )

                if refined.curve.is_instance or refined.curve.is_period_instance:
                    for instance in client.list_instances(
                        requests=curve_request,
                        raise_error=raise_error,
                        **options
                    ):
                        if curve_request.curve.is_instance:
                            yield refined.copy(
                                curve=instance.curve,
                                issued_at=instance.issued_at,
                                issued_at_earliest=None,
                                issued_at_latest=None,
                                request_tags=instance.tags,
                            )
                        else:
                            issued_at_earliest = truncate_datetime(
                                instance.issued_at,
                                tz=dt.timezone.utc,
                                interval=DEFAULT_ISSUE_INTERVAL,
                                add_interval=False,
                            )
                            yield refined.copy(
                                curve=instance.curve,
                                issued_at=None,
                                issued_at_earliest=issued_at_earliest,
                                issued_at_latest=issued_at_earliest
                                + DEFAULT_ISSUE_INTERVAL,
                                request_tags=instance.tags,
                            )
                else:
                    yield refined