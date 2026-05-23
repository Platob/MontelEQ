"""EventsClient – real-time curve-update event streaming."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import queue
import sys
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Iterator, Optional, TYPE_CHECKING

from energyquantified.events import CurveAttributeFilter, CurveUpdateEvent, EventType
from energyquantified.metadata import CurveType, DataType

from monteleq.model import Curve
from monteleq.api.request import CurveRequest

if TYPE_CHECKING:
    from monteleq.api.client import APIClient

__all__ = ["EventsClient"]

logger = logging.getLogger(__name__)

# Sentinel pushed onto the queue by the reader thread when its iterator ends
# (e.g. on disconnect or exception). Distinct from any real event.
_READER_DONE = object()


class _IdleTimeout(Exception):
    """Raised internally to force the reconnect path when no events have
    arrived from upstream within ``idle_reconnect_seconds``."""


class EventsClient:
    def __init__(self, api: "APIClient") -> None:
        self._api = api

    # ------------------------------------------------------------------ #
    # Connection
    # ------------------------------------------------------------------ #

    def _connect_and_subscribe(
        self,
        filters: CurveAttributeFilter,
        last_id: Optional[str] = None,
    ):
        logger.info(
            "Connecting to EQ event stream: filters=%s last_id=%s",
            filters,
            last_id,
        )
        client = self._api.new_eqclient()
        client.events.connect()
        if last_id is not None:
            client.events.subscribe_curve_events(filters=filters, last_id=last_id)
        else:
            client.events.subscribe_curve_events(filters=filters)
        logger.info("Connected and subscribed to EQ event stream")
        return client

    @staticmethod
    def _safe_disconnect(client) -> None:
        if client is None:
            return
        try:
            client.events.disconnect()
        except Exception:
            logger.exception("Failed to disconnect EQ event client cleanly")

    # ------------------------------------------------------------------ #
    # Checkpoint
    # ------------------------------------------------------------------ #

    @staticmethod
    def _load_checkpoint(path: Optional[Path]) -> Optional[str]:
        if path is None or not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
            last_id = data.get("last_id")
            if last_id is not None:
                logger.info("Resuming from checkpoint %s: last_id=%s", path, last_id)
            return last_id
        except (OSError, ValueError):
            logger.exception("Failed to read checkpoint %s; starting fresh", path)
            return None

    @staticmethod
    def _save_checkpoint(path: Optional[Path], last_id: Optional[str]) -> None:
        if path is None or last_id is None:
            return
        payload = json.dumps(
            {"last_id": last_id, "saved_at": dt.datetime.utcnow().isoformat()}
        )
        tmp = path.with_suffix(path.suffix + ".tmp")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(payload)
            os.replace(tmp, path)  # atomic on POSIX and Windows
            logger.debug("Checkpoint written: last_id=%s", last_id)
        except OSError:
            logger.exception("Failed to write checkpoint %s", path)
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass

    # ------------------------------------------------------------------ #
    # Filter resolution
    # ------------------------------------------------------------------ #

    def _resolve_curve_type_filter(
        self,
        curve_type: Optional[str | CurveType | list[str | CurveType]],
    ) -> Optional[set[CurveType]]:
        if curve_type is None:
            return None
        items = curve_type if isinstance(curve_type, (list, set, tuple)) else [curve_type]
        resolved = {
            ct if isinstance(ct, CurveType) else CurveType[ct]
            for ct in items
        }
        logger.info("curve_type_filter=%s", sorted(ct.name for ct in resolved))
        return resolved

    def _derive_data_types(
        self, curve_type_filter: set[CurveType]
    ) -> Optional[list[DataType]]:
        derived = list({
            c.data_type
            for c in self._api.metadata.curvemap.values()
            if c.curve_type in curve_type_filter
        }) or None
        logger.info("Derived data_types from curve_type_filter: %s", derived)
        return derived

    def _event_matches(
        self,
        event: CurveUpdateEvent,
        curve_type_filter: Optional[set[CurveType]],
    ) -> bool:
        if curve_type_filter is None:
            return True
        info = self._api.metadata.curvemap.get(event.curve.name)
        return info is not None and info.curve_type in curve_type_filter

    # ------------------------------------------------------------------ #
    # Reader thread
    # ------------------------------------------------------------------ #

    @staticmethod
    def _reader_loop(client, q: queue.Queue, stop: threading.Event) -> None:
        """Pump events from the blocking SDK iterator into a queue.

        Runs on a background thread so the main loop can wake up on a
        wall-clock timer even when no events are arriving.
        """
        try:
            for event in client.events.get_next():
                if stop.is_set():
                    break
                q.put(event)
                if event.event_type == EventType.DISCONNECTED:
                    break
        except Exception as exc:
            logger.exception("Reader thread crashed")
            q.put(exc)
        finally:
            q.put(_READER_DONE)

    # ------------------------------------------------------------------ #
    # Progress display
    # ------------------------------------------------------------------ #

    @staticmethod
    def _render_progress(
        batch_len: int,
        batch_size: Optional[int],
        elapsed: float,
        max_seconds: Optional[float],
        total_yielded: int,
        last_event_curve: Optional[str],
        enabled: bool,
    ) -> None:
        if not enabled:
            return
        size_part = (
            f"{batch_len}/{batch_size}" if batch_size is not None else f"{batch_len}"
        )
        if max_seconds is not None:
            remaining = max(0.0, max_seconds - elapsed)
            mm, ss = divmod(int(remaining), 60)
            time_part = f"{mm:02d}:{ss:02d} until flush"
        else:
            mm, ss = divmod(int(elapsed), 60)
            time_part = f"{mm:02d}:{ss:02d} elapsed"
        curve_part = f" last={last_event_curve}" if last_event_curve else ""
        line = (
            f"\r[events] batch={size_part}  {time_part}  "
            f"yielded={total_yielded}{curve_part}"
        )
        # \033[K clears from cursor to end of line — handles shrinking output
        sys.stdout.write(line + "\033[K")
        sys.stdout.flush()

    @staticmethod
    def _clear_progress(enabled: bool) -> None:
        if not enabled:
            return
        sys.stdout.write("\r\033[K")
        sys.stdout.flush()

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def stream(
        self,
        *,
        curves: Optional[Curve] = None,
        begin: Optional[dt.datetime | dt.date | str] = None,
        end: Optional[dt.datetime | dt.date | str] = None,
        event_types: Optional[list[EventType]] = None,
        tags: Optional[list[str]] = None,
        exclude_tags: Optional[list[str]] = None,
        areas: Optional[list[str]] = None,
        data_types: Optional[list[DataType]] = None,
        commodities: Optional[list[str]] = None,
        categories: Optional[list[str]] = None,
        exact_categories: Optional[list[str]] = None,
        curve_type: Optional[str | CurveType | list[str | CurveType]] = None,
        batch_size: Optional[int] = 1,
        max_batch_seconds: Optional[float] = 300.0,
        reconnect: bool = True,
        reconnect_backoff: float = 1.0,
        max_reconnect_backoff: float = 30.0,
        idle_reconnect_seconds: Optional[float] = 60.0,
        checkpoint_path: Optional[str | os.PathLike] = None,
        dedup_queue_size: int = 512,
        progress: bool = True,
        progress_interval: float = 1.0,
    ) -> Iterator[list[CurveUpdateEvent]]:
        """Yield batches of CurveUpdateEvents.

        A batch is flushed when either:
          * it reaches ``batch_size`` events, or
          * ``max_batch_seconds`` of wall-clock time has elapsed since the
            first event in the current batch was received — even if no new
            events have arrived in the meantime.

        Events are pulled on a background thread so the timer is honoured
        regardless of the upstream pace.

        If ``idle_reconnect_seconds`` is set (default 60s) and no item is
        received from upstream for that long, the reader is torn down and
        the stream reconnects via the normal backoff path. Set to ``None``
        to disable the watchdog. Any item from the reader — event,
        DISCONNECTED, or even one filtered out by ``curve_type`` — counts
        as activity and resets the timer.

        If ``checkpoint_path`` is given, the last successfully-yielded event
        id is persisted there atomically after every flush, and the stream
        resumes from that id on subsequent calls (or after a reconnect).

        Duplicate events (compared by ``str(event)``) are suppressed using a
        bounded FIFO of the most recent ``dedup_queue_size`` event strings.
        Set ``dedup_queue_size`` to 0 to disable deduplication.

        If ``progress`` is True, a single self-clearing status line is
        written to stdout roughly every ``progress_interval`` seconds.
        """
        if curves:
            curve_ids = {c.id for c in curves}
            curve_type = {c.curve_type for c in curves}
        else:
            curve_ids = {c.id for c in self._api.metadata.curvemap.values()}

        curve_type_filter = self._resolve_curve_type_filter(curve_type)
        if curve_type_filter is not None and data_types is None:
            data_types = self._derive_data_types(curve_type_filter)

        filters = CurveAttributeFilter(
            begin=begin,
            end=end,
            event_types=event_types,
            tags=tags,
            exclude_tags=exclude_tags,
            areas=areas,
            data_types=data_types,
            commodities=commodities,
            categories=categories,
            exact_categories=exact_categories,
        )

        cp_path = Path(checkpoint_path) if checkpoint_path is not None else None
        last_id = self._load_checkpoint(cp_path)

        batch: list[CurveUpdateEvent] = []
        batch_started_at: Optional[float] = None
        backoff = reconnect_backoff
        client = None
        reader: Optional[threading.Thread] = None
        stop_reader = threading.Event()
        total_yielded = 0
        last_event_curve: Optional[str] = None
        last_progress_at = 0.0

        # Bounded FIFO of recently-seen event strings for deduplication.
        seen: "OrderedDict[str, None]" = OrderedDict()

        def is_duplicate(event) -> bool:
            if dedup_queue_size <= 0:
                return False
            key = str(event)
            if key in seen:
                seen.move_to_end(key)
                return True
            seen[key] = None
            while len(seen) > dedup_queue_size:
                seen.popitem(last=False)
            return False

        def remaining_until_flush() -> Optional[float]:
            if max_batch_seconds is None or batch_started_at is None:
                return None
            return max_batch_seconds - (time.monotonic() - batch_started_at)

        def event_id_of(event) -> Optional[str]:
            return getattr(event, "event_id", None) or getattr(event, "id", None)

        def stop_reader_thread() -> None:
            nonlocal reader
            stop_reader.set()
            if reader is not None and reader.is_alive():
                # Daemon-style: don't block shutdown forever; the SDK
                # iterator may not honour our stop flag until its next
                # network read returns.
                reader.join(timeout=2.0)
            reader = None
            stop_reader.clear()

        try:
            while True:
                q: queue.Queue = queue.Queue(maxsize=10_000)
                try:
                    client = self._connect_and_subscribe(filters, last_id=last_id)
                    backoff = reconnect_backoff  # reset after a successful connect

                    reader = threading.Thread(
                        target=self._reader_loop,
                        args=(client, q, stop_reader),
                        name="eq-events-reader",
                        daemon=True,
                    )
                    reader.start()

                    # Idle watchdog: any item from the reader resets this.
                    last_activity_at = time.monotonic()

                    reader_done = False
                    while not reader_done:
                        # Decide how long we can afford to block on the queue.
                        # Cap at progress_interval so the status line ticks
                        # smoothly; cap at remaining-until-flush so the timer
                        # fires on time even with zero events; cap at the
                        # remaining idle budget so the watchdog fires on time.
                        timeouts = [progress_interval]
                        rem = remaining_until_flush()
                        if rem is not None:
                            timeouts.append(max(0.0, rem))
                        if idle_reconnect_seconds is not None:
                            idle_rem = idle_reconnect_seconds - (
                                time.monotonic() - last_activity_at
                            )
                            timeouts.append(max(0.0, idle_rem))
                        wait = min(timeouts)

                        try:
                            item = q.get(timeout=wait)
                            got_item = True
                        except queue.Empty:
                            item = None
                            got_item = False

                        now = time.monotonic()

                        # Any item from the reader proves the pipe is alive,
                        # even if it's a sentinel, an exception, a filtered-
                        # out event, or a DISCONNECTED notice.
                        if got_item:
                            last_activity_at = now

                        # ---- handle whatever we pulled (if anything) ----
                        if item is _READER_DONE:
                            reader_done = True
                        elif isinstance(item, BaseException):
                            raise item
                        elif item is not None:
                            event = item
                            logger.debug(
                                "Received event: type=%s curve=%s",
                                getattr(event.event_type, "name", event.event_type),
                                getattr(getattr(event, "curve", None), "name", None),
                            )

                            if event.event_type == EventType.DISCONNECTED:
                                logger.warning("EQ event stream disconnected")
                                reader_done = True
                            elif event.event_type == EventType.CURVE_UPDATE \
                                    and self._event_matches(event, curve_type_filter) \
                                    and not is_duplicate(event):
                                update_event: CurveUpdateEvent = event

                                try:
                                    curve = self._api.metadata.curvemap[update_event.curve.name]
                                    if curve.id in curve_ids:
                                        if not batch:
                                            batch_started_at = now
                                        batch.append(update_event)
                                        last_event_curve = curve.name
                                except KeyError:
                                    logger.warning(
                                        f"Curve {update_event.curve.name!r} not found"
                                    )

                        # ---- idle watchdog ----
                        if (
                            not reader_done
                            and idle_reconnect_seconds is not None
                            and (now - last_activity_at) >= idle_reconnect_seconds
                        ):
                            self._clear_progress(progress)
                            logger.warning(
                                "No events from upstream for %.1fs "
                                "(threshold=%.1fs); forcing reconnect",
                                now - last_activity_at,
                                idle_reconnect_seconds,
                            )
                            raise _IdleTimeout

                        # ---- check flush conditions every wakeup ----
                        size_hit = (
                            batch_size is not None and len(batch) >= batch_size
                        )
                        rem_after = remaining_until_flush()
                        time_hit = rem_after is not None and rem_after <= 0

                        if batch and (size_hit or time_hit):
                            reason = "size" if size_hit else "time"
                            self._clear_progress(progress)
                            logger.info(
                                "Flushing %d event(s) (reason=%s)", len(batch), reason
                            )
                            to_yield = batch
                            new_last_id = event_id_of(to_yield[-1])
                            batch, batch_started_at = [], None
                            yield to_yield
                            total_yielded += len(to_yield)
                            if new_last_id is not None:
                                last_id = new_last_id
                                self._save_checkpoint(cp_path, last_id)

                        # ---- progress line ----
                        if progress and (now - last_progress_at) >= progress_interval:
                            elapsed = (
                                now - batch_started_at
                                if batch_started_at is not None
                                else 0.0
                            )
                            self._render_progress(
                                batch_len=len(batch),
                                batch_size=batch_size,
                                elapsed=elapsed,
                                max_seconds=max_batch_seconds,
                                total_yielded=total_yielded,
                                last_event_curve=last_event_curve,
                                enabled=progress,
                            )
                            last_progress_at = now

                    # Reader stopped (disconnect / iterator end). Flush
                    # whatever we have before reconnecting.
                    if batch:
                        self._clear_progress(progress)
                        logger.info(
                            "Flushing %d event(s) (reason=disconnect)", len(batch)
                        )
                        to_yield = batch
                        new_last_id = event_id_of(to_yield[-1])
                        batch, batch_started_at = [], None
                        yield to_yield
                        total_yielded += len(to_yield)
                        if new_last_id is not None:
                            last_id = new_last_id
                            self._save_checkpoint(cp_path, last_id)

                except _IdleTimeout:
                    # Watchdog fired. Always reconnect on idle, regardless of
                    # the ``reconnect`` flag — a wedged stream is not a clean
                    # exit condition.
                    pass
                except Exception:
                    self._clear_progress(progress)
                    logger.exception("Error while consuming EQ event stream")
                    if not reconnect:
                        raise

                stop_reader_thread()

                if not reconnect:
                    return

                self._safe_disconnect(client)
                client = None

                self._clear_progress(progress)
                logger.warning("Reconnecting to EQ event stream in %.1fs", backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, max_reconnect_backoff)

        finally:
            stop_reader_thread()
            self._safe_disconnect(client)
            self._clear_progress(progress)
            if batch:
                logger.info("Final residual batch: %d event(s)", len(batch))
                to_yield = batch
                new_last_id = event_id_of(to_yield[-1])
                yield to_yield
                if new_last_id is not None:
                    self._save_checkpoint(cp_path, new_last_id)

    def requests(
        self,
        *,
        raise_error: bool = True,
        curves: Optional[list[Curve]] = None,
        begin: Optional[dt.datetime | dt.date | str] = None,
        end: Optional[dt.datetime | dt.date | str] = None,
        event_types: Optional[list[EventType]] = None,
        tags: Optional[list[str]] = None,
        exclude_tags: Optional[list[str]] = None,
        areas: Optional[list[str]] = None,
        data_types: Optional[list[DataType]] = None,
        commodities: Optional[list[str]] = None,
        categories: Optional[list[str]] = None,
        exact_categories: Optional[list[str]] = None,
        curve_type: Optional[str | CurveType | list[str | CurveType]] = None,
        batch_size: Optional[int] = 1,
        max_batch_seconds: Optional[float] = 300.0,
        reconnect: bool = True,
        reconnect_backoff: float = 1.0,
        max_reconnect_backoff: float = 30.0,
        idle_reconnect_seconds: Optional[float] = 60.0,
        checkpoint_path: Optional[str | os.PathLike] = None,
        dedup_queue_size: int = 512,
        progress: bool = True,
        progress_interval: float = 1.0,
        **kwargs,
    ):
        """Convenience wrapper around :meth:`stream` that yields CurveRequest
        objects instead of raw events, and skips events that fail to parse.
        """
        for batch in self.stream(
            curves=curves,
            begin=begin,
            end=end,
            event_types=event_types,
            tags=tags,
            exclude_tags=exclude_tags,
            areas=areas,
            data_types=data_types,
            commodities=commodities,
            categories=categories,
            exact_categories=exact_categories,
            curve_type=curve_type,
            batch_size=batch_size,
            max_batch_seconds=max_batch_seconds,
            reconnect=reconnect,
            reconnect_backoff=reconnect_backoff,
            max_reconnect_backoff=max_reconnect_backoff,
            idle_reconnect_seconds=idle_reconnect_seconds,
            checkpoint_path=checkpoint_path,
            dedup_queue_size=dedup_queue_size,
            progress=progress,
            progress_interval=progress_interval,
        ):
            yield from CurveRequest.http_requests(
                batch,
                client=self._api,
                raise_error=raise_error,
                **kwargs,
            )