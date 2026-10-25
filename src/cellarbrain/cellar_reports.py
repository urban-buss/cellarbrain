"""Scheduled weekly and monthly cellar reports backed by ETL inventory history."""

from __future__ import annotations

import calendar
import datetime as dt
import fcntl
import json
import logging
import os
import pathlib
import tempfile
import time
from zoneinfo import ZoneInfo

from .notifications import Notification, NotificationSender, SendResult, deliver_report_once
from .settings import NotificationsConfig, ReportsConfig

logger = logging.getLogger(__name__)
HISTORY_FILE = ".cellar-inventory-history.json"


def record_snapshot(data_dir: str | pathlib.Path, bottles: list[dict], observed_at: dt.datetime) -> None:
    """Persist a full inventory snapshot after a successful ETL run."""
    path = pathlib.Path(data_dir) / HISTORY_FILE
    stamp = observed_at.replace(tzinfo=dt.UTC) if observed_at.tzinfo is None else observed_at.astimezone(dt.UTC)
    entries = []
    for bottle in bottles:
        if bottle.get("status") != "stored":
            continue
        entries.append(
            {
                "id": str(bottle.get("bottle_id")),
                "segment": "on_order" if bottle.get("is_in_transit") else "stored",
                "value_chf": float(bottle.get("purchase_price", bottle.get("price")) or 0),
            }
        )
    snapshots = _read_history(path)
    if snapshots and snapshots[-1].get("observed_at") == stamp.isoformat():
        snapshots[-1] = {"observed_at": stamp.isoformat(), "bottles": entries}
    else:
        snapshots.append({"observed_at": stamp.isoformat(), "bottles": entries})
    _atomic_json(path, snapshots)


def _read_history(path: pathlib.Path) -> list[dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except (OSError, json.JSONDecodeError):
        return []


def _atomic_json(path: pathlib.Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=path.name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def period_bounds(kind: str, local_date: dt.date) -> tuple[dt.date, dt.date]:
    if kind == "monthly":
        end = local_date.replace(day=calendar.monthrange(local_date.year, local_date.month)[1])
        return end.replace(day=1), end
    if kind == "weekly":
        end = local_date - dt.timedelta(days=(local_date.weekday() + 1) % 7)
        return end - dt.timedelta(days=6), end
    raise ValueError("kind must be weekly or monthly")


def render_report(
    kind: str,
    data_dir: str | pathlib.Path,
    now: dt.datetime | None = None,
    period_end: dt.date | None = None,
    timezone: str = "Europe/Zurich",
) -> str:
    local_now = (now or dt.datetime.now(ZoneInfo(timezone))).astimezone(ZoneInfo(timezone))
    end = period_end or period_bounds(kind, local_now.date())[1]
    start = end.replace(day=1) if kind == "monthly" else end - dt.timedelta(days=6)
    snapshots = _read_history(pathlib.Path(data_dir) / HISTORY_FILE)
    parsed = [(dt.datetime.fromisoformat(s["observed_at"]), s["bottles"]) for s in snapshots if s.get("observed_at")]
    parsed.sort(key=lambda pair: pair[0])
    previous: list[dict] = next((rows for stamp, rows in reversed(parsed) if stamp.date() < start), [])
    latest: list[dict] = next((rows for stamp, rows in reversed(parsed) if stamp.date() <= end), [])
    changes = {"stored": {"in": [0, 0.0], "out": [0, 0.0]}, "on_order": {"in": [0, 0.0], "out": [0, 0.0]}}
    # Use adjacent snapshots within the period. Classification changes are a paired
    # outflow/inflow, and newly observed/deleted bottles are additions/removals.
    period_snaps = [(stamp, rows) for stamp, rows in parsed if start <= stamp.date() <= end]
    prior_rows = previous if previous else (period_snaps[0][1] if period_snaps else [])
    for _, rows in period_snaps:
        old = {b["id"]: b for b in prior_rows}
        new = {b["id"]: b for b in rows}
        for bottle_id in old.keys() | new.keys():
            before, after = old.get(bottle_id), new.get(bottle_id)
            if before and after and before["segment"] == after["segment"]:
                continue
            if before:
                v = changes[before["segment"]]["out"]
                v[0] += 1
                v[1] += before["value_chf"]
            if after:
                v = changes[after["segment"]]["in"]
                v[0] += 1
                v[1] += after["value_chf"]
        prior_rows = rows
    snapshot = {segment: [0, 0.0] for segment in ("stored", "on_order")}
    for bottle in latest:
        snapshot[bottle["segment"]][0] += 1
        snapshot[bottle["segment"]][1] += bottle["value_chf"]
    last_refresh = max((stamp for stamp, _ in parsed), default=None)
    if kind == "weekly":
        lines = [f"Cellar weekly report ({start:%d %b}–{end:%d %b %Y})"]
        lines.append(f"Stored: {snapshot['stored'][0]} bottles · CHF {snapshot['stored'][1]:,.2f}")
        lines.append(f"On order: {snapshot['on_order'][0]} bottles · CHF {snapshot['on_order'][1]:,.2f}")
        lines.append(
            f"Last successful ETL refresh: {last_refresh.astimezone(ZoneInfo(timezone)):%Y-%m-%d %H:%M %Z}"
            if last_refresh
            else "Last successful ETL refresh: unknown"
        )
    else:
        lines = [f"Cellar monthly report ({start:%B %Y})"]
        for segment in ("stored", "on_order"):
            lines.append(
                f"{segment.replace('_', ' ').title()} at period end: {snapshot[segment][0]} bottles · CHF {snapshot[segment][1]:,.2f}"
            )
            for direction in ("in", "out"):
                count, value = changes[segment][direction]
                lines.append(f"  {direction}: {count} bottles · CHF {value:,.2f}")
    return "\n".join(lines)


def deliver_report(
    kind: str,
    data_dir: str | pathlib.Path,
    notifications: NotificationsConfig,
    reports: ReportsConfig,
    *,
    sender: NotificationSender | None = None,
    now: dt.datetime | None = None,
    period_end: dt.date | None = None,
) -> SendResult:
    enabled = reports.weekly_enabled if kind == "weekly" else reports.monthly_enabled
    if not enabled:
        return SendResult(False, "disabled")
    local_now = (now or dt.datetime.now(ZoneInfo(reports.timezone))).astimezone(ZoneInfo(reports.timezone))
    end = period_end or period_bounds(kind, local_now.date())[1]
    message = render_report(kind, data_dir, local_now, period_end=end, timezone=reports.timezone)
    root = pathlib.Path(data_dir)
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".cellar-report-delivery.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        return deliver_report_once(
            notifications,
            root,
            Notification(f"cellar-{kind}:{end.isoformat()}", message),
            sender=sender,
        )


def due_reports(now: dt.datetime, config: ReportsConfig) -> list[str]:
    local = now.astimezone(ZoneInfo(config.timezone))
    hour, minute = map(int, config.send_time.split(":"))
    if (local.hour, local.minute) < (hour, minute):
        return []
    due = []
    # If the process was down at the scheduled instant, deliver the most
    # recent completed period after restart. Delivery keys make this safe.
    if config.weekly_enabled and local.weekday() <= 6:
        due.append("weekly")
    if config.monthly_enabled:
        due.append("monthly")
    return due


def scheduler(
    data_dir: str | pathlib.Path,
    notifications: NotificationsConfig,
    reports: ReportsConfig,
    *,
    sender: NotificationSender | None = None,
    poll_seconds: float = 30,
) -> None:
    """Run the local scheduler continuously; delivery keys make restarts idempotent."""
    lock_path = pathlib.Path(data_dir) / ".cellar-reports-scheduler.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("A cellar report scheduler is already running for this data directory") from exc
        logger.info("Cellar report scheduler active")
        while True:
            now = dt.datetime.now(ZoneInfo(reports.timezone))
            kinds = due_reports(now, reports)
            for kind in kinds:
                try:
                    end = period_bounds(kind, now.date())[1]
                    if kind == "monthly" and now.date().day != calendar.monthrange(now.year, now.month)[1]:
                        previous_month = now.date().replace(day=1) - dt.timedelta(days=1)
                        end = previous_month
                    deliver_report(kind, data_dir, notifications, reports, sender=sender, now=now, period_end=end)
                except Exception:
                    logger.exception("Scheduled %s cellar report failed", kind)
            time.sleep(poll_seconds)
