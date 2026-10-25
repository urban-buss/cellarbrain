"""Optional, local OpenClaw notifications for completed Cellarbrain ETL runs."""

from __future__ import annotations

import fcntl
import json
import logging
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import Protocol, TextIO

from .settings import NotificationsConfig

logger = logging.getLogger(__name__)
_STATE_FILE = ".cellarbrain-notifications.json"
_MAX_SENT_KEYS = 200


class NotificationError(ValueError):
    """Raised when strict mode requires successful notification delivery."""


@dataclass(frozen=True)
class Notification:
    key: str
    message: str


@dataclass(frozen=True)
class SendResult:
    sent: bool
    error: str | None = None
    # True only when the sender can prove the request never reached dispatch.
    retryable: bool = False
    message_id: str | None = None


class NotificationSender(Protocol):
    def send(self, notification: Notification) -> SendResult: ...


class OpenClawSender:
    """Send plain text through OpenClaw's documented local CLI."""

    def __init__(self, config: NotificationsConfig) -> None:
        self.config = config

    def send(self, notification: Notification, *, verbose: bool = False) -> SendResult:
        if not self.config.target.strip():
            return SendResult(False, "notifications.target is empty", retryable=True)
        executable = shutil.which(self.config.command) or self.config.command
        if verbose:
            print(f"OpenClaw executable: {executable}")
        command = [
            executable,
            "message",
            "send",
            "--channel",
            "imessage",
            "--json",
            "--target",
            self.config.target,
            "--message",
            notification.message,
        ]
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=self.config.timeout_seconds,
                check=False,
                shell=False,
            )
        except subprocess.TimeoutExpired as exc:
            if verbose:
                _print_process_output("stdout", exc.stdout)
                _print_process_output("stderr", exc.stderr, file=sys.stderr)
                print(f"OpenClaw iMessage send timed out after {self.config.timeout_seconds} seconds.", file=sys.stderr)
            return SendResult(False, "TimeoutExpired")
        except OSError as exc:
            if verbose:
                print(f"OpenClaw iMessage send could not start: {exc}", file=sys.stderr)
            return SendResult(False, type(exc).__name__, retryable=True)
        if verbose:
            _print_process_output("stdout", result.stdout)
            _print_process_output("stderr", result.stderr, file=sys.stderr)
        if result.returncode != 0:
            error = f"OpenClaw exited with status {result.returncode}"
            if verbose:
                print(f"OpenClaw iMessage send failed: {error}", file=sys.stderr)
            return SendResult(False, error)
        if verbose:
            print("OpenClaw iMessage send succeeded.")
        return SendResult(True, message_id=_message_id(result.stdout))


def _message_id(output: str | bytes | None) -> str | None:
    """Extract a message identifier when OpenClaw's JSON result includes one."""
    if not output:
        return None
    if isinstance(output, bytes):
        output = output.decode(errors="replace")
    try:
        payload = json.loads(output)
    except (json.JSONDecodeError, TypeError):
        return None
    if isinstance(payload, dict):
        for key in ("messageId", "message_id"):
            value = payload.get(key)
            if isinstance(value, (str, int)):
                return str(value)
        for key in ("result", "data"):
            found = _message_id(json.dumps(payload.get(key))) if key in payload else None
            if found is not None:
                return found
    if isinstance(payload, list):
        for item in payload:
            found = _message_id(json.dumps(item))
            if found is not None:
                return found
    return None


def _print_process_output(label: str, output: str | bytes | None, *, file: TextIO | None = None) -> None:
    if not output:
        return
    if isinstance(output, bytes):
        output = output.decode(errors="replace")
    output_stream = sys.stdout if file is None else file
    print(f"OpenClaw {label}:", file=output_stream)
    print(output, end="" if output.endswith("\n") else "\n", file=output_stream)


def summarize_changes(run_id: int, change_rows: list[dict]) -> str:
    """Build a compact message grouped by entity and actual change action."""
    rows = [row for row in change_rows if row.get("run_id") == run_id]
    labels = {"insert": "added", "update": "updated", "delete": "removed", "rename": "renamed"}
    wine_counts: dict[str, int] = {}
    bottle_counts: dict[str, int] = {}
    other_counts: dict[tuple[str, str], int] = {}
    for row in rows:
        entity = row.get("entity_type")
        change_type = row.get("change_type")
        if not isinstance(change_type, str):
            continue
        if entity == "wine":
            wine_counts[change_type] = wine_counts.get(change_type, 0) + 1
        elif entity == "bottle":
            bottle_counts[change_type] = bottle_counts.get(change_type, 0) + 1
        else:
            key = (str(entity or "other"), change_type)
            other_counts[key] = other_counts.get(key, 0) + 1

    lines = [f"Cellarbrain ETL completed (run {run_id})."]

    def _actions(counts: dict[str, int]) -> str:
        order = ("insert", "update", "delete", "rename")
        known = [f"{counts[kind]} {labels[kind]}" for kind in order if counts.get(kind)]
        known.extend(f"{count} {kind}" for kind, count in sorted(counts.items()) if kind not in labels)
        return ", ".join(known)

    if wine_counts:
        lines.append(f"Wines: {_actions(wine_counts)}")
    if bottle_counts:
        lines.append(f"Bottles: {_actions(bottle_counts)}")
    if other_counts:
        entity_names = {
            "winery": ("winery", "wineries"),
            "appellation": ("appellation", "appellations"),
            "grape": ("grape", "grapes"),
            "cellar": ("cellar", "cellars"),
            "provider": ("provider", "providers"),
            "tracked_wine": ("tracked wine", "tracked wines"),
            "wine_grape": ("wine/grape link", "wine/grape links"),
            "tasting": ("tasting", "tastings"),
            "pro_rating": ("professional rating", "professional ratings"),
        }
        other_parts = []
        for (entity, action), count in sorted(other_counts.items()):
            singular, plural = entity_names.get(entity, (entity.replace("_", " "), entity.replace("_", " ") + "s"))
            other_parts.append(f"{count} {singular if count == 1 else plural} {labels.get(action, action)}")
        lines.append("Other: " + ", ".join(other_parts))
    if not wine_counts and not bottle_counts and not other_counts:
        lines.append("No data changes detected.")
    return "\n".join(lines)


def _load_document(path: pathlib.Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(value, list):  # legacy notifications state format
            return {"sent": [item for item in value if isinstance(item, str)], "deliveries": {}}
        if isinstance(value, dict):
            deliveries = {}
            for field in ("deliveries", "report_deliveries", "notification_deliveries"):
                records = value.get(field, {})
                if isinstance(records, dict):
                    deliveries.update(records)
            return {
                "sent": [item for item in value.get("sent", []) if isinstance(item, str)],
                "deliveries": deliveries,
            }
    except (OSError, json.JSONDecodeError):
        pass
    return {"sent": [], "deliveries": {}}


def _load_state(path: pathlib.Path) -> list[str]:
    document = _load_document(path)
    sent = list(document["sent"])
    for key, value in document["deliveries"].items():
        if isinstance(value, dict) and value.get("state") == "sent" and key not in sent:
            sent.append(key)
    return sent


def _write_document(path: pathlib.Path, document: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f"{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(document, stream, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _state_lock(path: pathlib.Path):
    lock = path.with_suffix(path.suffix + ".lock")
    lock.parent.mkdir(parents=True, exist_ok=True)
    return lock.open("a")


def _deliver_once(
    config: NotificationsConfig,
    output_dir: str | pathlib.Path,
    notification: Notification,
    *,
    sender: NotificationSender | None = None,
) -> SendResult:
    """Durably dispatch a notification key at most once unless reconciled."""
    if not config.enabled:
        return SendResult(False, "disabled")
    state_path = pathlib.Path(output_dir) / _STATE_FILE
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with _state_lock(state_path) as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        document = _load_document(state_path)
        record = document["deliveries"].get(notification.key)
        if notification.key in document["sent"] or (isinstance(record, dict) and record.get("state") == "sent"):
            logger.info("Notification delivery key=%s state=sent", notification.key)
            return SendResult(True, message_id=record.get("message_id") if isinstance(record, dict) else None)
        if isinstance(record, dict) and record.get("state") == "in_flight":
            # We hold the file lock, so no other process can still be dispatching.
            # A persisted in_flight record here belongs to a process that exited
            # before it could record the outcome.
            document["deliveries"][notification.key] = {"state": "uncertain", "updated_at": time.time()}
            _write_document(state_path, document)
            logger.info("Notification delivery key=%s state=uncertain; recovered stale in_flight", notification.key)
            return SendResult(False, "uncertain")
        if isinstance(record, dict) and record.get("state") == "uncertain":
            state = record["state"]
            logger.info("Notification delivery key=%s state=%s; suppressing dispatch", notification.key, state)
            return SendResult(False, state)

        document["deliveries"][notification.key] = {"state": "in_flight", "updated_at": time.time()}
        _write_document(state_path, document)
        logger.info("Notification delivery key=%s state=in_flight", notification.key)
        try:
            result = (sender or OpenClawSender(config)).send(notification)
        except Exception as exc:
            result = SendResult(False, type(exc).__name__)

        if result.sent:
            state = {"state": "sent", "updated_at": time.time()}
            message_id = getattr(result, "message_id", None)
            if message_id is not None:
                state["message_id"] = message_id
            document["deliveries"][notification.key] = state
            document["sent"] = [key for key in document["sent"] if key != notification.key]
            document["sent"] = (document["sent"] + [notification.key])[-_MAX_SENT_KEYS:]
            try:
                _write_document(state_path, document)
            except OSError as exc:
                # The durable in_flight record still suppresses another dispatch.
                logger.warning(
                    "Notification delivery key=%s state=in_flight; sent state update failed (%s)",
                    notification.key,
                    type(exc).__name__,
                )
            else:
                logger.info("Notification delivery key=%s state=sent", notification.key)
            return result
        if getattr(result, "retryable", False):
            document["deliveries"][notification.key] = {"state": "retryable", "updated_at": time.time()}
            _write_document(state_path, document)
            logger.info("Notification delivery key=%s state=retryable", notification.key)
            return result
        document["deliveries"][notification.key] = {"state": "uncertain", "updated_at": time.time()}
        _write_document(state_path, document)
        logger.info("Notification delivery key=%s state=uncertain", notification.key)
        return result


def deliver_report_once(
    config: NotificationsConfig,
    output_dir: str | pathlib.Path,
    notification: Notification,
    *,
    sender: NotificationSender | None = None,
) -> SendResult:
    """Durably dispatch a report key at most once unless reconciled."""
    return _deliver_once(config, output_dir, notification, sender=sender)


def reconcile_report_delivery(output_dir: str | pathlib.Path, key: str, action: str) -> None:
    """Mark an uncertain notification sent, or clear it after confirming non-delivery."""
    if action not in {"delivered", "retry"}:
        raise ValueError("action must be delivered or retry")
    state_path = pathlib.Path(output_dir) / _STATE_FILE
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with _state_lock(state_path) as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        document = _load_document(state_path)
        if action == "delivered":
            document["deliveries"][key] = {"state": "sent", "updated_at": time.time(), "reconciled": True}
            document["sent"] = [item for item in document["sent"] if item != key]
            document["sent"] = (document["sent"] + [key])[-_MAX_SENT_KEYS:]
            state = "sent"
        else:
            document["deliveries"].pop(key, None)
            document["sent"] = [item for item in document["sent"] if item != key]
            state = "retry"
        _write_document(state_path, document)
    logger.info("Notification delivery key=%s state=%s (operator reconciliation)", key, state)


def deliver(
    config: NotificationsConfig,
    output_dir: str | pathlib.Path,
    notification: Notification,
    *,
    sender: NotificationSender | None = None,
) -> SendResult:
    """Deliver once per stable key with durable protection from ambiguous retries."""
    return _deliver_once(config, output_dir, notification, sender=sender)


def notify_success(
    config: NotificationsConfig,
    output_dir: str | pathlib.Path,
    run_id: int,
    change_rows: list[dict],
) -> None:
    if not config.enabled:
        return
    result = deliver(
        config,
        output_dir,
        Notification(f"success:{run_id}", summarize_changes(run_id, change_rows)),
    )
    if result.sent:
        return
    message = f"Could not send OpenClaw ETL notification ({result.error})."
    if config.strict:
        raise NotificationError(message)
    logger.warning(message)


def notify_failure(config: NotificationsConfig, output_dir: str | pathlib.Path, run_id: int | None = None) -> None:
    """Best effort failure alert; intentionally never raises into ETL handling."""
    if not config.enabled:
        return
    key = f"failure:{run_id}" if run_id is not None else f"failure:unknown:{time.time_ns()}"
    notification = Notification(key, "Cellarbrain ETL failed. Check the local Cellarbrain logs for details.")
    try:
        result = deliver(config, output_dir, notification)
        if not result.sent:
            logger.warning("Could not send OpenClaw ETL failure notification (%s).", result.error)
    except Exception:
        logger.exception("Could not send OpenClaw ETL failure notification")
