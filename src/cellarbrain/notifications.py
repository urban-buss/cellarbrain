"""Optional, local OpenClaw notifications for completed Cellarbrain ETL runs."""

from __future__ import annotations

import json
import logging
import os
import pathlib
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from typing import Protocol

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


class NotificationSender(Protocol):
    def send(self, notification: Notification) -> SendResult: ...


class OpenClawSender:
    """Send plain text through OpenClaw's documented local CLI."""

    def __init__(self, config: NotificationsConfig) -> None:
        self.config = config

    def send(self, notification: Notification) -> SendResult:
        if not self.config.target.strip():
            return SendResult(False, "notifications.target is empty")
        executable = shutil.which(self.config.command) or self.config.command
        command = [
            executable,
            "message",
            "send",
            "--channel",
            "imessage",
            "--target",
            self.config.target,
            "--message",
            notification.message,
        ]
        for attempt in range(3):
            try:
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=self.config.timeout_seconds,
                    check=False,
                    shell=False,
                )
                if result.returncode == 0:
                    return SendResult(True)
                error = f"OpenClaw exited with status {result.returncode}"
            except (OSError, subprocess.TimeoutExpired) as exc:
                error = type(exc).__name__
            if attempt < 2:
                time.sleep(0.25 * (attempt + 1))
        return SendResult(False, error)


def summarize_changes(run_id: int, change_rows: list[dict]) -> str:
    """Build a compact, count-only message from the existing change log."""
    rows = [row for row in change_rows if row.get("run_id") == run_id]
    wines: dict[str, int] = {kind: 0 for kind in ("insert", "update", "delete", "rename")}
    bottles: dict[str, int] = {kind: 0 for kind in ("insert", "update", "delete", "rename")}
    other = 0
    for row in rows:
        entity = row.get("entity_type")
        change_type = row.get("change_type")
        if entity == "wine" and change_type in wines:
            wines[change_type] += 1
        elif entity == "bottle" and change_type in bottles:
            bottles[change_type] += 1
        else:
            other += 1

    lines = [f"Cellarbrain ETL completed (run {run_id})."]
    wine_parts = []
    for kind, label in (("insert", "added"), ("update", "updated"), ("delete", "deleted"), ("rename", "renamed")):
        if wines[kind]:
            wine_parts.append(f"{wines[kind]} {label}")
    lines.append("Wines: " + (", ".join(wine_parts) if wine_parts else "no changes"))
    bottle_count = sum(bottles.values())
    if bottle_count:
        lines.append(f"Inventory: {bottle_count} change record(s)")
    if other:
        lines.append(f"Other cellar records: {other} change record(s)")
    if not rows:
        lines.append("No changes detected.")
    return "\n".join(lines)


def _load_state(path: pathlib.Path) -> list[str]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return [str(item) for item in value if isinstance(item, str)] if isinstance(value, list) else []
    except (OSError, json.JSONDecodeError):
        return []


def _record_sent(path: pathlib.Path, key: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sent = [item for item in _load_state(path) if item != key]
    sent.append(key)
    fd, temporary = tempfile.mkstemp(prefix=f"{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(sent[-_MAX_SENT_KEYS:], stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def deliver(
    config: NotificationsConfig,
    output_dir: str | pathlib.Path,
    notification: Notification,
    *,
    sender: NotificationSender | None = None,
) -> SendResult:
    """Deliver once per key, persisting success only after confirmed delivery."""
    if not config.enabled:
        return SendResult(False, "disabled")
    state_path = pathlib.Path(output_dir) / _STATE_FILE
    if notification.key in _load_state(state_path):
        return SendResult(True)
    result = (sender or OpenClawSender(config)).send(notification)
    if result.sent:
        try:
            _record_sent(state_path, notification.key)
        except OSError as exc:
            logger.warning("Notification sent but delivery state could not be saved (%s).", type(exc).__name__)
    return result


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
