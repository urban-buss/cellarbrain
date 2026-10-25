"""Optional, local OpenClaw notifications for completed Cellarbrain ETL runs."""

from __future__ import annotations

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


class NotificationSender(Protocol):
    def send(self, notification: Notification) -> SendResult: ...


class OpenClawSender:
    """Send plain text through OpenClaw's documented local CLI."""

    def __init__(self, config: NotificationsConfig) -> None:
        self.config = config

    def send(self, notification: Notification, *, verbose: bool = False) -> SendResult:
        if not self.config.target.strip():
            return SendResult(False, "notifications.target is empty")
        executable = shutil.which(self.config.command) or self.config.command
        if verbose:
            print(f"OpenClaw executable: {executable}")
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
                if verbose:
                    _print_process_output("stdout", result.stdout)
                    _print_process_output("stderr", result.stderr, file=sys.stderr)
                if result.returncode == 0:
                    if verbose:
                        print(f"OpenClaw iMessage attempt {attempt + 1}/3 succeeded.")
                    return SendResult(True)
                error = f"OpenClaw exited with status {result.returncode}"
                if verbose:
                    print(f"OpenClaw iMessage attempt {attempt + 1}/3 failed: {error}", file=sys.stderr)
            except subprocess.TimeoutExpired as exc:
                error = type(exc).__name__
                if verbose:
                    _print_process_output("stdout", exc.stdout)
                    _print_process_output("stderr", exc.stderr, file=sys.stderr)
                    print(
                        f"OpenClaw iMessage attempt {attempt + 1}/3 timed out "
                        f"after {self.config.timeout_seconds} seconds.",
                        file=sys.stderr,
                    )
            except OSError as exc:
                error = type(exc).__name__
                if verbose:
                    print(f"OpenClaw iMessage attempt {attempt + 1}/3 could not start: {exc}", file=sys.stderr)
            if attempt < 2:
                time.sleep(0.25 * (attempt + 1))
        return SendResult(False, error)


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
