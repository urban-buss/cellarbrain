"""Tests for the optional OpenClaw ETL notification integration."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cellarbrain import notifications
from cellarbrain.cli import _run_etl_command
from cellarbrain.settings import NotificationsConfig, Settings


class TestSummarizeChanges:
    def test_summarizes_wine_inventory_and_other_changes(self):
        rows = [
            {"run_id": 7, "entity_type": "wine", "change_type": "insert"},
            {"run_id": 7, "entity_type": "wine", "change_type": "update"},
            {"run_id": 7, "entity_type": "wine", "change_type": "rename"},
            {"run_id": 7, "entity_type": "bottle", "change_type": "delete"},
            {"run_id": 7, "entity_type": "winery", "change_type": "update"},
            {"run_id": 6, "entity_type": "wine", "change_type": "delete"},
        ]
        summary = notifications.summarize_changes(7, rows)
        assert "run 7" in summary
        assert "1 added, 1 updated, 1 renamed" in summary
        assert "Inventory: 1 change record(s)" in summary
        assert "Other cellar records: 1" in summary
        assert "No changes" not in summary

    def test_always_reports_no_changes(self):
        summary = notifications.summarize_changes(3, [])
        assert "ETL completed" in summary
        assert "Wines: no changes" in summary
        assert "No changes detected." in summary


class TestOpenClawSender:
    @patch("cellarbrain.notifications.subprocess.run")
    @patch("cellarbrain.notifications.shutil.which", return_value=None)
    def test_uses_shell_free_documented_message_command(self, which, run):
        run.return_value = subprocess.CompletedProcess([], 0, "", "")
        config = NotificationsConfig(enabled=True, target="+41788689840")
        result = notifications.OpenClawSender(config).send(notifications.Notification("k", "hello"))
        assert result.sent
        which.assert_called_once_with("openclaw")
        args, kwargs = run.call_args
        assert args[0] == [
            "openclaw",
            "message",
            "send",
            "--channel",
            "imessage",
            "--target",
            "+41788689840",
            "--message",
            "hello",
        ]
        assert kwargs["shell"] is False
        assert kwargs["timeout"] == 15

    @patch("cellarbrain.notifications.time.sleep")
    @patch("cellarbrain.notifications.subprocess.run")
    def test_retries_then_returns_failure_without_output_text(self, run, sleep):
        run.return_value = subprocess.CompletedProcess([], 1, "private output", "secret token")
        config = NotificationsConfig(enabled=True, target="+41788689840")
        result = notifications.OpenClawSender(config).send(notifications.Notification("k", "hello"))
        assert not result.sent
        assert result.error == "OpenClaw exited with status 1"
        assert run.call_count == 3
        assert sleep.call_count == 2


class TestDelivery:
    def test_disabled_does_not_call_sender(self, tmp_path):
        sender = MagicMock()
        result = notifications.deliver(
            NotificationsConfig(), tmp_path, notifications.Notification("k", "hello"), sender=sender
        )
        assert not result.sent
        sender.send.assert_not_called()

    def test_persists_success_and_suppresses_duplicate(self, tmp_path):
        sender = MagicMock()
        sender.send.return_value = notifications.SendResult(True)
        config = NotificationsConfig(enabled=True, target="dest")
        event = notifications.Notification("success:1", "hello")
        assert notifications.deliver(config, tmp_path, event, sender=sender).sent
        assert notifications.deliver(config, tmp_path, event, sender=sender).sent
        sender.send.assert_called_once_with(event)
        assert (Path(tmp_path) / ".cellarbrain-notifications.json").exists()

    def test_failed_send_is_not_recorded_for_future_retry(self, tmp_path):
        sender = MagicMock()
        sender.send.return_value = notifications.SendResult(False, "not available")
        config = NotificationsConfig(enabled=True, target="dest")
        event = notifications.Notification("success:1", "hello")
        notifications.deliver(config, tmp_path, event, sender=sender)
        notifications.deliver(config, tmp_path, event, sender=sender)
        assert sender.send.call_count == 2

    def test_strict_success_delivery_raises(self, tmp_path):
        with patch.object(notifications, "deliver", return_value=notifications.SendResult(False, "missing")):
            with pytest.raises(notifications.NotificationError, match="Could not send"):
                notifications.notify_success(NotificationsConfig(enabled=True, strict=True), tmp_path, 1, [])

    def test_failure_notification_never_raises(self, tmp_path):
        with patch.object(notifications, "deliver", side_effect=OSError("disk full")):
            notifications.notify_failure(NotificationsConfig(enabled=True), tmp_path, 1)


class TestEtlHook:
    def test_success_notifies_after_run_from_change_log(self, tmp_path, monkeypatch):
        settings = Settings(notifications=NotificationsConfig(enabled=True, target="dest"))
        events: list[tuple] = []

        def fake_run(*args, **kwargs):
            events.append(("etl", kwargs["sync_mode"]))
            return True

        monkeypatch.setattr("cellarbrain.cli.run", fake_run)
        monkeypatch.setattr(
            "cellarbrain.notifications.notify_success", lambda *args: events.append(("success", args[2], args[3]))
        )
        monkeypatch.setattr("cellarbrain.notifications.notify_failure", lambda *args: events.append(("failure",)))
        monkeypatch.setattr("cellarbrain.incremental.load_change_log", lambda path: [{"run_id": 1}])

        assert _run_etl_command(
            "w.csv", "b.csv", str(tmp_path), sync_mode=True, bottles_gone_csv="g.csv", settings=settings
        )
        assert events == [("etl", True), ("success", 1, [{"run_id": 1}])]

    def test_run_exception_sends_best_effort_failure_and_preserves_error(self, tmp_path, monkeypatch):
        settings = Settings(notifications=NotificationsConfig(enabled=True, target="dest"))
        failure = OSError("ETL broke")
        sent = []
        monkeypatch.setattr("cellarbrain.cli.run", MagicMock(side_effect=failure))
        monkeypatch.setattr("cellarbrain.notifications.notify_failure", lambda *args: sent.append(args))

        with pytest.raises(OSError, match="ETL broke"):
            _run_etl_command(
                "w.csv", "b.csv", str(tmp_path), sync_mode=False, bottles_gone_csv="g.csv", settings=settings
            )
        assert len(sent) == 1

    def test_validation_failure_sends_failure_and_returns_false(self, tmp_path, monkeypatch):
        settings = Settings(notifications=NotificationsConfig(enabled=True, target="dest"))
        sent = []
        monkeypatch.setattr("cellarbrain.cli.run", lambda *args, **kwargs: False)
        monkeypatch.setattr("cellarbrain.notifications.notify_failure", lambda *args: sent.append(args))
        assert not _run_etl_command(
            "w.csv", "b.csv", str(tmp_path), sync_mode=False, bottles_gone_csv="g.csv", settings=settings
        )
        assert len(sent) == 1
