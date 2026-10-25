"""Tests for the optional OpenClaw ETL notification integration."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cellarbrain import notifications
from cellarbrain.cli import _run_etl_command, main
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
        assert "Bottles: 1 removed" in summary
        assert "Other: 1 winery updated" in summary
        assert "No changes" not in summary

    def test_summarizes_bottle_actions_separately_and_ignores_other_runs(self):
        rows = [
            {"run_id": 8, "entity_type": "bottle", "change_type": "insert"},
            {"run_id": 8, "entity_type": "bottle", "change_type": "update"},
            {"run_id": 8, "entity_type": "bottle", "change_type": "delete"},
            {"run_id": 7, "entity_type": "bottle", "change_type": "insert"},
            {"run_id": 8, "entity_type": "grape", "change_type": "insert"},
        ]
        summary = notifications.summarize_changes(8, rows)
        assert "Bottles: 1 added, 1 updated, 1 removed" in summary
        assert "Other: 1 grape added" in summary
        assert "3 added" not in summary

    def test_always_reports_no_changes(self):
        summary = notifications.summarize_changes(3, [])
        assert "ETL completed" in summary
        assert "No data changes detected." in summary


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
            "--json",
            "--target",
            "+41788689840",
            "--message",
            "hello",
        ]
        assert kwargs["shell"] is False
        assert kwargs["timeout"] == 15

    @patch("cellarbrain.notifications.time.sleep")
    @patch("cellarbrain.notifications.subprocess.run")
    def test_nonzero_exit_is_not_retried_and_does_not_expose_output(self, run, sleep):
        run.return_value = subprocess.CompletedProcess([], 1, "private output", "secret token")
        config = NotificationsConfig(enabled=True, target="+41788689840")
        result = notifications.OpenClawSender(config).send(notifications.Notification("k", "hello"))
        assert not result.sent
        assert result.error == "OpenClaw exited with status 1"
        assert run.call_count == 1
        sleep.assert_not_called()

    def test_verbose_send_prints_openclaw_output(self, capsys):
        completed = subprocess.CompletedProcess([], 0, "sent\n", "connected\n")
        config = NotificationsConfig(enabled=True, target="dest")
        with patch("cellarbrain.notifications.subprocess.run", return_value=completed):
            result = notifications.OpenClawSender(config).send(
                notifications.Notification("test", "hello"),
                verbose=True,
            )
        captured = capsys.readouterr()
        assert result.sent
        assert "OpenClaw stdout:\nsent" in captured.out
        assert "send succeeded" in captured.out
        assert "OpenClaw stderr:\nconnected" in captured.err

    def test_verbose_send_reports_startup_errors(self, capsys):
        config = NotificationsConfig(enabled=True, target="dest")
        with (
            patch("cellarbrain.notifications.subprocess.run", side_effect=FileNotFoundError("openclaw missing")),
            patch("cellarbrain.notifications.time.sleep"),
        ):
            result = notifications.OpenClawSender(config).send(
                notifications.Notification("test", "hello"),
                verbose=True,
            )
        captured = capsys.readouterr()
        assert not result.sent
        assert result.error == "FileNotFoundError"
        assert "could not start: openclaw missing" in captured.err


class TestNotificationsCli:
    def test_test_command_sends_even_if_automatic_notifications_are_disabled(self, tmp_path, capsys):
        config_path = tmp_path / "cellarbrain.toml"
        config_path.write_text('[notifications]\ntarget = "configured-target"\n', encoding="utf-8")
        completed = subprocess.CompletedProcess([], 0, "message accepted\n", "")
        with (
            patch("cellarbrain.notifications.subprocess.run", return_value=completed) as run,
            patch("cellarbrain.notifications.shutil.which", return_value=None),
        ):
            main(["-c", str(config_path), "notifications", "test"])
        captured = capsys.readouterr()
        assert "Diagnostic iMessage sent successfully." in captured.out
        assert "message accepted" in captured.out
        assert run.call_args.args[0][-4:] == [
            "--target",
            "configured-target",
            "--message",
            "Cellarbrain OpenClaw iMessage integration test.",
        ]

    def test_test_command_overrides_target_and_fails_with_openclaw_details(self, tmp_path, capsys):
        config_path = tmp_path / "cellarbrain.toml"
        config_path.write_text("[notifications]\nenabled = false\n", encoding="utf-8")
        failed = subprocess.CompletedProcess([], 1, "request started\n", "channel unavailable\n")
        with (
            patch("cellarbrain.notifications.subprocess.run", return_value=failed) as run,
            patch("cellarbrain.notifications.shutil.which", return_value=None),
            patch("cellarbrain.notifications.time.sleep"),
            pytest.raises(SystemExit) as exc_info,
        ):
            main(
                [
                    "-c",
                    str(config_path),
                    "notifications",
                    "test",
                    "--target",
                    "override-target",
                    "--message",
                    "custom test",
                ]
            )
        captured = capsys.readouterr()
        assert exc_info.value.code == 1
        assert "channel unavailable" in captured.err
        assert "OpenClaw exited with status 1" in captured.err
        assert run.call_args.args[0][-4:] == ["--target", "override-target", "--message", "custom test"]

    def test_test_command_requires_a_target(self, tmp_path, capsys):
        config_path = tmp_path / "cellarbrain.toml"
        config_path.write_text("[notifications]\n", encoding="utf-8")
        with pytest.raises(SystemExit) as exc_info:
            main(["-c", str(config_path), "notifications", "test"])
        captured = capsys.readouterr()
        assert exc_info.value.code == 1
        assert "notifications.target is empty" in captured.err


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
