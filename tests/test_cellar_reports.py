from __future__ import annotations

import datetime as dt
import json
import subprocess

from cellarbrain.cellar_reports import (
    deliver_report,
    due_reports,
    period_bounds,
    record_snapshot,
    render_report,
)
from cellarbrain.cli import main
from cellarbrain.notifications import reconcile_report_delivery
from cellarbrain.settings import NotificationsConfig, ReportsConfig, load_settings


def test_period_bounds_handles_month_lengths_and_sunday_weeks():
    assert period_bounds("monthly", dt.date(2025, 2, 4)) == (dt.date(2025, 2, 1), dt.date(2025, 2, 28))
    assert period_bounds("monthly", dt.date(2024, 2, 4))[1] == dt.date(2024, 2, 29)
    assert period_bounds("weekly", dt.date(2025, 3, 9)) == (dt.date(2025, 3, 3), dt.date(2025, 3, 9))


def test_monthly_counts_and_transit_transition(tmp_path):
    record_snapshot(
        tmp_path,
        [
            {"bottle_id": 1, "status": "stored", "is_in_transit": True, "purchase_price": 50},
            {"bottle_id": 2, "status": "stored", "is_in_transit": False, "purchase_price": 20},
        ],
        dt.datetime(2025, 1, 31, 12),
    )
    record_snapshot(
        tmp_path,
        [
            {"bottle_id": 1, "status": "stored", "is_in_transit": False, "purchase_price": 50},
            {"bottle_id": 2, "status": "stored", "is_in_transit": False, "purchase_price": 20},
            {"bottle_id": 3, "status": "stored", "is_in_transit": True, "purchase_price": 30},
        ],
        dt.datetime(2025, 2, 10, 12),
    )
    record_snapshot(
        tmp_path,
        [
            {"bottle_id": 1, "status": "stored", "is_in_transit": False, "purchase_price": 50},
            {"bottle_id": 3, "status": "stored", "is_in_transit": True, "purchase_price": 30},
        ],
        dt.datetime(2025, 2, 20, 12),
    )
    text = render_report("monthly", tmp_path, dt.datetime(2025, 2, 28, 18, tzinfo=dt.UTC))
    assert "Stored at period end: 1 bottles · CHF 50.00" in text
    assert "On Order at period end: 1 bottles · CHF 30.00" in text
    assert "Stored at period end" in text and "  in: 1 bottles · CHF 50.00" in text
    assert "On Order at period end" in text and "  out: 1 bottles · CHF 50.00" in text


def test_weekly_shows_last_refresh_and_stays_short(tmp_path):
    record_snapshot(tmp_path, [{"bottle_id": 1, "status": "stored", "price": 9}], dt.datetime(2025, 3, 8, 10))
    text = render_report("weekly", tmp_path, dt.datetime(2025, 3, 9, 18, tzinfo=dt.UTC))
    assert "Last successful ETL refresh: 2025-03-08 11:00 CET" in text
    assert "Stored: 1 bottles" in text
    assert "Fluctuation" not in text


def test_toml_enable_disable_and_validation(tmp_path):
    config = tmp_path / "cellarbrain.toml"
    config.write_text('[reports]\nweekly_enabled = true\nmonthly_enabled = false\ntimezone = "Europe/Zurich"\n')
    assert load_settings(config).reports.weekly_enabled
    assert not load_settings(config).reports.monthly_enabled
    config.write_text('[reports]\nsend_time = "25:00"\n')
    try:
        load_settings(config)
    except ValueError as exc:
        assert "HH:MM" in str(exc)
    else:
        raise AssertionError("invalid time accepted")


def test_cli_preview_prints_without_delivery(tmp_path, capsys):
    record_snapshot(tmp_path, [{"bottle_id": 1, "status": "stored", "price": 9}], dt.datetime(2025, 3, 8, 10))
    config = tmp_path / "cellarbrain.toml"
    config.write_text(f'[paths]\ndata_dir = "{tmp_path}"\n', encoding="utf-8")
    main(["-c", str(config), "reports", "preview", "weekly"])
    assert "Cellar weekly report" in capsys.readouterr().out


def test_scheduler_due_and_delivery_idempotency_and_failure(tmp_path):
    cfg = ReportsConfig(weekly_enabled=True, monthly_enabled=True)
    sunday = dt.datetime(2025, 3, 30, 17, 5, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    assert due_reports(sunday, cfg) == ["weekly"]
    assert due_reports(sunday.replace(hour=16), cfg) == []
    assert due_reports(dt.datetime(2025, 3, 31, 17, 5, tzinfo=sunday.tzinfo), cfg) == ["monthly"]
    assert due_reports(dt.datetime(2025, 3, 24, 17, 5, tzinfo=sunday.tzinfo), cfg) == []

    class Sender:
        def __init__(self, result):
            self.result = result
            self.calls = 0

        def send(self, notification):
            self.calls += 1
            self.message = notification.message
            return self.result

    success = Sender(type("Result", (), {"sent": True, "error": None})())
    notif_cfg = NotificationsConfig(enabled=True, target="fake")
    assert deliver_report("weekly", tmp_path, notif_cfg, cfg, sender=success, now=sunday).sent
    assert deliver_report("weekly", tmp_path, notif_cfg, cfg, sender=success, now=sunday).sent
    assert success.calls == 1
    assert "Last successful ETL refresh" in success.message
    failed = Sender(type("Result", (), {"sent": False, "error": "mock failure"})())
    assert not deliver_report("monthly", tmp_path, notif_cfg, cfg, sender=failed, now=sunday).sent
    assert failed.calls == 1


def test_reports_are_due_only_on_local_calendar_boundaries():
    weekly = ReportsConfig(weekly_enabled=True, monthly_enabled=False, send_time="17:00", timezone="Europe/Zurich")
    monthly = ReportsConfig(weekly_enabled=False, monthly_enabled=True, send_time="17:00", timezone="Europe/Zurich")

    # Sunday 17:00 Zurich is the boundary even when supplied as a UTC instant.
    assert due_reports(dt.datetime(2025, 3, 30, 15, 0, tzinfo=dt.UTC), weekly) == ["weekly"]
    assert due_reports(dt.datetime(2025, 3, 30, 14, 59, tzinfo=dt.UTC), weekly) == []
    assert due_reports(dt.datetime(2025, 3, 31, 17, 30, tzinfo=dt.UTC), weekly) == []
    tuesday = ReportsConfig(weekly_enabled=True, monthly_enabled=False, weekly_day="tuesday")
    assert due_reports(dt.datetime(2025, 3, 25, 16, 0, tzinfo=dt.UTC), tuesday) == ["weekly"]

    # October 25 is not month end; no previous-month report is caught up.
    assert due_reports(dt.datetime(2025, 10, 25, 18, 0, tzinfo=dt.UTC), monthly) == []
    assert due_reports(dt.datetime(2025, 10, 31, 15, 59, tzinfo=dt.UTC), monthly) == []
    assert due_reports(dt.datetime(2025, 10, 31, 16, 0, tzinfo=dt.UTC), monthly) == ["monthly"]


def test_ambiguous_timeout_is_suppressed_across_polls_and_restart(tmp_path):
    cfg = ReportsConfig(weekly_enabled=True, monthly_enabled=False)
    notif_cfg = NotificationsConfig(enabled=True, target="fake")
    sunday = dt.datetime(2025, 3, 30, 17, 5, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    accepted = []

    def accepted_then_timeout(*args, **kwargs):
        accepted.append("cellar-weekly:2025-03-30")
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    from unittest.mock import patch

    with patch("cellarbrain.notifications.subprocess.run", side_effect=accepted_then_timeout) as run:
        first = deliver_report("weekly", tmp_path, notif_cfg, cfg, now=sunday)
        assert not first.sent
        second_poll = deliver_report("weekly", tmp_path, notif_cfg, cfg, now=sunday)
        # A fresh call after a simulated process restart reads the same durable state.
        restarted = deliver_report("weekly", tmp_path, notif_cfg, cfg, now=sunday)

    assert len(accepted) == 1
    assert run.call_count == 1
    assert second_poll.error == "uncertain"
    assert restarted.error == "uncertain"
    state = json.loads((tmp_path / ".cellarbrain-notifications.json").read_text())
    assert state["deliveries"]["cellar-weekly:2025-03-30"]["state"] == "uncertain"


def test_report_success_deduplicates_and_stores_openclaw_message_id(tmp_path):
    cfg = ReportsConfig(weekly_enabled=True, monthly_enabled=False)
    notif_cfg = NotificationsConfig(enabled=True, target="fake")
    sunday = dt.datetime(2025, 3, 30, 17, 5, tzinfo=dt.timezone(dt.timedelta(hours=2)))

    class Sender:
        calls = 0

        def send(self, notification):
            self.calls += 1
            return type("Result", (), {"sent": True, "error": None, "message_id": "msg-123"})()

    sender = Sender()
    assert deliver_report("weekly", tmp_path, notif_cfg, cfg, sender=sender, now=sunday).sent
    assert deliver_report("weekly", tmp_path, notif_cfg, cfg, sender=sender, now=sunday).sent
    assert sender.calls == 1
    state = json.loads((tmp_path / ".cellarbrain-notifications.json").read_text())
    record = state["deliveries"]["cellar-weekly:2025-03-30"]
    assert record["state"] == "sent"
    assert record["message_id"] == "msg-123"


def test_report_retries_only_failure_proven_before_dispatch(tmp_path):
    cfg = ReportsConfig(weekly_enabled=True, monthly_enabled=False)
    notif_cfg = NotificationsConfig(enabled=True, target="fake")
    sunday = dt.datetime(2025, 3, 30, 17, 5, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    from unittest.mock import patch

    with patch(
        "cellarbrain.notifications.subprocess.run",
        side_effect=[
            FileNotFoundError("missing executable"),
            subprocess.CompletedProcess([], 0, '{"messageId":"m2"}', ""),
        ],
    ) as run:
        first = deliver_report("weekly", tmp_path, notif_cfg, cfg, now=sunday)
        second = deliver_report("weekly", tmp_path, notif_cfg, cfg, now=sunday)
    assert not first.sent and first.retryable
    assert second.sent
    assert run.call_count == 2


def test_legacy_notification_state_and_uncertain_reconciliation(tmp_path):
    state_path = tmp_path / ".cellarbrain-notifications.json"
    state_path.write_text('["cellar-weekly:2025-03-30", "etl:7"]')

    class Sender:
        calls = 0

        def send(self, notification):
            self.calls += 1
            raise AssertionError("legacy sent key must be preserved")

    sender = Sender()
    cfg = ReportsConfig(weekly_enabled=True, monthly_enabled=False)
    sunday = dt.datetime(2025, 3, 30, 17, 5, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    assert deliver_report("weekly", tmp_path, NotificationsConfig(enabled=True), cfg, sender=sender, now=sunday).sent
    assert sender.calls == 0

    state_path.write_text('{"sent":["etl:7"],"report_deliveries":{"cellar-weekly:2025-03-30":{"state":"uncertain"}}}')
    reconcile_report_delivery(tmp_path, "cellar-weekly:2025-03-30", "delivered")
    state = json.loads(state_path.read_text())
    assert state["deliveries"]["cellar-weekly:2025-03-30"]["state"] == "sent"
    reconcile_report_delivery(tmp_path, "cellar-weekly:2025-03-30", "retry")
    state = json.loads(state_path.read_text())
    assert "cellar-weekly:2025-03-30" not in state["deliveries"]
    assert "cellar-weekly:2025-03-30" not in state["sent"]


def test_reconcile_cli_requires_explicit_non_delivery_confirmation(tmp_path, capsys):
    config = tmp_path / "cellarbrain.toml"
    config.write_text(f'[paths]\ndata_dir = "{tmp_path}"\n')
    state_path = tmp_path / ".cellarbrain-notifications.json"
    state_path.write_text('{"sent":[],"report_deliveries":{"cellar-weekly:2025-03-30":{"state":"uncertain"}}}')
    try:
        main(["-c", str(config), "reports", "reconcile", "cellar-weekly:2025-03-30", "--retry"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("retry without confirmation should fail")
    assert json.loads(state_path.read_text())["report_deliveries"]["cellar-weekly:2025-03-30"]["state"] == "uncertain"

    main(
        [
            "-c",
            str(config),
            "reports",
            "reconcile",
            "cellar-weekly:2025-03-30",
            "--retry",
            "--confirm-not-delivered",
        ]
    )
    assert "reconciled as retry" in capsys.readouterr().out
    assert "cellar-weekly:2025-03-30" not in json.loads(state_path.read_text())["deliveries"]


def test_disabled_report_never_calls_sender(tmp_path):
    class Sender:
        def send(self, notification):
            raise AssertionError("disabled report must not send")

    result = deliver_report("monthly", tmp_path, NotificationsConfig(enabled=True), ReportsConfig(), sender=Sender())
    assert not result.sent
    assert result.error == "disabled"
