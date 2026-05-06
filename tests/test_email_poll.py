"""Tests for cellarbrain.email_poll — grouping, placement, credentials, ETL runner."""

from __future__ import annotations

import subprocess
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from cellarbrain.email_poll.grouping import Batch, EmailMessage, group_messages
from cellarbrain.email_poll.placement import SnapshotCollisionError, place_batch

EXPECTED = [
    "export-wines.csv",
    "export-bottles-stored.csv",
    "export-bottles-gone.csv",
]


def _msg(uid: int, dt: datetime, filename: str, size: int = 100) -> EmailMessage:
    return EmailMessage(uid=uid, date=dt, filename=filename, size=size)


# ---------------------------------------------------------------------------
# TestGroupMessages
# ---------------------------------------------------------------------------


class TestGroupMessages:
    def test_complete_batch(self):
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        msgs = [
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=5), "export-bottles-stored.csv"),
            _msg(3, t0 + timedelta(seconds=10), "export-bottles-gone.csv"),
        ]
        batches = group_messages(msgs, EXPECTED, 300)
        assert len(batches) == 1
        assert batches[0].filenames == frozenset(EXPECTED)
        assert batches[0].uids == (1, 2, 3)

    def test_empty_input(self):
        assert group_messages([], EXPECTED, 300) == []

    def test_incomplete_batch_discarded(self):
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        msgs = [
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=5), "export-bottles-stored.csv"),
        ]
        batches = group_messages(msgs, EXPECTED, 300)
        assert len(batches) == 0

    def test_out_of_window_split(self):
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        t1 = t0 + timedelta(seconds=600)  # 10 min later — outside 300s window
        msgs = [
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=5), "export-bottles-stored.csv"),
            # gap > 300s
            _msg(3, t1, "export-bottles-gone.csv"),
        ]
        batches = group_messages(msgs, EXPECTED, 300)
        assert len(batches) == 0  # neither group is complete

    def test_multiple_complete_batches(self):
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        t1 = t0 + timedelta(minutes=10)
        msgs = [
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=5), "export-bottles-stored.csv"),
            _msg(3, t0 + timedelta(seconds=10), "export-bottles-gone.csv"),
            _msg(4, t1, "export-wines.csv"),
            _msg(5, t1 + timedelta(seconds=5), "export-bottles-stored.csv"),
            _msg(6, t1 + timedelta(seconds=10), "export-bottles-gone.csv"),
        ]
        batches = group_messages(msgs, EXPECTED, 300)
        assert len(batches) == 2
        assert batches[0].uids == (1, 2, 3)
        assert batches[1].uids == (4, 5, 6)

    def test_duplicate_filenames_rejected(self):
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        msgs = [
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=5), "export-wines.csv"),
            _msg(3, t0 + timedelta(seconds=10), "export-bottles-stored.csv"),
        ]
        batches = group_messages(msgs, EXPECTED, 300)
        assert len(batches) == 0

    def test_single_message(self):
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        msgs = [_msg(1, t0, "export-wines.csv")]
        batches = group_messages(msgs, EXPECTED, 300)
        assert len(batches) == 0

    def test_unsorted_input_sorted_internally(self):
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        msgs = [
            _msg(3, t0 + timedelta(seconds=10), "export-bottles-gone.csv"),
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=5), "export-bottles-stored.csv"),
        ]
        batches = group_messages(msgs, EXPECTED, 300)
        assert len(batches) == 1
        # Messages sorted by date internally
        assert batches[0].messages[0].uid == 1

    def test_batch_properties(self):
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        msgs = [
            _msg(10, t0, "export-wines.csv", 400),
            _msg(20, t0 + timedelta(seconds=5), "export-bottles-stored.csv", 800),
            _msg(30, t0 + timedelta(seconds=10), "export-bottles-gone.csv", 300),
        ]
        batches = group_messages(msgs, EXPECTED, 300)
        batch = batches[0]
        assert isinstance(batch, Batch)
        assert batch.uids == (10, 20, 30)
        assert batch.filenames == frozenset(EXPECTED)


# ---------------------------------------------------------------------------
# TestPlaceBatch
# ---------------------------------------------------------------------------


class TestPlaceBatch:
    def test_writes_snapshot_and_toplevel(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        files = {
            "export-wines.csv": b"wines-data",
            "export-bottles-stored.csv": b"bottles-data",
            "export-bottles-gone.csv": b"gone-data",
        }
        now = datetime(2026, 4, 28, 14, 35)
        snapshot = place_batch(files, raw_dir, now=now)

        assert snapshot.name == "260428-1435"
        assert (snapshot / "export-wines.csv").read_bytes() == b"wines-data"
        assert (raw_dir / "export-wines.csv").read_bytes() == b"wines-data"
        assert (raw_dir / "export-bottles-stored.csv").read_bytes() == b"bottles-data"
        assert (raw_dir / "export-bottles-gone.csv").read_bytes() == b"gone-data"

    def test_flushes_old_csvs(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        (raw_dir / "old-file.csv").write_text("old", encoding="utf-8")
        (raw_dir / "another.csv").write_text("old2", encoding="utf-8")

        files = {"export-wines.csv": b"new"}
        now = datetime(2026, 4, 28, 15, 0)
        place_batch(files, raw_dir, now=now)

        assert not (raw_dir / "old-file.csv").exists()
        assert not (raw_dir / "another.csv").exists()
        assert (raw_dir / "export-wines.csv").read_bytes() == b"new"

    def test_collision_raises(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        (raw_dir / "260428-1435").mkdir()

        now = datetime(2026, 4, 28, 14, 35)
        with pytest.raises(SnapshotCollisionError):
            place_batch({"export-wines.csv": b"data"}, raw_dir, now=now)

    def test_preserves_non_csv_files(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        (raw_dir / "readme.txt").write_text("keep me", encoding="utf-8")
        (raw_dir / ".ingest-state.json").write_text("{}", encoding="utf-8")

        files = {"export-wines.csv": b"data"}
        now = datetime(2026, 4, 28, 15, 0)
        place_batch(files, raw_dir, now=now)

        assert (raw_dir / "readme.txt").read_text(encoding="utf-8") == "keep me"
        assert (raw_dir / ".ingest-state.json").exists()


# ---------------------------------------------------------------------------
# TestCredentialResolution
# ---------------------------------------------------------------------------


class TestCredentialResolution:
    def test_env_var_fallback(self, monkeypatch):
        from cellarbrain.email_poll.credentials import resolve_credentials

        monkeypatch.setenv("CELLARBRAIN_IMAP_USER", "user@test.com")
        monkeypatch.setenv("CELLARBRAIN_IMAP_PASSWORD", "secret123")

        with patch("cellarbrain.email_poll.credentials._try_keyring", return_value=("", "")):
            user, pw = resolve_credentials()
        assert user == "user@test.com"
        assert pw == "secret123"

    def test_keyring_priority(self, monkeypatch):
        from cellarbrain.email_poll.credentials import resolve_credentials

        monkeypatch.setenv("CELLARBRAIN_IMAP_USER", "env@test.com")
        monkeypatch.setenv("CELLARBRAIN_IMAP_PASSWORD", "env-pass")

        with patch(
            "cellarbrain.email_poll.credentials._try_keyring",
            return_value=("keyring@test.com", "kr-pass"),
        ):
            user, pw = resolve_credentials()
        assert user == "keyring@test.com"
        assert pw == "kr-pass"

    def test_no_credentials_raises(self, monkeypatch):
        from cellarbrain.email_poll.credentials import resolve_credentials

        monkeypatch.delenv("CELLARBRAIN_IMAP_USER", raising=False)
        monkeypatch.delenv("CELLARBRAIN_IMAP_PASSWORD", raising=False)

        with patch("cellarbrain.email_poll.credentials._try_keyring", return_value=("", "")):
            with pytest.raises(ValueError, match="IMAP credentials not found"):
                resolve_credentials()

    def test_keyring_import_error_falls_through(self, monkeypatch):
        from cellarbrain.email_poll.credentials import resolve_credentials

        monkeypatch.setenv("CELLARBRAIN_IMAP_USER", "user@test.com")
        monkeypatch.setenv("CELLARBRAIN_IMAP_PASSWORD", "pass")

        # Simulate keyring not installed — _try_keyring returns empty
        with patch("cellarbrain.email_poll.credentials._try_keyring", return_value=("", "")):
            user, pw = resolve_credentials()
        assert user == "user@test.com"


# ---------------------------------------------------------------------------
# TestRunEtl
# ---------------------------------------------------------------------------


class TestRunEtl:
    def test_success(self, tmp_path):
        from cellarbrain.email_poll.etl_runner import run_etl

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "ETL done\n"
        mock_result.stderr = ""

        with patch("cellarbrain.email_poll.etl_runner.subprocess.run", return_value=mock_result) as mock_run:
            code, output = run_etl(raw_dir, output_dir)
        assert code == 0
        assert "ETL done" in output
        mock_run.assert_called_once()

    def test_failure(self, tmp_path):
        from cellarbrain.email_poll.etl_runner import run_etl

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Error: bad data"

        with patch("cellarbrain.email_poll.etl_runner.subprocess.run", return_value=mock_result):
            code, output = run_etl(raw_dir, output_dir)
        assert code == 1
        assert "Error: bad data" in output

    def test_timeout(self, tmp_path):
        from cellarbrain.email_poll.etl_runner import run_etl

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with patch(
            "cellarbrain.email_poll.etl_runner.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="test", timeout=300),
        ):
            code, output = run_etl(raw_dir, output_dir)
        assert code == -1
        assert "timed out" in output

    def test_config_path_included(self, tmp_path):
        from cellarbrain.email_poll.etl_runner import run_etl

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        config = tmp_path / "cellarbrain.toml"
        config.write_text("", encoding="utf-8")

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_result.stderr = ""

        with patch("cellarbrain.email_poll.etl_runner.subprocess.run", return_value=mock_result) as mock_run:
            run_etl(raw_dir, output_dir, config)
        cmd = mock_run.call_args[0][0]
        assert "--config" in cmd
        assert str(config) in cmd

    def test_utf8_env_forced(self, tmp_path):
        """ETL subprocess gets PYTHONUTF8=1 and PYTHONIOENCODING=utf-8 (#002)."""
        from cellarbrain.email_poll.etl_runner import run_etl

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_result.stderr = ""

        with patch("cellarbrain.email_poll.etl_runner.subprocess.run", return_value=mock_result) as mock_run:
            run_etl(raw_dir, output_dir)
        env = mock_run.call_args[1]["env"]
        assert env["PYTHONUTF8"] == "1"
        assert env["PYTHONIOENCODING"] == "utf-8"


# ---------------------------------------------------------------------------
# TestStateFile
# ---------------------------------------------------------------------------


class TestStateFile:
    def test_load_missing_state(self, tmp_path):
        from cellarbrain.email_poll import _load_state

        state = _load_state(tmp_path)
        assert state["processed_uids"] == []

    def test_save_and_load_roundtrip(self, tmp_path):
        from cellarbrain.email_poll import _load_state, _save_state

        state = {
            "processed_uids": [1, 2, 3],
            "last_poll": "2026-04-28T14:35:00+00:00",
            "last_batch": "260428-1435",
        }
        _save_state(tmp_path, state)

        loaded = _load_state(tmp_path)
        assert loaded["processed_uids"] == [1, 2, 3]
        assert loaded["last_batch"] == "260428-1435"


# ---------------------------------------------------------------------------
# TestImapFetchMessages
# ---------------------------------------------------------------------------


def _make_mime_message(filename: str, payload: bytes) -> bytes:
    """Build a minimal MIME message with one attachment."""
    import email.mime.application
    import email.mime.multipart

    msg = email.mime.multipart.MIMEMultipart()
    msg["Subject"] = "[VinoCell] CSV file"
    att = email.mime.application.MIMEApplication(payload, Name=filename)
    att.add_header("Content-Disposition", "attachment", filename=filename)
    msg.attach(att)
    return msg.as_bytes()


class TestImapFetchMessages:
    """Regression tests for iCloud IMAP BODY[] key issue (#001)."""

    def test_body_bracket_key(self):
        """iCloud returns body under b'BODY[]' — should still work."""
        from cellarbrain.email_poll.imap import ImapClient

        mime_bytes = _make_mime_message("export-wines.csv", b"wine,data")

        mock_client = MagicMock()
        mock_client.fetch.return_value = {
            48: {
                b"BODY[]": mime_bytes,
                b"INTERNALDATE": datetime(2026, 5, 1, 10, 0),
            }
        }

        imap = ImapClient.__new__(ImapClient)
        imap._client = mock_client

        results = imap.fetch_messages([48], ["export-wines.csv"])
        assert len(results) == 1
        em, data = results[0]
        assert em.uid == 48
        assert em.filename == "export-wines.csv"
        assert data == b"wine,data"
        mock_client.fetch.assert_called_once_with([48], ["BODY.PEEK[]", "INTERNALDATE"])

    def test_rfc822_key_fallback(self):
        """Servers returning b'RFC822' should still work (fallback)."""
        from cellarbrain.email_poll.imap import ImapClient

        mime_bytes = _make_mime_message("export-wines.csv", b"wine,data")

        mock_client = MagicMock()
        mock_client.fetch.return_value = {
            99: {
                b"RFC822": mime_bytes,
                b"INTERNALDATE": datetime(2026, 5, 1, 10, 0),
            }
        }

        imap = ImapClient.__new__(ImapClient)
        imap._client = mock_client

        results = imap.fetch_messages([99], ["export-wines.csv"])
        assert len(results) == 1
        em, data = results[0]
        assert em.uid == 99
        assert em.filename == "export-wines.csv"

    def test_empty_body_skipped(self):
        """Messages with no body data should be skipped."""
        from cellarbrain.email_poll.imap import ImapClient

        mock_client = MagicMock()
        mock_client.fetch.return_value = {
            50: {
                b"INTERNALDATE": datetime(2026, 5, 1, 10, 0),
            }
        }

        imap = ImapClient.__new__(ImapClient)
        imap._client = mock_client

        results = imap.fetch_messages([50], ["export-wines.csv"])
        assert len(results) == 0


# ---------------------------------------------------------------------------
# TestPollOnceEtlFailure
# ---------------------------------------------------------------------------


class TestPollOnceEtlFailure:
    """Verify poll_once does NOT mark messages processed on ETL failure (#003)."""

    def _make_settings(self, tmp_path):
        """Build minimal Settings and IngestConfig for testing."""
        from cellarbrain.settings import IngestConfig

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        config = IngestConfig()

        settings = MagicMock()
        settings.paths.raw_dir = str(raw_dir)
        settings.paths.data_dir = str(output_dir)
        settings.config_source = None

        return config, settings, raw_dir

    def test_etl_failure_leaves_messages_unprocessed(self, tmp_path):
        """On ETL failure, messages should NOT be marked as read."""
        from cellarbrain.email_poll import poll_once
        from cellarbrain.email_poll.grouping import Batch

        config, settings, raw_dir = self._make_settings(tmp_path)

        t0 = datetime(2026, 5, 1, 10, 0)

        mock_imap = MagicMock()
        mock_imap.search_unseen.return_value = [48, 49, 50]
        mock_imap.fetch_messages.return_value = [
            (EmailMessage(uid=48, date=t0, filename="export-wines.csv", size=100), b"wines"),
            (EmailMessage(uid=49, date=t0, filename="export-bottles-stored.csv", size=100), b"bottles"),
            (EmailMessage(uid=50, date=t0, filename="export-bottles-gone.csv", size=100), b"gone"),
        ]
        mock_imap.__enter__ = MagicMock(return_value=mock_imap)
        mock_imap.__exit__ = MagicMock(return_value=False)

        batch = Batch(
            messages=[
                EmailMessage(uid=48, date=t0, filename="export-wines.csv", size=100),
                EmailMessage(uid=49, date=t0, filename="export-bottles-stored.csv", size=100),
                EmailMessage(uid=50, date=t0, filename="export-bottles-gone.csv", size=100),
            ]
        )

        with (
            patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_imap),
            patch("cellarbrain.email_poll.poll_once.__module__", "cellarbrain.email_poll"),
            patch("cellarbrain.email_poll.credentials.resolve_credentials", return_value=("u", "p")),
            patch("cellarbrain.email_poll.grouping.group_messages", return_value=[batch]),
            patch("cellarbrain.email_poll.placement.place_batch", return_value=raw_dir / "260501-1000"),
            patch("cellarbrain.email_poll.etl_runner.run_etl", return_value=(1, "Error: bad data")),
        ):
            result = poll_once(config, settings)

        # ETL failed → should NOT mark messages
        mock_imap.mark_seen.assert_not_called()
        mock_imap.move_messages.assert_not_called()
        # Return value should be negative (indicating failure)
        assert result == -1

    def test_etl_success_marks_messages(self, tmp_path):
        """On ETL success, messages should be marked as read."""
        from cellarbrain.email_poll import poll_once
        from cellarbrain.email_poll.grouping import Batch

        config, settings, raw_dir = self._make_settings(tmp_path)

        t0 = datetime(2026, 5, 1, 10, 0)

        mock_imap = MagicMock()
        mock_imap.search_unseen.return_value = [48, 49, 50]
        mock_imap.fetch_messages.return_value = [
            (EmailMessage(uid=48, date=t0, filename="export-wines.csv", size=100), b"wines"),
            (EmailMessage(uid=49, date=t0, filename="export-bottles-stored.csv", size=100), b"bottles"),
            (EmailMessage(uid=50, date=t0, filename="export-bottles-gone.csv", size=100), b"gone"),
        ]
        mock_imap.__enter__ = MagicMock(return_value=mock_imap)
        mock_imap.__exit__ = MagicMock(return_value=False)

        batch = Batch(
            messages=[
                EmailMessage(uid=48, date=t0, filename="export-wines.csv", size=100),
                EmailMessage(uid=49, date=t0, filename="export-bottles-stored.csv", size=100),
                EmailMessage(uid=50, date=t0, filename="export-bottles-gone.csv", size=100),
            ]
        )

        snapshot_dir = raw_dir / "260501-1000"
        snapshot_dir.mkdir(parents=True)

        with (
            patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_imap),
            patch("cellarbrain.email_poll.credentials.resolve_credentials", return_value=("u", "p")),
            patch("cellarbrain.email_poll.grouping.group_messages", return_value=[batch]),
            patch("cellarbrain.email_poll.placement.place_batch", return_value=snapshot_dir),
            patch("cellarbrain.email_poll.etl_runner.run_etl", return_value=(0, "OK")),
        ):
            result = poll_once(config, settings)

        # ETL succeeded → messages should be marked
        mock_imap.mark_seen.assert_called_once_with([48, 49, 50])
        assert result == 1
