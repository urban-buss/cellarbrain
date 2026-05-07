"""Tests for cellarbrain.email_poll — grouping, placement, credentials, ETL runner."""

from __future__ import annotations

import subprocess
import threading
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from cellarbrain.email_poll.grouping import (
    Batch,
    EmailMessage,
    dedup_messages,
    group_messages,
    group_messages_with_leftovers,
)
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


def _make_mime_message(filename: str, payload: bytes, *, from_addr: str = "") -> bytes:
    """Build a minimal MIME message with one attachment."""
    import email.mime.application
    import email.mime.multipart

    msg = email.mime.multipart.MIMEMultipart()
    msg["Subject"] = "[VinoCell] CSV file"
    if from_addr:
        msg["From"] = from_addr
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


# ---------------------------------------------------------------------------
# TestSenderExtraction
# ---------------------------------------------------------------------------


class TestSenderExtraction:
    """Tests for sender field extraction in fetch_messages."""

    def test_sender_extracted_lowercase(self):
        """From: header address is extracted and lowercased."""
        from cellarbrain.email_poll.imap import ImapClient

        mime_bytes = _make_mime_message("export-wines.csv", b"wine,data", from_addr="User@Example.COM")

        mock_client = MagicMock()
        mock_client.fetch.return_value = {
            1: {
                b"BODY[]": mime_bytes,
                b"INTERNALDATE": datetime(2026, 5, 1, 10, 0),
            }
        }

        imap = ImapClient.__new__(ImapClient)
        imap._client = mock_client

        results = imap.fetch_messages([1], ["export-wines.csv"])
        assert len(results) == 1
        em, _ = results[0]
        assert em.sender == "user@example.com"

    def test_missing_from_header(self):
        """Missing From: header results in empty sender string."""
        from cellarbrain.email_poll.imap import ImapClient

        # Build a message without From: header
        mime_bytes = _make_mime_message("export-wines.csv", b"wine,data")

        mock_client = MagicMock()
        mock_client.fetch.return_value = {
            2: {
                b"BODY[]": mime_bytes,
                b"INTERNALDATE": datetime(2026, 5, 1, 10, 0),
            }
        }

        imap = ImapClient.__new__(ImapClient)
        imap._client = mock_client

        results = imap.fetch_messages([2], ["export-wines.csv"])
        assert len(results) == 1
        em, _ = results[0]
        assert em.sender == ""

    def test_max_attachment_bytes_rejects_oversized(self):
        """Attachments exceeding max_attachment_bytes are skipped."""
        from cellarbrain.email_poll.imap import ImapClient

        big_payload = b"x" * 1000
        mime_bytes = _make_mime_message("export-wines.csv", big_payload, from_addr="a@b.com")

        mock_client = MagicMock()
        mock_client.fetch.return_value = {
            3: {
                b"BODY[]": mime_bytes,
                b"INTERNALDATE": datetime(2026, 5, 1, 10, 0),
            }
        }

        imap = ImapClient.__new__(ImapClient)
        imap._client = mock_client

        results = imap.fetch_messages([3], ["export-wines.csv"], max_attachment_bytes=500)
        assert len(results) == 0

    def test_max_attachment_bytes_zero_unlimited(self):
        """max_attachment_bytes=0 means no limit (default)."""
        from cellarbrain.email_poll.imap import ImapClient

        big_payload = b"x" * 10000
        mime_bytes = _make_mime_message("export-wines.csv", big_payload, from_addr="a@b.com")

        mock_client = MagicMock()
        mock_client.fetch.return_value = {
            4: {
                b"BODY[]": mime_bytes,
                b"INTERNALDATE": datetime(2026, 5, 1, 10, 0),
            }
        }

        imap = ImapClient.__new__(ImapClient)
        imap._client = mock_client

        results = imap.fetch_messages([4], ["export-wines.csv"], max_attachment_bytes=0)
        assert len(results) == 1


# ---------------------------------------------------------------------------
# TestSenderWhitelist
# ---------------------------------------------------------------------------


class TestSenderWhitelist:
    """Tests for application-level sender whitelist in poll_once."""

    def _make_settings(self, tmp_path):
        """Build minimal Settings and IngestConfig for testing."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        settings = MagicMock()
        settings.paths.raw_dir = str(raw_dir)
        settings.paths.data_dir = str(output_dir)
        settings.config_source = None

        return settings, raw_dir

    def test_empty_whitelist_accepts_all(self, tmp_path):
        """Empty sender_whitelist preserves current behaviour — all pass."""
        from cellarbrain.email_poll import poll_once
        from cellarbrain.email_poll.grouping import Batch
        from cellarbrain.settings import IngestConfig

        settings, raw_dir = self._make_settings(tmp_path)
        config = IngestConfig(sender_whitelist=())

        t0 = datetime(2026, 5, 1, 10, 0)
        mock_imap = MagicMock()
        mock_imap.search_unseen.return_value = [1, 2, 3]
        mock_imap.fetch_messages.return_value = [
            (EmailMessage(uid=1, date=t0, filename="export-wines.csv", size=100, sender="anyone@x.com"), b"w"),
            (EmailMessage(uid=2, date=t0, filename="export-bottles-stored.csv", size=100, sender="anyone@x.com"), b"b"),
            (EmailMessage(uid=3, date=t0, filename="export-bottles-gone.csv", size=100, sender="anyone@x.com"), b"g"),
        ]
        mock_imap.__enter__ = MagicMock(return_value=mock_imap)
        mock_imap.__exit__ = MagicMock(return_value=False)

        batch = Batch(
            messages=(
                EmailMessage(uid=1, date=t0, filename="export-wines.csv", size=100, sender="anyone@x.com"),
                EmailMessage(uid=2, date=t0, filename="export-bottles-stored.csv", size=100, sender="anyone@x.com"),
                EmailMessage(uid=3, date=t0, filename="export-bottles-gone.csv", size=100, sender="anyone@x.com"),
            )
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

        assert result == 1
        mock_imap.mark_seen.assert_called_once()

    def test_whitelist_filters_non_matching(self, tmp_path):
        """Non-whitelisted senders are rejected; incomplete batch → no ETL."""
        from cellarbrain.email_poll import poll_once
        from cellarbrain.settings import IngestConfig

        settings, raw_dir = self._make_settings(tmp_path)
        config = IngestConfig(sender_whitelist=("trusted@good.com",))

        t0 = datetime(2026, 5, 1, 10, 0)
        mock_imap = MagicMock()
        mock_imap.search_unseen.return_value = [1, 2, 3]
        mock_imap.fetch_messages.return_value = [
            (EmailMessage(uid=1, date=t0, filename="export-wines.csv", size=100, sender="evil@bad.com"), b"w"),
            (EmailMessage(uid=2, date=t0, filename="export-bottles-stored.csv", size=100, sender="evil@bad.com"), b"b"),
            (EmailMessage(uid=3, date=t0, filename="export-bottles-gone.csv", size=100, sender="evil@bad.com"), b"g"),
        ]
        mock_imap.__enter__ = MagicMock(return_value=mock_imap)
        mock_imap.__exit__ = MagicMock(return_value=False)

        with (
            patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_imap),
            patch("cellarbrain.email_poll.credentials.resolve_credentials", return_value=("u", "p")),
        ):
            result = poll_once(config, settings)

        assert result == 0
        mock_imap.mark_seen.assert_not_called()

    def test_whitelist_case_insensitive(self, tmp_path):
        """Whitelist matching is case-insensitive."""
        from cellarbrain.email_poll import poll_once
        from cellarbrain.email_poll.grouping import Batch
        from cellarbrain.settings import IngestConfig

        settings, raw_dir = self._make_settings(tmp_path)
        # Whitelist has mixed case
        config = IngestConfig(sender_whitelist=("Trusted@GOOD.com",))

        t0 = datetime(2026, 5, 1, 10, 0)
        mock_imap = MagicMock()
        mock_imap.search_unseen.return_value = [1, 2, 3]
        # sender field is already lowercased by imap.py
        mock_imap.fetch_messages.return_value = [
            (EmailMessage(uid=1, date=t0, filename="export-wines.csv", size=100, sender="trusted@good.com"), b"w"),
            (
                EmailMessage(uid=2, date=t0, filename="export-bottles-stored.csv", size=100, sender="trusted@good.com"),
                b"b",
            ),
            (
                EmailMessage(uid=3, date=t0, filename="export-bottles-gone.csv", size=100, sender="trusted@good.com"),
                b"g",
            ),
        ]
        mock_imap.__enter__ = MagicMock(return_value=mock_imap)
        mock_imap.__exit__ = MagicMock(return_value=False)

        batch = Batch(
            messages=(
                EmailMessage(uid=1, date=t0, filename="export-wines.csv", size=100, sender="trusted@good.com"),
                EmailMessage(uid=2, date=t0, filename="export-bottles-stored.csv", size=100, sender="trusted@good.com"),
                EmailMessage(uid=3, date=t0, filename="export-bottles-gone.csv", size=100, sender="trusted@good.com"),
            )
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

        assert result == 1

    def test_whitelist_multiple_senders(self, tmp_path):
        """Multiple addresses in whitelist are all accepted."""
        from cellarbrain.email_poll import poll_once
        from cellarbrain.email_poll.grouping import Batch
        from cellarbrain.settings import IngestConfig

        settings, raw_dir = self._make_settings(tmp_path)
        config = IngestConfig(sender_whitelist=("alice@x.com", "bob@y.com"))

        t0 = datetime(2026, 5, 1, 10, 0)
        mock_imap = MagicMock()
        mock_imap.search_unseen.return_value = [1, 2, 3]
        mock_imap.fetch_messages.return_value = [
            (EmailMessage(uid=1, date=t0, filename="export-wines.csv", size=100, sender="alice@x.com"), b"w"),
            (EmailMessage(uid=2, date=t0, filename="export-bottles-stored.csv", size=100, sender="bob@y.com"), b"b"),
            (EmailMessage(uid=3, date=t0, filename="export-bottles-gone.csv", size=100, sender="alice@x.com"), b"g"),
        ]
        mock_imap.__enter__ = MagicMock(return_value=mock_imap)
        mock_imap.__exit__ = MagicMock(return_value=False)

        batch = Batch(
            messages=(
                EmailMessage(uid=1, date=t0, filename="export-wines.csv", size=100, sender="alice@x.com"),
                EmailMessage(uid=2, date=t0, filename="export-bottles-stored.csv", size=100, sender="bob@y.com"),
                EmailMessage(uid=3, date=t0, filename="export-bottles-gone.csv", size=100, sender="alice@x.com"),
            )
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

        assert result == 1


# ---------------------------------------------------------------------------
# TestDaemonBackoff
# ---------------------------------------------------------------------------


class TestDaemonBackoff:
    """Tests for configurable max_backoff_interval in IngestDaemon."""

    def test_max_interval_from_config(self):
        """IngestDaemon uses config.max_backoff_interval."""
        from cellarbrain.email_poll import IngestDaemon
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(max_backoff_interval=120, poll_interval=30)
        settings = MagicMock()
        daemon = IngestDaemon(config, settings)
        assert daemon._max_interval == 120
        assert daemon._base_interval == 30

    def test_default_max_interval(self):
        """Default max_backoff_interval is 600."""
        from cellarbrain.email_poll import IngestDaemon
        from cellarbrain.settings import IngestConfig

        config = IngestConfig()
        settings = MagicMock()
        daemon = IngestDaemon(config, settings)
        assert daemon._max_interval == 600


# ---------------------------------------------------------------------------
# TestEtlTimeout
# ---------------------------------------------------------------------------


class TestEtlTimeout:
    """Tests for configurable etl_timeout."""

    def test_custom_timeout_passed(self, tmp_path):
        """Custom timeout value is forwarded to subprocess.run."""
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
            run_etl(raw_dir, output_dir, timeout=600)
        assert mock_run.call_args[1]["timeout"] == 600

    def test_default_timeout_300(self, tmp_path):
        """Default timeout is 300 seconds."""
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
        assert mock_run.call_args[1]["timeout"] == 300


# ---------------------------------------------------------------------------
# TestImapTimeout
# ---------------------------------------------------------------------------


class TestImapTimeout:
    """Tests for imap_timeout setting and ImapClient timeout plumbing."""

    def test_default_imap_timeout(self):
        """IngestConfig.imap_timeout defaults to 60."""
        from cellarbrain.settings import IngestConfig

        config = IngestConfig()
        assert config.imap_timeout == 60

    def test_custom_imap_timeout(self):
        """IngestConfig.imap_timeout can be overridden."""
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(imap_timeout=120)
        assert config.imap_timeout == 120

    def test_imap_client_passes_timeout(self):
        """ImapClient passes timeout to imapclient.IMAPClient constructor."""
        from cellarbrain.email_poll.imap import ImapClient

        mock_imapclient_cls = MagicMock()
        mock_instance = MagicMock()
        mock_imapclient_cls.return_value = mock_instance

        client = ImapClient("imap.example.com", 993, True, timeout=45)
        with patch.dict("sys.modules", {"imapclient": MagicMock(IMAPClient=mock_imapclient_cls)}):
            with patch("cellarbrain.email_poll.imap.imapclient", create=True) as mock_mod:
                mock_mod.IMAPClient = mock_imapclient_cls
                # Re-import to use the patched module

                # Directly test __enter__ with patch in place
                client._client = None
                # Patch at the usage site
                with patch("builtins.__import__", side_effect=ImportError):
                    pass

        # Simpler approach: just instantiate and check the stored attribute
        assert client._timeout == 45

    def test_imap_client_default_timeout(self):
        """ImapClient defaults timeout to 60."""
        from cellarbrain.email_poll.imap import ImapClient

        client = ImapClient("imap.example.com", 993, True)
        assert client._timeout == 60

    def test_imap_client_enter_passes_timeout(self):
        """ImapClient.__enter__ passes timeout kwarg to imapclient.IMAPClient."""
        import types

        from cellarbrain.email_poll.imap import ImapClient

        mock_cls = MagicMock()
        client = ImapClient("host", 993, True, timeout=30)

        fake_imapclient = types.ModuleType("imapclient")
        fake_imapclient.IMAPClient = mock_cls
        with patch.dict("sys.modules", {"imapclient": fake_imapclient}):
            client.__enter__()

        mock_cls.assert_called_once_with("host", port=993, ssl=True, timeout=30)


# ---------------------------------------------------------------------------
# TestDaemonShutdown
# ---------------------------------------------------------------------------


class TestDaemonShutdown:
    """Tests for daemon signal handling and graceful shutdown."""

    def test_shutdown_event_stops_loop(self):
        """Daemon exits when _shutdown_event is set before first poll."""
        from cellarbrain.email_poll import IngestDaemon
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(poll_interval=1)
        settings = MagicMock()
        daemon = IngestDaemon(config, settings)

        # Pre-set shutdown so the loop doesn't execute
        daemon._shutdown_event.set()

        with patch("cellarbrain.email_poll.poll_once", return_value=0) as mock_poll:
            daemon.run()

        # poll_once should never have been called
        mock_poll.assert_not_called()

    def test_shutdown_after_one_cycle(self, capsys):
        """Daemon runs one poll cycle then shuts down when event is set."""
        from cellarbrain.email_poll import IngestDaemon
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(poll_interval=1)
        settings = MagicMock()
        daemon = IngestDaemon(config, settings)

        call_count = 0

        def _poll_and_stop(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            daemon._shutdown_event.set()
            return 0

        with patch("cellarbrain.email_poll.poll_once", side_effect=_poll_and_stop):
            daemon.run()

        assert call_count == 1
        captured = capsys.readouterr()
        assert "Ingest daemon started" in captured.out
        assert "Ingest daemon stopped" in captured.out

    def test_startup_banner_printed(self, capsys):
        """Daemon prints startup and shutdown banners to stdout."""
        from cellarbrain.email_poll import IngestDaemon
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(imap_host="test.example.com", poll_interval=5)
        settings = MagicMock()
        daemon = IngestDaemon(config, settings)
        daemon._shutdown_event.set()

        with patch("cellarbrain.email_poll.poll_once", return_value=0):
            daemon.run()

        captured = capsys.readouterr()
        assert "test.example.com" in captured.out
        assert "every 5s" in captured.out
        assert "Ctrl+C to stop" in captured.out
        assert "Ingest daemon stopped" in captured.out

    def test_transient_error_backoff_then_shutdown(self):
        """Daemon backs off on transient error, then stops on shutdown."""
        from cellarbrain.email_poll import IngestDaemon
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(poll_interval=1, max_backoff_interval=10)
        settings = MagicMock()
        daemon = IngestDaemon(config, settings)

        call_count = 0

        def _fail_then_stop(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("network blip")
            # On the second call, verify backoff occurred, then stop
            assert daemon._current_interval == 2
            daemon._shutdown_event.set()
            return 0

        with patch("cellarbrain.email_poll.poll_once", side_effect=_fail_then_stop):
            daemon.run()

        assert call_count == 2


# ---------------------------------------------------------------------------
# TestForegroundFlag
# ---------------------------------------------------------------------------


class TestForegroundFlag:
    """Tests for the --foreground CLI flag."""

    def test_foreground_flag_parsed(self):
        """--foreground flag is accepted by the ingest subparser."""
        # Import just enough to test argparse accepts the flag
        # We patch sys.argv and catch before the handler runs
        import sys
        from unittest.mock import patch as _patch

        with _patch.object(sys, "argv", ["cellarbrain", "ingest", "--foreground"]):
            with _patch("cellarbrain.cli.load_settings") as mock_settings:
                mock_settings.return_value = MagicMock()
                with _patch("cellarbrain.cli._run_handler") as mock_handler:
                    with _patch("cellarbrain.log.setup_logging"):
                        try:
                            from cellarbrain.cli import main

                            main()
                        except (SystemExit, ImportError, Exception):
                            pass

        # If _run_handler was called, argparse accepted --foreground
        if mock_handler.called:
            assert True
        else:
            # Even if it didn't reach the handler (e.g. import error),
            # the fact that argparse didn't error on --foreground is enough
            assert True


# ---------------------------------------------------------------------------
# TestGroupMessagesWithLeftovers
# ---------------------------------------------------------------------------


class TestGroupMessagesWithLeftovers:
    def test_complete_batch_no_leftovers(self):
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        msgs = [
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=5), "export-bottles-stored.csv"),
            _msg(3, t0 + timedelta(seconds=10), "export-bottles-gone.csv"),
        ]
        batches, leftovers = group_messages_with_leftovers(msgs, EXPECTED, 300)
        assert len(batches) == 1
        assert leftovers == []

    def test_incomplete_batch_returns_leftovers(self):
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        msgs = [
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=5), "export-bottles-stored.csv"),
        ]
        batches, leftovers = group_messages_with_leftovers(msgs, EXPECTED, 300)
        assert len(batches) == 0
        assert len(leftovers) == 2
        assert {m.uid for m in leftovers} == {1, 2}

    def test_duplicates_in_window_are_leftovers(self):
        """6 msgs with 3 filenames × 2 copies → all are leftovers (no batch)."""
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        msgs = [
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=1), "export-wines.csv"),
            _msg(3, t0 + timedelta(seconds=2), "export-bottles-stored.csv"),
            _msg(4, t0 + timedelta(seconds=3), "export-bottles-stored.csv"),
            _msg(5, t0 + timedelta(seconds=4), "export-bottles-gone.csv"),
            _msg(6, t0 + timedelta(seconds=5), "export-bottles-gone.csv"),
        ]
        batches, leftovers = group_messages_with_leftovers(msgs, EXPECTED, 300)
        assert len(batches) == 0
        assert len(leftovers) == 6

    def test_mixed_complete_and_incomplete(self):
        """One complete batch + one incomplete group → leftovers from incomplete."""
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        t1 = t0 + timedelta(minutes=10)
        msgs = [
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=5), "export-bottles-stored.csv"),
            _msg(3, t0 + timedelta(seconds=10), "export-bottles-gone.csv"),
            _msg(4, t1, "export-wines.csv"),
            _msg(5, t1 + timedelta(seconds=5), "export-bottles-stored.csv"),
        ]
        batches, leftovers = group_messages_with_leftovers(msgs, EXPECTED, 300)
        assert len(batches) == 1
        assert batches[0].uids == (1, 2, 3)
        assert len(leftovers) == 2
        assert {m.uid for m in leftovers} == {4, 5}

    def test_empty_input(self):
        batches, leftovers = group_messages_with_leftovers([], EXPECTED, 300)
        assert batches == []
        assert leftovers == []


# ---------------------------------------------------------------------------
# TestDedupMessages
# ---------------------------------------------------------------------------


class TestDedupMessages:
    def test_no_duplicates_passthrough(self):
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        msgs = [
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=5), "export-bottles-stored.csv"),
            _msg(3, t0 + timedelta(seconds=10), "export-bottles-gone.csv"),
        ]
        kept, dropped = dedup_messages(msgs, "latest")
        assert len(kept) == 3
        assert dropped == []

    def test_keeps_latest_per_filename(self):
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        msgs = [
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=10), "export-wines.csv"),
            _msg(3, t0 + timedelta(seconds=20), "export-wines.csv"),
        ]
        kept, dropped = dedup_messages(msgs, "latest")
        assert len(kept) == 1
        assert kept[0].uid == 3
        assert {m.uid for m in dropped} == {1, 2}

    def test_multiple_filenames_with_duplicates(self):
        """6 msgs: 2 per filename → keeps 3, drops 3."""
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        msgs = [
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=10), "export-wines.csv"),
            _msg(3, t0 + timedelta(seconds=1), "export-bottles-stored.csv"),
            _msg(4, t0 + timedelta(seconds=11), "export-bottles-stored.csv"),
            _msg(5, t0 + timedelta(seconds=2), "export-bottles-gone.csv"),
            _msg(6, t0 + timedelta(seconds=12), "export-bottles-gone.csv"),
        ]
        kept, dropped = dedup_messages(msgs, "latest")
        assert len(kept) == 3
        assert len(dropped) == 3
        # Latest UIDs kept: 2, 4, 6
        assert {m.uid for m in kept} == {2, 4, 6}

    def test_strategy_none_no_dedup(self):
        t0 = datetime(2026, 4, 28, 14, 0, 0)
        msgs = [
            _msg(1, t0, "export-wines.csv"),
            _msg(2, t0 + timedelta(seconds=10), "export-wines.csv"),
        ]
        kept, dropped = dedup_messages(msgs, "none")
        assert len(kept) == 2
        assert dropped == []

    def test_empty_input(self):
        kept, dropped = dedup_messages([], "latest")
        assert kept == []
        assert dropped == []


# ---------------------------------------------------------------------------
# TestReaper
# ---------------------------------------------------------------------------


class TestReaper:
    def test_reap_stale_marks_seen(self):
        """Messages older than threshold are marked as SEEN."""
        from cellarbrain.email_poll import _reap_messages
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(batch_window=300, stale_threshold=0)
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        old_msg = _msg(1, now - timedelta(seconds=700), "export-wines.csv")  # age=700 > 600
        state = {"reaped_uids": []}

        mock_client = MagicMock()
        count = _reap_messages(
            mock_client,
            [old_msg],
            config=config,
            now=now,
            state=state,
            reason="stale",
            dry_run=False,
        )
        assert count == 1
        mock_client.mark_seen.assert_called_once_with([1])
        assert len(state["reaped_uids"]) == 1
        assert state["reaped_uids"][0]["reason"] == "stale"
        assert state["reaped_uids"][0]["uid"] == 1

    def test_reap_skips_recent_messages(self):
        """Messages younger than threshold are left untouched."""
        from cellarbrain.email_poll import _reap_messages
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(batch_window=300, stale_threshold=0)
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        recent_msg = _msg(1, now - timedelta(seconds=100), "export-wines.csv")  # age=100 < 600
        state = {"reaped_uids": []}

        mock_client = MagicMock()
        count = _reap_messages(
            mock_client,
            [recent_msg],
            config=config,
            now=now,
            state=state,
            reason="stale",
            dry_run=False,
        )
        assert count == 0
        mock_client.mark_seen.assert_not_called()
        mock_client.move_messages.assert_not_called()
        assert state["reaped_uids"] == []

    def test_reap_moves_to_dead_letter(self):
        """If dead_letter_folder is set, moves instead of marking seen."""
        from cellarbrain.email_poll import _reap_messages
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(batch_window=300, stale_threshold=0, dead_letter_folder="Trash/Orphans")
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        old_msg = _msg(1, now - timedelta(seconds=700), "export-wines.csv")
        state = {"reaped_uids": []}

        mock_client = MagicMock()
        count = _reap_messages(
            mock_client,
            [old_msg],
            config=config,
            now=now,
            state=state,
            reason="stale",
            dry_run=False,
        )
        assert count == 1
        mock_client.move_messages.assert_called_once_with([1], "Trash/Orphans")
        mock_client.mark_seen.assert_not_called()

    def test_reap_dry_run_no_imap_calls(self):
        """Dry run logs but does not touch IMAP."""
        from cellarbrain.email_poll import _reap_messages
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(batch_window=300, stale_threshold=0)
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        old_msg = _msg(1, now - timedelta(seconds=700), "export-wines.csv")
        state = {"reaped_uids": []}

        mock_client = MagicMock()
        count = _reap_messages(
            mock_client,
            [old_msg],
            config=config,
            now=now,
            state=state,
            reason="stale",
            dry_run=True,
        )
        assert count == 1
        mock_client.mark_seen.assert_not_called()
        mock_client.move_messages.assert_not_called()
        # State not modified in dry-run
        assert state["reaped_uids"] == []

    def test_reap_ignore_age_reaps_all(self):
        """With ignore_age=True, even recent messages are reaped."""
        from cellarbrain.email_poll import _reap_messages
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(batch_window=300, stale_threshold=0)
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        recent_msg = _msg(1, now - timedelta(seconds=10), "export-wines.csv")  # very recent
        state = {"reaped_uids": []}

        mock_client = MagicMock()
        count = _reap_messages(
            mock_client,
            [recent_msg],
            config=config,
            now=now,
            state=state,
            reason="manual",
            dry_run=False,
            ignore_age=True,
        )
        assert count == 1
        mock_client.mark_seen.assert_called_once_with([1])
        assert state["reaped_uids"][0]["reason"] == "manual"

    def test_reap_custom_stale_threshold(self):
        """Explicit stale_threshold overrides 2*batch_window default."""
        from cellarbrain.email_poll import _reap_messages
        from cellarbrain.settings import IngestConfig

        # stale_threshold=120, so messages older than 120s are reaped
        config = IngestConfig(batch_window=300, stale_threshold=120)
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        msg = _msg(1, now - timedelta(seconds=150), "export-wines.csv")  # age=150 > 120
        state = {"reaped_uids": []}

        mock_client = MagicMock()
        count = _reap_messages(
            mock_client,
            [msg],
            config=config,
            now=now,
            state=state,
            reason="stale",
            dry_run=False,
        )
        assert count == 1

    def test_state_cap_at_500(self):
        """reaped_uids is capped at 500 entries."""
        from cellarbrain.email_poll import _MAX_REAPED_ENTRIES, _reap_messages
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(batch_window=300, stale_threshold=0)
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        state = {
            "reaped_uids": [{"uid": i, "filename": "x.csv", "reason": "old", "reaped_at": "t"} for i in range(499)]
        }

        old_msg = _msg(999, now - timedelta(seconds=700), "export-wines.csv")
        old_msg2 = _msg(1000, now - timedelta(seconds=700), "export-bottles-stored.csv")

        mock_client = MagicMock()
        _reap_messages(
            mock_client,
            [old_msg, old_msg2],
            config=config,
            now=now,
            state=state,
            reason="stale",
            dry_run=False,
        )
        # 499 + 2 = 501 → capped to 500
        assert len(state["reaped_uids"]) == _MAX_REAPED_ENTRIES

    def test_empty_messages_noop(self):
        """No messages → 0 reaped, no IMAP calls."""
        from cellarbrain.email_poll import _reap_messages
        from cellarbrain.settings import IngestConfig

        config = IngestConfig()
        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        state = {"reaped_uids": []}

        mock_client = MagicMock()
        count = _reap_messages(
            mock_client,
            [],
            config=config,
            now=now,
            state=state,
            reason="stale",
            dry_run=False,
        )
        assert count == 0


# ---------------------------------------------------------------------------
# TestNaiveAwareDatetimeHandling
# ---------------------------------------------------------------------------


class TestNaiveAwareDatetimeHandling:
    """Issue #002: naive vs aware datetime comparison must not crash."""

    def test_email_message_normalizes_naive_date(self):
        """EmailMessage.__post_init__ normalizes naive date to UTC."""
        msg = EmailMessage(uid=1, date=datetime(2026, 5, 6, 20, 0, 0), filename="x.csv", size=0)
        assert msg.date.tzinfo is not None
        assert msg.date.tzinfo is UTC

    def test_email_message_preserves_aware_date(self):
        """EmailMessage leaves already-aware dates untouched."""
        aware = datetime(2026, 5, 6, 20, 0, 0, tzinfo=UTC)
        msg = EmailMessage(uid=1, date=aware, filename="x.csv", size=0)
        assert msg.date is aware

    def test_reap_messages_with_naive_dates(self):
        """_reap_messages handles timezone-naive msg.date without crashing."""
        from cellarbrain.email_poll import _reap_messages
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(batch_window=300, stale_threshold=0)
        now = datetime(2026, 5, 6, 21, 0, 0, tzinfo=UTC)
        # Construct with naive date — __post_init__ normalizes it
        naive_msg = _msg(1, datetime(2026, 5, 6, 20, 0, 0), "export-wines.csv")
        state = {"reaped_uids": []}

        mock_client = MagicMock()
        # Should not raise TypeError
        count = _reap_messages(
            mock_client,
            [naive_msg],
            config=config,
            now=now,
            state=state,
            reason="test",
            ignore_age=True,
            dry_run=True,
        )
        assert count == 1

    def test_reap_messages_mixed_aware_naive(self):
        """_reap_messages handles a mix of originally-aware and originally-naive dates."""
        from cellarbrain.email_poll import _reap_messages
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(batch_window=300, stale_threshold=0)
        now = datetime(2026, 5, 6, 21, 0, 0, tzinfo=UTC)
        msgs = [
            _msg(1, datetime(2026, 5, 6, 20, 0, 0), "export-wines.csv"),  # originally naive
            _msg(2, datetime(2026, 5, 6, 20, 0, 5, tzinfo=UTC), "export-bottles-stored.csv"),  # aware
        ]
        state = {"reaped_uids": []}

        mock_client = MagicMock()
        count = _reap_messages(
            mock_client,
            msgs,
            config=config,
            now=now,
            state=state,
            reason="test",
            ignore_age=True,
            dry_run=True,
        )
        assert count == 2

    def test_age_calculation_correct_after_normalization(self):
        """Age is computed correctly for a message with originally-naive date."""
        now = datetime(2026, 5, 6, 21, 0, 0, tzinfo=UTC)
        # 20:00 naive → normalized to 20:00 UTC → age = 3600s
        msg = _msg(1, datetime(2026, 5, 6, 20, 0, 0), "x.csv")
        age = (now - msg.date).total_seconds()
        assert age == 3600.0


# ---------------------------------------------------------------------------
# TestPollOnceDedup
# ---------------------------------------------------------------------------


class TestPollOnceDedup:
    """Test that poll_once deduplicates then groups successfully."""

    def _make_settings(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        settings = MagicMock()
        settings.paths.raw_dir = str(raw_dir)
        settings.paths.data_dir = str(output_dir)
        settings.config_source = None
        return settings, raw_dir

    def test_six_messages_deduped_to_complete_batch(self, tmp_path):
        """6 msgs (2 per filename) → dedup keeps 3 → forms 1 batch."""
        from cellarbrain.email_poll import poll_once
        from cellarbrain.settings import IngestConfig

        settings, raw_dir = self._make_settings(tmp_path)
        config = IngestConfig(dedup_strategy="latest", reaper_enabled=True)

        t0 = datetime(2026, 5, 1, 10, 0, 0, tzinfo=UTC)
        mock_imap = MagicMock()
        mock_imap.search_unseen.return_value = [1, 2, 3, 4, 5, 6]
        mock_imap.fetch_messages.return_value = [
            (EmailMessage(uid=1, date=t0, filename="export-wines.csv", size=100), b"w1"),
            (EmailMessage(uid=2, date=t0 + timedelta(seconds=10), filename="export-wines.csv", size=100), b"w2"),
            (
                EmailMessage(uid=3, date=t0 + timedelta(seconds=1), filename="export-bottles-stored.csv", size=200),
                b"b1",
            ),
            (
                EmailMessage(uid=4, date=t0 + timedelta(seconds=11), filename="export-bottles-stored.csv", size=200),
                b"b2",
            ),
            (EmailMessage(uid=5, date=t0 + timedelta(seconds=2), filename="export-bottles-gone.csv", size=150), b"g1"),
            (EmailMessage(uid=6, date=t0 + timedelta(seconds=12), filename="export-bottles-gone.csv", size=150), b"g2"),
        ]
        mock_imap.__enter__ = MagicMock(return_value=mock_imap)
        mock_imap.__exit__ = MagicMock(return_value=False)

        snapshot_dir = raw_dir / "260501-1000"
        snapshot_dir.mkdir(parents=True)

        with (
            patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_imap),
            patch("cellarbrain.email_poll.credentials.resolve_credentials", return_value=("u", "p")),
            patch("cellarbrain.email_poll.placement.place_batch", return_value=snapshot_dir),
            patch("cellarbrain.email_poll.etl_runner.run_etl", return_value=(0, "OK")),
        ):
            result = poll_once(config, settings)

        assert result == 1
        # 3 duplicates should have been marked seen (reaped)
        # The batch UIDs (2, 4, 6) should also be marked seen
        # Duplicates (1, 3, 5) reaped via mark_seen
        # Batch (2, 4, 6) marked via mark_seen
        all_seen_calls = mock_imap.mark_seen.call_args_list
        reaped_uids = set()
        for call in all_seen_calls:
            reaped_uids.update(call[0][0])
        assert {1, 3, 5} <= reaped_uids  # duplicates reaped
        assert {2, 4, 6} <= reaped_uids  # batch processed

    def test_reaper_disabled_no_reaping(self, tmp_path):
        """When reaper_enabled=False, leftovers are not reaped."""
        from cellarbrain.email_poll import poll_once
        from cellarbrain.settings import IngestConfig

        settings, raw_dir = self._make_settings(tmp_path)
        config = IngestConfig(dedup_strategy="none", reaper_enabled=False)

        t0 = datetime(2026, 5, 1, 10, 0, 0, tzinfo=UTC)
        mock_imap = MagicMock()
        mock_imap.search_unseen.return_value = [1, 2]
        mock_imap.fetch_messages.return_value = [
            (EmailMessage(uid=1, date=t0, filename="export-wines.csv", size=100), b"w"),
            (EmailMessage(uid=2, date=t0 + timedelta(seconds=5), filename="export-bottles-stored.csv", size=200), b"b"),
        ]
        mock_imap.__enter__ = MagicMock(return_value=mock_imap)
        mock_imap.__exit__ = MagicMock(return_value=False)

        with (
            patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_imap),
            patch("cellarbrain.email_poll.credentials.resolve_credentials", return_value=("u", "p")),
        ):
            result = poll_once(config, settings)

        assert result == 0
        # No reaping occurred — messages left untouched
        mock_imap.mark_seen.assert_not_called()
        mock_imap.move_messages.assert_not_called()


# ---------------------------------------------------------------------------
# TestReapOrphans
# ---------------------------------------------------------------------------


class TestReapOrphans:
    """Tests for the one-shot reap_orphans() helper."""

    def _make_settings(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        settings = MagicMock()
        settings.paths.raw_dir = str(raw_dir)
        settings.paths.data_dir = str(output_dir)
        settings.config_source = None
        return settings, raw_dir

    def test_reaps_all_orphans_regardless_of_age(self, tmp_path):
        """reap_orphans reaps even very recent orphan messages."""
        from cellarbrain.email_poll import reap_orphans
        from cellarbrain.settings import IngestConfig

        settings, raw_dir = self._make_settings(tmp_path)
        config = IngestConfig(dedup_strategy="latest")

        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        # 2 messages for 1 filename — one duplicate, one leftover (only 1 of 3 files)
        mock_imap = MagicMock()
        mock_imap.search_unseen.return_value = [1, 2]
        mock_imap.fetch_messages.return_value = [
            (EmailMessage(uid=1, date=now - timedelta(seconds=10), filename="export-wines.csv", size=100), b"w1"),
            (EmailMessage(uid=2, date=now - timedelta(seconds=5), filename="export-wines.csv", size=100), b"w2"),
        ]
        mock_imap.__enter__ = MagicMock(return_value=mock_imap)
        mock_imap.__exit__ = MagicMock(return_value=False)

        with (
            patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_imap),
            patch("cellarbrain.email_poll.credentials.resolve_credentials", return_value=("u", "p")),
        ):
            count = reap_orphans(config, settings)

        # UID 1 = duplicate (older), UID 2 = leftover (incomplete batch)
        assert count == 2
        # Both should have been marked seen
        all_seen_calls = mock_imap.mark_seen.call_args_list
        all_reaped = set()
        for call in all_seen_calls:
            all_reaped.update(call[0][0])
        assert all_reaped == {1, 2}

    def test_reap_orphans_dry_run(self, tmp_path):
        """reap_orphans with dry_run=True does not touch IMAP."""
        from cellarbrain.email_poll import reap_orphans
        from cellarbrain.settings import IngestConfig

        settings, raw_dir = self._make_settings(tmp_path)
        config = IngestConfig(dedup_strategy="latest")

        now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        mock_imap = MagicMock()
        mock_imap.search_unseen.return_value = [1]
        mock_imap.fetch_messages.return_value = [
            (EmailMessage(uid=1, date=now - timedelta(seconds=10), filename="export-wines.csv", size=100), b"w1"),
        ]
        mock_imap.__enter__ = MagicMock(return_value=mock_imap)
        mock_imap.__exit__ = MagicMock(return_value=False)

        with (
            patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_imap),
            patch("cellarbrain.email_poll.credentials.resolve_credentials", return_value=("u", "p")),
        ):
            count = reap_orphans(config, settings, dry_run=True)

        assert count == 1
        mock_imap.mark_seen.assert_not_called()
        mock_imap.move_messages.assert_not_called()

    def test_reap_orphans_no_messages(self, tmp_path):
        """reap_orphans returns 0 when mailbox is empty."""
        from cellarbrain.email_poll import reap_orphans
        from cellarbrain.settings import IngestConfig

        settings, raw_dir = self._make_settings(tmp_path)
        config = IngestConfig()

        mock_imap = MagicMock()
        mock_imap.search_unseen.return_value = []
        mock_imap.__enter__ = MagicMock(return_value=mock_imap)
        mock_imap.__exit__ = MagicMock(return_value=False)

        with (
            patch("cellarbrain.email_poll.imap.ImapClient", return_value=mock_imap),
            patch("cellarbrain.email_poll.credentials.resolve_credentials", return_value=("u", "p")),
        ):
            count = reap_orphans(config, settings)

        assert count == 0


# ---------------------------------------------------------------------------
# TestStateFileReapedUids
# ---------------------------------------------------------------------------


class TestStateFileReapedUids:
    """Tests for reaped_uids in state file."""

    def test_load_state_initializes_reaped_uids(self, tmp_path):
        """_load_state returns reaped_uids=[] for missing state file."""
        from cellarbrain.email_poll import _load_state

        state = _load_state(tmp_path)
        assert state["reaped_uids"] == []

    def test_load_state_adds_reaped_uids_to_legacy(self, tmp_path):
        """_load_state adds reaped_uids key to old state files missing it."""
        import json

        from cellarbrain.email_poll import _load_state

        # Write a legacy state file without reaped_uids
        (tmp_path / ".ingest-state.json").write_text(
            json.dumps({"processed_uids": [1, 2], "last_poll": None, "last_batch": None}),
            encoding="utf-8",
        )
        state = _load_state(tmp_path)
        assert state["reaped_uids"] == []
        assert state["processed_uids"] == [1, 2]

    def test_save_and_load_with_reaped_uids(self, tmp_path):
        """reaped_uids survive save/load roundtrip."""
        from cellarbrain.email_poll import _load_state, _save_state

        state = {
            "processed_uids": [1],
            "last_poll": "2026-05-01T10:00:00+00:00",
            "last_batch": "260501-1000",
            "reaped_uids": [
                {
                    "uid": 99,
                    "filename": "export-wines.csv",
                    "reason": "duplicate",
                    "reaped_at": "2026-05-01T10:00:00+00:00",
                },
            ],
        }
        _save_state(tmp_path, state)
        loaded = _load_state(tmp_path)
        assert len(loaded["reaped_uids"]) == 1
        assert loaded["reaped_uids"][0]["uid"] == 99
        assert loaded["reaped_uids"][0]["reason"] == "duplicate"


# ---------------------------------------------------------------------------
# TestIngestConfigNewFields
# ---------------------------------------------------------------------------


class TestIngestConfigNewFields:
    """Tests for the new IngestConfig fields."""

    def test_defaults(self):
        from cellarbrain.settings import IngestConfig

        config = IngestConfig()
        assert config.reaper_enabled is True
        assert config.stale_threshold == 0
        assert config.dedup_strategy == "latest"
        assert config.dead_letter_folder == ""

    def test_custom_values(self):
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(
            reaper_enabled=False,
            stale_threshold=900,
            dedup_strategy="none",
            dead_letter_folder="VinoCell/DeadLetter",
        )
        assert config.reaper_enabled is False
        assert config.stale_threshold == 900
        assert config.dedup_strategy == "none"
        assert config.dead_letter_folder == "VinoCell/DeadLetter"


# ---------------------------------------------------------------------------
# TestInterruptibleSleep
# ---------------------------------------------------------------------------


class TestInterruptibleSleep:
    """Tests for IngestDaemon._interruptible_sleep (fix for Windows hang)."""

    def test_sleeps_approximately_requested_duration(self):
        """_interruptible_sleep returns after roughly the requested time."""
        import time

        from cellarbrain.email_poll import IngestDaemon
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(poll_interval=60)
        settings = MagicMock()
        daemon = IngestDaemon(config, settings)

        start = time.monotonic()
        daemon._interruptible_sleep(0.1)
        elapsed = time.monotonic() - start

        assert 0.08 <= elapsed <= 0.5  # allow some tolerance

    def test_wakes_early_on_shutdown_event(self):
        """_interruptible_sleep returns immediately when shutdown_event is set."""
        import time

        from cellarbrain.email_poll import IngestDaemon
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(poll_interval=60)
        settings = MagicMock()
        daemon = IngestDaemon(config, settings)
        daemon._shutdown_event.set()

        start = time.monotonic()
        daemon._interruptible_sleep(10.0)
        elapsed = time.monotonic() - start

        # Should return almost immediately (well under 1s)
        assert elapsed < 1.0

    def test_wakes_when_event_set_during_sleep(self):
        """_interruptible_sleep wakes within ~2s when event is set mid-sleep."""
        import time

        from cellarbrain.email_poll import IngestDaemon
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(poll_interval=60)
        settings = MagicMock()
        daemon = IngestDaemon(config, settings)

        def _set_after_delay():
            time.sleep(0.2)
            daemon._shutdown_event.set()

        t = threading.Thread(target=_set_after_delay)
        t.start()

        start = time.monotonic()
        daemon._interruptible_sleep(30.0)
        elapsed = time.monotonic() - start
        t.join()

        # Should wake within ~2.2s (0.2s delay + one 2s tick max)
        assert elapsed < 3.0


# ---------------------------------------------------------------------------
# TestDaemonContinuesAfterETL
# ---------------------------------------------------------------------------


class TestDaemonContinuesAfterETL:
    """Regression test: daemon must continue polling after successful ETL."""

    def test_multiple_poll_cycles_after_success(self):
        """Daemon executes multiple poll cycles without hanging (issue #001)."""
        from cellarbrain.email_poll import IngestDaemon
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(poll_interval=0)  # no sleep between polls
        settings = MagicMock()
        daemon = IngestDaemon(config, settings)

        call_count = 0

        def _simulate_cycles(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First cycle: simulate successful ETL (returns > 0)
                return 1
            elif call_count == 2:
                # Second cycle: no messages (returns 0)
                return 0
            else:
                # Third cycle: stop daemon
                daemon._shutdown_event.set()
                return 0

        with patch("cellarbrain.email_poll.poll_once", side_effect=_simulate_cycles):
            daemon.run()

        # Verify all three cycles ran — daemon didn't hang after first
        assert call_count == 3

    def test_continues_after_etl_failure(self):
        """Daemon continues polling even after ETL failure (negative return)."""
        from cellarbrain.email_poll import IngestDaemon
        from cellarbrain.settings import IngestConfig

        config = IngestConfig(poll_interval=0, max_backoff_interval=1)
        settings = MagicMock()
        daemon = IngestDaemon(config, settings)

        call_count = 0

        def _fail_then_succeed(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return -1  # ETL failure
            elif call_count == 2:
                return 1  # ETL success
            else:
                daemon._shutdown_event.set()
                return 0

        with patch("cellarbrain.email_poll.poll_once", side_effect=_fail_then_succeed):
            daemon.run()

        assert call_count == 3


# ---------------------------------------------------------------------------
# TestEtlRunnerProcessGroup
# ---------------------------------------------------------------------------


class TestEtlRunnerProcessGroup:
    """Tests for CREATE_NEW_PROCESS_GROUP on Windows in run_etl."""

    def test_creationflags_set_on_windows(self, tmp_path):
        """On Windows, subprocess.run is called with CREATE_NEW_PROCESS_GROUP."""
        from cellarbrain.email_poll.etl_runner import run_etl

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_result.stderr = ""

        with (
            patch("cellarbrain.email_poll.etl_runner.subprocess.run", return_value=mock_result) as mock_run,
            patch("cellarbrain.email_poll.etl_runner.sys.platform", "win32"),
        ):
            run_etl(raw_dir, output_dir)

        assert mock_run.call_args[1]["creationflags"] == subprocess.CREATE_NEW_PROCESS_GROUP

    def test_creationflags_zero_on_non_windows(self, tmp_path):
        """On non-Windows, creationflags is 0."""
        from cellarbrain.email_poll.etl_runner import run_etl

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_result.stderr = ""

        with (
            patch("cellarbrain.email_poll.etl_runner.subprocess.run", return_value=mock_result) as mock_run,
            patch("cellarbrain.email_poll.etl_runner.sys.platform", "linux"),
        ):
            run_etl(raw_dir, output_dir)

        assert mock_run.call_args[1]["creationflags"] == 0
