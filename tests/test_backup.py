"""Tests for the backup and restore module."""

from __future__ import annotations

import time
import zipfile

import pytest

from cellarbrain.backup import (
    _prune_backups,
    create_backup,
    list_backups,
    restore_backup,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_data_dir(tmp_path):
    """Create a minimal data directory with Parquet stubs and dossiers."""
    data = tmp_path / "output"
    data.mkdir()
    # Parquet stubs
    (data / "wine.parquet").write_bytes(b"PAR1fake-wine-data")
    (data / "bottle.parquet").write_bytes(b"PAR1fake-bottle-data")
    # JSON config
    (data / "currency-rates.json").write_text('{"EUR": 0.93}')
    # Dossiers
    cellar = data / "wines" / "cellar"
    cellar.mkdir(parents=True)
    (cellar / "0001-test-wine.md").write_text("# Test Wine\n")
    archive = data / "wines" / "archive"
    archive.mkdir(parents=True)
    (archive / "0002-old-wine.md").write_text("# Old Wine\n")
    # Excluded dirs
    sommelier = data / "sommelier"
    sommelier.mkdir()
    (sommelier / "wine.index").write_bytes(b"FAISS-INDEX")
    logs = data / "logs"
    logs.mkdir()
    (logs / "cellarbrain-logs.duckdb").write_bytes(b"DUCKDB-FAKE")
    return data


# ---------------------------------------------------------------------------
# TestCreateBackup
# ---------------------------------------------------------------------------


class TestCreateBackup:
    """Test backup creation."""

    def test_creates_zip_with_parquet_files(self, tmp_path):
        data = _setup_data_dir(tmp_path)
        bkp_dir = tmp_path / "bkp"

        path = create_backup(data, bkp_dir)

        assert path.exists()
        assert path.suffix == ".zip"
        with zipfile.ZipFile(path, "r") as zf:
            names = zf.namelist()
        assert "wine.parquet" in names
        assert "bottle.parquet" in names

    def test_creates_zip_with_dossiers(self, tmp_path):
        data = _setup_data_dir(tmp_path)
        bkp_dir = tmp_path / "bkp"

        path = create_backup(data, bkp_dir)

        with zipfile.ZipFile(path, "r") as zf:
            names = zf.namelist()
        # Use forward slashes as zipfile normalises paths
        assert any("0001-test-wine.md" in n for n in names)
        assert any("0002-old-wine.md" in n for n in names)

    def test_creates_zip_with_json_config(self, tmp_path):
        data = _setup_data_dir(tmp_path)
        bkp_dir = tmp_path / "bkp"

        path = create_backup(data, bkp_dir)

        with zipfile.ZipFile(path, "r") as zf:
            names = zf.namelist()
        assert "currency-rates.json" in names

    def test_excludes_sommelier_by_default(self, tmp_path):
        data = _setup_data_dir(tmp_path)
        bkp_dir = tmp_path / "bkp"

        path = create_backup(data, bkp_dir)

        with zipfile.ZipFile(path, "r") as zf:
            names = zf.namelist()
        assert not any("sommelier" in n for n in names)

    def test_includes_sommelier_when_requested(self, tmp_path):
        data = _setup_data_dir(tmp_path)
        bkp_dir = tmp_path / "bkp"

        path = create_backup(data, bkp_dir, include_sommelier=True)

        with zipfile.ZipFile(path, "r") as zf:
            names = zf.namelist()
        assert any("sommelier" in n for n in names)

    def test_excludes_logs_by_default(self, tmp_path):
        data = _setup_data_dir(tmp_path)
        bkp_dir = tmp_path / "bkp"

        path = create_backup(data, bkp_dir)

        with zipfile.ZipFile(path, "r") as zf:
            names = zf.namelist()
        assert not any("logs" in n for n in names)

    def test_includes_logs_when_requested(self, tmp_path):
        data = _setup_data_dir(tmp_path)
        bkp_dir = tmp_path / "bkp"

        path = create_backup(data, bkp_dir, include_logs=True)

        with zipfile.ZipFile(path, "r") as zf:
            names = zf.namelist()
        assert any("logs" in n for n in names)

    def test_raises_if_data_dir_missing(self, tmp_path):
        bkp_dir = tmp_path / "bkp"
        missing = tmp_path / "nonexistent"

        with pytest.raises(FileNotFoundError, match="Data directory not found"):
            create_backup(missing, bkp_dir)

    def test_creates_backup_dir_if_absent(self, tmp_path):
        data = _setup_data_dir(tmp_path)
        bkp_dir = tmp_path / "deep" / "nested" / "bkp"

        path = create_backup(data, bkp_dir)

        assert path.exists()
        assert bkp_dir.exists()

    def test_timestamp_format_in_filename(self, tmp_path):
        data = _setup_data_dir(tmp_path)
        bkp_dir = tmp_path / "bkp"

        path = create_backup(data, bkp_dir)

        # Format: cellarbrain-YYYY-MM-DDTHHMMSS.zip
        import re

        assert re.match(
            r"cellarbrain-\d{4}-\d{2}-\d{2}T\d{6}\.zip",
            path.name,
        )

    def test_prunes_old_backups(self, tmp_path):
        data = _setup_data_dir(tmp_path)
        bkp_dir = tmp_path / "bkp"
        bkp_dir.mkdir()

        # Create 5 fake old backups
        for i in range(5):
            p = bkp_dir / f"cellarbrain-2026-01-0{i + 1}T120000.zip"
            with zipfile.ZipFile(p, "w") as zf:
                zf.writestr("dummy.txt", f"backup {i}")
            time.sleep(0.01)  # ensure different mtime

        # Create a new backup with max_backups=3
        create_backup(data, bkp_dir, max_backups=3)

        archives = list(bkp_dir.glob("cellarbrain-*.zip"))
        assert len(archives) == 3


# ---------------------------------------------------------------------------
# TestListBackups
# ---------------------------------------------------------------------------


class TestListBackups:
    """Test backup listing."""

    def test_returns_empty_for_no_backups(self, tmp_path):
        result = list_backups(tmp_path / "nonexistent")
        assert result == []

    def test_returns_empty_for_empty_dir(self, tmp_path):
        bkp_dir = tmp_path / "bkp"
        bkp_dir.mkdir()
        result = list_backups(bkp_dir)
        assert result == []

    def test_lists_backups_sorted_newest_first(self, tmp_path):
        bkp_dir = tmp_path / "bkp"
        bkp_dir.mkdir()

        for name in [
            "cellarbrain-2026-01-01T120000.zip",
            "cellarbrain-2026-03-15T090000.zip",
            "cellarbrain-2026-02-10T180000.zip",
        ]:
            with zipfile.ZipFile(bkp_dir / name, "w") as zf:
                zf.writestr("dummy.txt", "x")

        result = list_backups(bkp_dir)

        assert len(result) == 3
        # Sorted by filename descending (newest first)
        assert result[0]["name"] == "cellarbrain-2026-03-15T090000.zip"
        assert result[1]["name"] == "cellarbrain-2026-02-10T180000.zip"
        assert result[2]["name"] == "cellarbrain-2026-01-01T120000.zip"

    def test_includes_size_and_count(self, tmp_path):
        bkp_dir = tmp_path / "bkp"
        bkp_dir.mkdir()

        with zipfile.ZipFile(bkp_dir / "cellarbrain-2026-01-01T120000.zip", "w") as zf:
            zf.writestr("wine.parquet", "data" * 100)
            zf.writestr("bottle.parquet", "data" * 50)

        result = list_backups(bkp_dir)

        assert len(result) == 1
        assert result[0]["file_count"] == 2
        assert isinstance(result[0]["size_mb"], float)
        assert result[0]["size_mb"] >= 0


# ---------------------------------------------------------------------------
# TestRestoreBackup
# ---------------------------------------------------------------------------


class TestRestoreBackup:
    """Test backup restoration."""

    def test_restores_parquet_files(self, tmp_path):
        # Create a backup archive
        archive = tmp_path / "backup.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("wine.parquet", "restored-wine-data")
            zf.writestr("bottle.parquet", "restored-bottle-data")

        target = tmp_path / "restored"
        target.mkdir()

        count = restore_backup(archive, target)

        assert count == 2
        assert (target / "wine.parquet").read_text() == "restored-wine-data"
        assert (target / "bottle.parquet").read_text() == "restored-bottle-data"

    def test_restores_dossiers(self, tmp_path):
        archive = tmp_path / "backup.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("wines/cellar/0001-test.md", "# Test")

        target = tmp_path / "restored"

        count = restore_backup(archive, target)

        assert count == 1
        assert (target / "wines" / "cellar" / "0001-test.md").read_text() == "# Test"

    def test_dry_run_does_not_write(self, tmp_path):
        archive = tmp_path / "backup.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("wine.parquet", "data")

        target = tmp_path / "restored"
        target.mkdir()

        count = restore_backup(archive, target, dry_run=True)

        assert count == 1
        assert not (target / "wine.parquet").exists()

    def test_raises_if_archive_missing(self, tmp_path):
        missing = tmp_path / "nonexistent.zip"
        target = tmp_path / "output"

        with pytest.raises(FileNotFoundError, match="Backup not found"):
            restore_backup(missing, target)

    def test_preserves_files_not_in_archive(self, tmp_path):
        # Create target with an existing file
        target = tmp_path / "output"
        target.mkdir()
        (target / "sommelier").mkdir()
        (target / "sommelier" / "wine.index").write_bytes(b"INDEX")

        # Restore archive that only has Parquet
        archive = tmp_path / "backup.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("wine.parquet", "data")

        restore_backup(archive, target)

        # Sommelier index preserved
        assert (target / "sommelier" / "wine.index").read_bytes() == b"INDEX"
        # Parquet restored
        assert (target / "wine.parquet").read_text() == "data"

    def test_overwrites_existing_files(self, tmp_path):
        target = tmp_path / "output"
        target.mkdir()
        (target / "wine.parquet").write_text("old-data")

        archive = tmp_path / "backup.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("wine.parquet", "new-data")

        restore_backup(archive, target)

        assert (target / "wine.parquet").read_text() == "new-data"


# ---------------------------------------------------------------------------
# TestPruneBackups
# ---------------------------------------------------------------------------


class TestPruneBackups:
    """Test retention policy."""

    def test_keeps_max_backups(self, tmp_path):
        bkp_dir = tmp_path / "bkp"
        bkp_dir.mkdir()

        for i in range(7):
            p = bkp_dir / f"cellarbrain-2026-01-{i + 1:02d}T120000.zip"
            with zipfile.ZipFile(p, "w") as zf:
                zf.writestr("dummy.txt", f"backup {i}")
            time.sleep(0.01)

        removed = _prune_backups(bkp_dir, max_backups=3)

        assert len(removed) == 4
        remaining = list(bkp_dir.glob("cellarbrain-*.zip"))
        assert len(remaining) == 3

    def test_removes_oldest_first(self, tmp_path):
        bkp_dir = tmp_path / "bkp"
        bkp_dir.mkdir()

        names = [
            "cellarbrain-2026-01-01T120000.zip",
            "cellarbrain-2026-01-02T120000.zip",
            "cellarbrain-2026-01-03T120000.zip",
        ]
        for name in names:
            p = bkp_dir / name
            with zipfile.ZipFile(p, "w") as zf:
                zf.writestr("dummy.txt", "x")
            time.sleep(0.01)

        removed = _prune_backups(bkp_dir, max_backups=1)

        assert len(removed) == 2
        remaining = list(bkp_dir.glob("cellarbrain-*.zip"))
        assert len(remaining) == 1
        # Most recent should remain
        assert remaining[0].name == "cellarbrain-2026-01-03T120000.zip"

    def test_noop_when_under_limit(self, tmp_path):
        bkp_dir = tmp_path / "bkp"
        bkp_dir.mkdir()

        for i in range(2):
            p = bkp_dir / f"cellarbrain-2026-01-{i + 1:02d}T120000.zip"
            with zipfile.ZipFile(p, "w") as zf:
                zf.writestr("dummy.txt", f"backup {i}")

        removed = _prune_backups(bkp_dir, max_backups=5)

        assert removed == []
        remaining = list(bkp_dir.glob("cellarbrain-*.zip"))
        assert len(remaining) == 2
