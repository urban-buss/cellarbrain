"""Integration test — run full pipeline on real CSV files."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from cellarbrain.cli import main, run

RAW_DIR = Path(__file__).resolve().parent.parent / "raw"
WINES_CSV = RAW_DIR / "export-wines.csv"
BOTTLES_CSV = RAW_DIR / "export-bottles-stored.csv"
BOTTLES_GONE_CSV = RAW_DIR / "export-bottles-gone.csv"

_SKIP = not WINES_CSV.exists() or not BOTTLES_CSV.exists() or not BOTTLES_GONE_CSV.exists()
_REASON = "Raw CSV files not present in raw/ directory"


@pytest.mark.skipif(_SKIP, reason=_REASON)
class TestIntegration:
    def test_full_pipeline(self, tmp_path):
        ok = run(str(WINES_CSV), str(BOTTLES_CSV), str(tmp_path), bottles_gone_csv=str(BOTTLES_GONE_CSV))
        assert ok, "Validation failed — check pipeline output"

        # Verify all entity + tracking parquet files exist
        expected = [
            "winery",
            "appellation",
            "grape",
            "cellar",
            "provider",
            "wine",
            "wine_grape",
            "bottle",
            "tasting",
            "pro_rating",
            "etl_run",
            "change_log",
        ]
        for name in expected:
            assert (tmp_path / f"{name}.parquet").exists(), f"Missing {name}.parquet"

        # Verify wine dossier markdown files
        wines_dir = tmp_path / "wines"
        assert wines_dir.is_dir(), "Missing wines/ directory"
        assert (wines_dir / "cellar").is_dir(), "Missing wines/cellar/ directory"
        assert (wines_dir / "archive").is_dir(), "Missing wines/archive/ directory"
        md_files = list((wines_dir / "cellar").glob("*.md")) + list((wines_dir / "archive").glob("*.md"))
        wine_count = pq.read_metadata(tmp_path / "wine.parquet").num_rows
        assert len(md_files) == wine_count, f"Expected {wine_count} dossier files, got {len(md_files)}"

    def test_sync_after_full(self, tmp_path):
        """Incremental sync on the same CSVs should produce zero changes."""
        run(str(WINES_CSV), str(BOTTLES_CSV), str(tmp_path), bottles_gone_csv=str(BOTTLES_GONE_CSV))
        ok = run(
            str(WINES_CSV), str(BOTTLES_CSV), str(tmp_path), sync_mode=True, bottles_gone_csv=str(BOTTLES_GONE_CSV)
        )
        assert ok

        import pyarrow.parquet as pq

        runs = pq.read_table(tmp_path / "etl_run.parquet").to_pydict()
        assert len(runs["run_id"]) == 2
        assert runs["run_type"] == ["full", "incremental"]
        # Second run should have zero inserts/updates/deletes
        assert runs["total_inserts"][1] == 0
        assert runs["total_updates"][1] == 0
        assert runs["total_deletes"][1] == 0
        # Wine-level columns should also be zero on unchanged re-run
        assert runs["wines_inserted"][1] == 0
        assert runs["wines_updated"][1] == 0
        assert runs["wines_deleted"][1] == 0
        assert runs["wines_renamed"][1] == 0

        # No dossiers should have been rewritten (zero changes)
        wines_dir = tmp_path / "wines"
        assert wines_dir.is_dir()


# ---------------------------------------------------------------------------
# Shared fixture: run ETL once, reuse for CLI + MCP E2E tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def etl_output(tmp_path_factory):
    """Run the full ETL pipeline once and return the output directory."""
    if _SKIP:
        pytest.skip(_REASON)
    out = tmp_path_factory.mktemp("e2e")
    ok = run(str(WINES_CSV), str(BOTTLES_CSV), str(out), bottles_gone_csv=str(BOTTLES_GONE_CSV))
    assert ok, "ETL pipeline failed during E2E fixture setup"
    return out


# ---------------------------------------------------------------------------
# E2E CLI subcommands against real data
# ---------------------------------------------------------------------------


@pytest.mark.skipif(_SKIP, reason=_REASON)
class TestCLIEndToEnd:
    """Full end-to-end tests exercising every CLI subcommand on real data."""

    def test_etl_subcommand(self, tmp_path):
        argv = [
            "etl",
            str(WINES_CSV),
            str(BOTTLES_CSV),
            str(BOTTLES_GONE_CSV),
            "-o",
            str(tmp_path),
        ]
        with pytest.raises(SystemExit) as exc_info:
            main(argv)
        assert exc_info.value.code == 0

    def test_validate_subcommand(self, etl_output):
        with pytest.raises(SystemExit) as exc_info:
            main(["-d", str(etl_output), "validate"])
        assert exc_info.value.code == 0

    def test_query_select_wines(self, etl_output, capsys):
        main(["-d", str(etl_output), "query", "SELECT count(*) AS n FROM wines"])
        out = capsys.readouterr().out
        wine_count = pq.read_metadata(etl_output / "wine.parquet").num_rows
        assert str(wine_count) in out

    def test_query_join(self, etl_output, capsys):
        main(["-d", str(etl_output), "query", "SELECT wine_name, winery_name FROM wines LIMIT 5"])
        out = capsys.readouterr().out
        assert "|" in out  # Markdown table

    def test_query_csv_format(self, etl_output, capsys):
        main(["-d", str(etl_output), "query", "SELECT wine_id, wine_name FROM wines LIMIT 3", "--format", "csv"])
        out = capsys.readouterr().out
        assert "wine_id" in out  # CSV header
        lines = [l for l in out.strip().split("\n") if l.strip()]
        assert len(lines) == 4  # header + 3 rows

    def test_query_json_format(self, etl_output, capsys):
        main(["-d", str(etl_output), "query", "SELECT wine_id, wine_name FROM wines LIMIT 2", "--format", "json"])
        import json

        out = capsys.readouterr().out
        data = json.loads(out)
        assert len(data) == 2
        assert "wine_id" in data[0]

    def test_stats_default(self, etl_output, capsys):
        main(["-d", str(etl_output), "stats"])
        out = capsys.readouterr().out
        assert "Cellar Summary" in out
        assert "| wines" in out
        assert "Drinking Window" in out

    @pytest.mark.parametrize(
        "dimension",
        [
            "country",
            "region",
            "category",
            "vintage",
            "winery",
            "grape",
            "cellar",
            "provider",
            "status",
        ],
    )
    def test_stats_all_dimensions(self, etl_output, capsys, dimension):
        main(["-d", str(etl_output), "stats", "--by", dimension])
        out = capsys.readouterr().out
        assert f"by {dimension.title()}" in out
        assert "|" in out

    def test_dossier_read(self, etl_output, capsys):
        main(["-d", str(etl_output), "dossier", "1"])
        out = capsys.readouterr().out
        assert "---" in out  # frontmatter
        assert "Identity" in out

    def test_dossier_search(self, etl_output, capsys):
        main(["-d", str(etl_output), "dossier", "--search", "Bordeaux"])
        out = capsys.readouterr().out
        assert "|" in out  # Markdown table
        assert "Bordeaux" in out or "bordeaux" in out.lower()

    def test_dossier_pending(self, etl_output, capsys):
        main(["-d", str(etl_output), "dossier", "--pending", "--limit", "5"])
        out = capsys.readouterr().out
        # All wines should have pending sections after fresh ETL
        assert "wine_id" in out
        lines = [l for l in out.strip().split("\n") if l.startswith("|") and "---" not in l and "wine_id" not in l]
        assert len(lines) <= 5


# ---------------------------------------------------------------------------
# E2E MCP server tools against real data
# ---------------------------------------------------------------------------


@pytest.mark.skipif(_SKIP, reason=_REASON)
class TestMCPEndToEnd:
    """Full end-to-end tests calling MCP tool functions against real data."""

    @pytest.fixture(autouse=True)
    def _set_data_dir(self, etl_output, monkeypatch):
        monkeypatch.setenv("CELLARBRAIN_DATA_DIR", str(etl_output))

    @pytest.fixture()
    def server(self):
        from cellarbrain import mcp_server

        mcp_server._mcp_settings = None  # force re-read from env
        mcp_server.invalidate_connections()
        return mcp_server

    # -- query_cellar --

    def test_query_cellar_wine_count(self, server, etl_output):
        result = server.query_cellar("SELECT count(*) AS n FROM wines")
        wine_count = pq.read_metadata(etl_output / "wine.parquet").num_rows
        assert str(wine_count) in result

    def test_query_cellar_complex_join(self, server):
        result = server.query_cellar("""
            SELECT country, count(*) AS n
            FROM wines
            GROUP BY country
            ORDER BY n DESC
            LIMIT 5
        """)
        assert "|" in result
        assert "country" in result.lower() or "Italy" in result or "France" in result

    def test_query_cellar_rejects_insert(self, server):
        result = server.query_cellar("INSERT INTO wine VALUES (999)")
        assert result.startswith("Error:")

    # -- cellar_stats --

    def test_cellar_stats_default(self, server):
        result = server.cellar_stats()
        assert "Cellar Summary" in result
        assert "| wines" in result
        assert "| bottles" in result

    def test_cellar_stats_by_country(self, server):
        result = server.cellar_stats(group_by="country")
        assert "France" in result
        assert "Italy" in result

    # -- find_wine --

    def test_find_wine_by_region(self, server):
        result = server.find_wine("Bordeaux")
        assert "|" in result
        assert "Bordeaux" in result

    def test_find_wine_by_grape(self, server):
        result = server.find_wine("Tempranillo")
        assert "|" in result

    def test_find_wine_no_match(self, server):
        result = server.find_wine("ZzAbsolutelyNonexistentWineZz")
        assert "No wines found" in result

    # -- read_dossier --

    def test_read_dossier_existing(self, server):
        result = server.read_dossier(1)
        assert "---" in result
        assert "Identity" in result
        assert "Drinking Window" in result

    def test_read_dossier_missing(self, server):
        result = server.read_dossier(99999)
        assert result.startswith("Error:")

    # -- update_dossier + roundtrip --

    def test_update_dossier_and_verify(self, server):
        marker = "E2E integration test content — safe to delete"
        result = server.update_dossier(
            wine_id=1,
            section="producer_profile",
            content=marker,
            agent_name="e2e-test",
        )
        assert "Updated" in result

        dossier = server.read_dossier(1)
        assert marker in dossier

    def test_update_dossier_protected_section(self, server):
        result = server.update_dossier(
            wine_id=1,
            section="identity",
            content="bad",
        )
        assert result.startswith("Error:")

    # -- pending_research --

    def test_pending_research_returns_results(self, server):
        result = server.pending_research(limit=10)
        assert "wine_id" in result
        lines = [l for l in result.strip().split("\n") if l.startswith("|") and "---" not in l and "wine_id" not in l]
        assert 1 <= len(lines) <= 10

    # -- prompts --

    def test_cellar_qa_prompt_with_real_stats(self, server):
        prompt = server.cellar_qa()
        assert "wine cellar assistant" in prompt.lower()
        assert "Cellar Summary" in prompt  # real stats embedded

    def test_food_pairing_prompt(self, server):
        prompt = server.food_pairing("grilled steak")
        assert "grilled steak" in prompt

    def test_wine_research_prompt(self, server):
        prompt = server.wine_research(1)
        assert "read_dossier" in prompt

    def test_batch_research_prompt(self, server):
        prompt = server.batch_research(3)
        assert "3" in prompt
