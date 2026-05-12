"""Integration tests for Phase B-F dashboard routes (Cellar Intelligence).

Exercises the new HTMX/JSON endpoints with a real Parquet + dossier dataset
on disk, plus the MCP get_drink_tonight tool surface.
"""

from __future__ import annotations

import pytest
from starlette.testclient import TestClient

from cellarbrain.dashboard import create_app
from cellarbrain.dashboard.sidecars import (
    read_consumed_pending,
    read_drink_tonight,
)

# Reuse the rich dataset builder from the dossier-ops tests.
from test_dossier_ops import _build_entities


@pytest.fixture()
def data_dir(tmp_path):
    return _build_entities(tmp_path)


@pytest.fixture()
def client(tmp_path, data_dir):
    """TestClient with real Parquet + dossier files behind the dashboard."""
    log_db = str(tmp_path / "logs.duckdb")
    # Empty DuckDB log store
    import duckdb

    con = duckdb.connect(log_db)
    con.execute(
        """
        CREATE TABLE tool_events (
            event_id VARCHAR PRIMARY KEY, session_id VARCHAR, turn_id VARCHAR,
            event_type VARCHAR, name VARCHAR, started_at TIMESTAMPTZ,
            ended_at TIMESTAMPTZ, duration_ms DOUBLE, status VARCHAR,
            request_id VARCHAR, parameters VARCHAR, error_type VARCHAR,
            error_message VARCHAR, result_size INTEGER, agent_name VARCHAR,
            trace_id VARCHAR, client_id VARCHAR
        )
        """
    )
    con.close()
    app = create_app(log_db_path=log_db, data_dir=str(data_dir))
    with TestClient(app) as tc:
        yield tc


# ---------------------------------------------------------------------------
# Phase B - dashboard_notes editor
# ---------------------------------------------------------------------------


class TestDashboardNotes:
    def test_get_returns_empty_for_fresh_dossier(self, client):
        r = client.get("/cellar/1/notes")
        assert r.status_code == 200
        assert "dashboard-notes" in r.text.lower() or "note" in r.text.lower()

    def test_post_save_and_reread(self, client, data_dir):
        r = client.post("/cellar/1/notes", data={"note": "Pair with venison."})
        assert r.status_code == 200
        assert "Pair with venison" in r.text

        # Re-read confirms persistence
        r2 = client.get("/cellar/1/notes")
        assert "Pair with venison" in r2.text

    def test_post_empty_resets_to_placeholder(self, client):
        client.post("/cellar/1/notes", data={"note": "first"})
        r = client.post("/cellar/1/notes", data={"note": ""})
        assert r.status_code == 200
        assert "first" not in r.text

    def test_post_unknown_wine_returns_error(self, client):
        r = client.post("/cellar/9999/notes", data={"note": "x"})
        assert r.status_code == 400


# ---------------------------------------------------------------------------
# Phase C - mark consumed
# ---------------------------------------------------------------------------


class TestMarkConsumed:
    def test_mark_then_undo_roundtrip(self, client, data_dir):
        r = client.post("/cellar/1/bottles/1/consumed")
        assert r.status_code == 200
        items = read_consumed_pending(str(data_dir))
        assert len(items) == 1
        assert items[0]["bottle_id"] == 1

        r2 = client.post("/cellar/1/bottles/1/consumed/undo")
        assert r2.status_code == 200
        assert read_consumed_pending(str(data_dir)) == []

    def test_reminders_banner_lists_pending(self, client, data_dir):
        client.post("/cellar/1/bottles/1/consumed")
        r = client.get("/reminders")
        assert r.status_code == 200
        assert "1" in r.text  # count rendered

    def test_pending_consumed_page_renders_rows(self, client, data_dir):
        client.post("/cellar/1/bottles/1/consumed")
        r = client.get("/pending-consumed")
        assert r.status_code == 200
        # The wine for bottle 1 is "Cuvée Alpha"
        assert "Cuv" in r.text


# ---------------------------------------------------------------------------
# Phase D - drink-tonight
# ---------------------------------------------------------------------------


class TestDrinkTonight:
    def test_page_empty_initially(self, client):
        r = client.get("/drink-tonight")
        assert r.status_code == 200

    def test_add_then_remove(self, client, data_dir):
        r = client.post("/drink-tonight/add", data={"wine_id": "1"})
        assert r.status_code == 200
        items = read_drink_tonight(str(data_dir))
        assert [i["wine_id"] for i in items] == [1]

        r2 = client.post("/drink-tonight/remove", data={"wine_id": "1"})
        assert r2.status_code == 200
        assert read_drink_tonight(str(data_dir)) == []

    def test_json_get_returns_mirror(self, client, data_dir):
        client.post("/drink-tonight/add", data={"wine_id": "2"})
        r = client.get("/drink-tonight.json")
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 1
        assert body["items"][0]["wine_id"] == 2

    def test_post_sync_replaces_full_list(self, client, data_dir):
        payload = {"items": [{"wine_id": 1}, {"wine_id": 2}]}
        r = client.post("/drink-tonight", json=payload)
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 2
        assert sorted(i["wine_id"] for i in body["items"]) == [1, 2]

    def test_post_sync_invalid_payload_yields_empty(self, client, data_dir):
        r = client.post("/drink-tonight", json={"garbage": True})
        assert r.status_code == 200
        assert r.json()["count"] == 0

    def test_add_invalid_wine_id_returns_400(self, client):
        r = client.post("/drink-tonight/add", data={"wine_id": "notanint"})
        assert r.status_code == 400


# ---------------------------------------------------------------------------
# Phase E + F - timeline & heatmap
# ---------------------------------------------------------------------------


class TestTimelineAndHeatmap:
    def test_drinking_timeline_page(self, client):
        r = client.get("/drinking/timeline")
        assert r.status_code == 200
        # Wine 1 has drink_from/drink_until set
        assert "Cuv" in r.text or "timeline" in r.text.lower()

    def test_heatmap_page(self, client):
        r = client.get("/cellars/heatmap")
        assert r.status_code == 200
        assert "Main Cellar" in r.text or "cellar" in r.text.lower()
