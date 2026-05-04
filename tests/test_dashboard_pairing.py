"""Tests for dashboard pairing page — food-pairing retrieval UI."""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import pytest


@pytest.fixture()
def _log_db(tmp_path):
    """Create a minimal log database."""
    db_path = str(tmp_path / "test-logs.duckdb")
    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE tool_events (
            event_id        VARCHAR PRIMARY KEY,
            session_id      VARCHAR NOT NULL,
            turn_id         VARCHAR NOT NULL,
            event_type      VARCHAR NOT NULL,
            name            VARCHAR NOT NULL,
            started_at      TIMESTAMPTZ NOT NULL,
            ended_at        TIMESTAMPTZ NOT NULL,
            duration_ms     DOUBLE NOT NULL,
            status          VARCHAR NOT NULL,
            request_id      VARCHAR,
            parameters      VARCHAR,
            error_type      VARCHAR,
            error_message   VARCHAR,
            result_size     INTEGER,
            agent_name      VARCHAR,
            trace_id        VARCHAR,
            client_id       VARCHAR
        )
    """)
    now = datetime.now(UTC)
    con.execute(
        """INSERT INTO tool_events VALUES (
            'evt1', 'sess1', 'turn1', 'tool', 'test_tool', ?, ?, 50.0, 'ok',
            NULL, NULL, NULL, NULL, NULL, 'test-agent', NULL, NULL
        )""",
        [now, now],
    )
    con.close()
    return db_path


@pytest.fixture()
def pairing_client(_log_db, tmp_path):
    """TestClient with cellar data for pairing page tests."""
    from unittest.mock import patch

    from starlette.testclient import TestClient

    from cellarbrain.dashboard import create_app

    # Create a DuckDB file with wines_full view
    data_db = str(tmp_path / "cellar.duckdb")
    con = duckdb.connect(data_db)
    con.execute("""
        CREATE TABLE wine_data AS
        SELECT * FROM (VALUES
            (1, 'Château Margaux', 2015, 'Red wine', 'France', 'Margaux',
             'Cabernet Sauvignon', 3, 120.0, 'optimal', 92.0,
             ['duck-confit', 'beef-bourguignon']::VARCHAR[],
             ['red_meat', 'heavy', 'French']::VARCHAR[]),
            (2, 'Chablis Premier Cru', 2020, 'White wine', 'France', 'Chablis',
             'Chardonnay', 4, 45.0, 'drinkable', 88.0,
             ['grilled-fish', 'seafood-platter']::VARCHAR[],
             ['fish', 'light', 'French']::VARCHAR[]),
            (3, 'Barolo DOCG', 2018, 'Red wine', 'Italy', 'Barolo',
             'Nebbiolo', 2, 55.0, 'optimal', 94.0,
             ['truffle-pasta']::VARCHAR[],
             ['red_meat', 'heavy', 'Italian']::VARCHAR[]),
            (4, 'Chasselas Dézaley', 2021, 'White wine', 'Switzerland', 'Lavaux',
             'Chasselas', 6, 28.0, 'drinkable', NULL,
             ['raclette', 'fondue']::VARCHAR[],
             ['cheese', 'medium', 'Swiss']::VARCHAR[])
        ) AS t(wine_id, wine_name, vintage, category, country, region,
               primary_grape, bottles_stored, price, drinking_status,
               best_pro_score, food_tags, food_groups)
    """)
    con.execute("CREATE VIEW wines_full AS SELECT * FROM wine_data")
    con.close()

    cellar_con = duckdb.connect(data_db, read_only=True)
    app = create_app(log_db_path=_log_db, data_dir=str(tmp_path))

    with patch("cellarbrain.query.get_agent_connection", return_value=cellar_con), TestClient(app) as client:
        yield client

    cellar_con.close()


@pytest.fixture()
def pairing_client_no_data(_log_db, tmp_path):
    """TestClient without cellar data."""
    from starlette.testclient import TestClient

    from cellarbrain.dashboard import create_app

    app = create_app(log_db_path=_log_db)

    with TestClient(app) as client:
        yield client


class TestPairingPageGet:
    """GET /pairing tests."""

    def test_returns_200(self, pairing_client):
        resp = pairing_client.get("/pairing")
        assert resp.status_code == 200
        assert "Dish description" in resp.text
        assert "Find Pairing Candidates" in resp.text

    def test_has_form_fields(self, pairing_client):
        resp = pairing_client.get("/pairing")
        assert 'name="category"' in resp.text
        assert 'name="weight"' in resp.text
        assert 'name="protein"' in resp.text
        assert 'name="cuisine"' in resp.text
        assert 'name="grapes"' in resp.text

    def test_nav_link_present(self, pairing_client):
        resp = pairing_client.get("/pairing")
        assert "/pairing" in resp.text


class TestPairingPagePost:
    """POST /pairing tests — form submission."""

    def test_returns_results_table(self, pairing_client):
        resp = pairing_client.post(
            "/pairing",
            data={
                "dish_description": "grilled steak",
                "protein": "red_meat",
                "category": "red",
            },
        )
        assert resp.status_code == 200
        assert "candidates found" in resp.text or "No candidates found" in resp.text

    def test_htmx_returns_partial(self, pairing_client):
        resp = pairing_client.post(
            "/pairing",
            data={"dish_description": "grilled steak", "protein": "red_meat", "category": "red"},
            headers={"HX-Request": "true", "HX-Target": "#pairing-results"},
        )
        assert resp.status_code == 200
        # Partial should NOT contain full page wrapper
        assert "<!doctype" not in resp.text.lower()

    def test_results_have_wine_links(self, pairing_client):
        resp = pairing_client.post(
            "/pairing",
            data={
                "dish_description": "steak",
                "protein": "red_meat",
                "category": "red",
            },
        )
        assert resp.status_code == 200
        assert "/cellar/" in resp.text

    def test_empty_results_message(self, pairing_client):
        resp = pairing_client.post(
            "/pairing",
            data={
                "dish_description": "xyznonexistent",
                "category": "sparkling",
                "grapes": "Nonexistent Grape",
            },
        )
        assert "No candidates found" in resp.text

    def test_swiss_cuisine_finds_chasselas(self, pairing_client):
        resp = pairing_client.post(
            "/pairing",
            data={
                "dish_description": "raclette",
                "protein": "cheese",
                "cuisine": "Swiss",
                "category": "white",
            },
        )
        assert resp.status_code == 200
        assert "Chasselas" in resp.text


class TestPairingPageNoData:
    """Tests when cellar data is unavailable."""

    def test_returns_503(self, pairing_client_no_data):
        resp = pairing_client_no_data.get("/pairing")
        assert resp.status_code == 503
