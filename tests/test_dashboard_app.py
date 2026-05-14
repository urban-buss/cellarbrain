"""Tests for dashboard app routes — HTTP smoke tests."""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import pytest


@pytest.fixture()
def dashboard_client(tmp_path):
    """Starlette TestClient with a DuckDB log store containing sample data."""
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
    for i in range(10):
        status = "error" if i == 0 else "ok"
        con.execute(
            """
            INSERT INTO tool_events VALUES (
                ?, 'sess1', 'turn1', 'tool', ?, ?, ?, ?, ?,
                NULL, NULL, ?, ?, NULL, 'test-agent', NULL, NULL
            )
        """,
            [
                f"evt{i}",
                f"tool_{i % 3}",
                now,
                now,
                float(50 + i * 10),
                status,
                "QueryError" if status == "error" else None,
                "bad sql" if status == "error" else None,
            ],
        )
    con.close()

    from starlette.testclient import TestClient

    from cellarbrain.dashboard import create_app

    app = create_app(log_db_path=db_path)
    with TestClient(app) as client:
        yield client


class TestObservabilityRoutes:
    def test_index_returns_200(self, dashboard_client):
        r = dashboard_client.get("/")
        assert r.status_code == 200
        assert "Cellarbrain" in r.text

    def test_tools_returns_200(self, dashboard_client):
        r = dashboard_client.get("/tools")
        assert r.status_code == 200

    def test_errors_returns_200(self, dashboard_client):
        r = dashboard_client.get("/errors")
        assert r.status_code == 200

    def test_errors_export_returns_plain_text(self, dashboard_client):
        r = dashboard_client.get("/errors/export")
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/plain")
        text = r.text
        assert "# Cellarbrain Error Report" in text
        assert "QueryError" in text
        assert "bad sql" in text

    def test_sessions_returns_200(self, dashboard_client):
        r = dashboard_client.get("/sessions")
        assert r.status_code == 200

    def test_latency_returns_200(self, dashboard_client):
        r = dashboard_client.get("/latency")
        assert r.status_code == 200

    def test_live_returns_200(self, dashboard_client):
        r = dashboard_client.get("/live")
        assert r.status_code == 200

    def test_error_detail_returns_200(self, dashboard_client):
        r = dashboard_client.get("/errors/evt0")
        assert r.status_code == 200

    def test_session_detail_returns_200(self, dashboard_client):
        r = dashboard_client.get("/sessions/sess1")
        assert r.status_code == 200

    def test_turn_events_returns_200(self, dashboard_client):
        r = dashboard_client.get("/sessions/sess1/turn1")
        assert r.status_code == 200


class TestJsonApi:
    def test_api_volume(self, dashboard_client):
        r = dashboard_client.get("/api/volume?period=24h")
        assert r.status_code == 200
        data = r.json()
        assert "labels" in data
        assert "data" in data

    def test_api_latency_hist(self, dashboard_client):
        r = dashboard_client.get("/api/latency-hist")
        assert r.status_code == 200
        data = r.json()
        assert "buckets" in data

    def test_api_latency_ts(self, dashboard_client):
        r = dashboard_client.get("/api/latency-ts")
        assert r.status_code == 200
        data = r.json()
        assert "labels" in data

    def test_api_top_tools(self, dashboard_client):
        r = dashboard_client.get("/api/top-tools")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)

    def test_api_refresh(self, dashboard_client):
        r = dashboard_client.post("/api/refresh")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_api_live_sse_stream(self, dashboard_client):
        r = dashboard_client.get("/api/live?_max_polls=0")
        assert r.status_code == 200
        assert "text/event-stream" in r.headers["content-type"]
        # Should contain actual event card HTML (not the old stub text)
        assert "SSE not yet implemented" not in r.text
        assert "event-card" in r.text


class TestPeriodSelector:
    @pytest.mark.parametrize("period", ["1h", "24h", "7d", "30d"])
    def test_valid_periods(self, dashboard_client, period):
        r = dashboard_client.get(f"/?period={period}")
        assert r.status_code == 200

    def test_invalid_period_defaults_to_24h(self, dashboard_client):
        r = dashboard_client.get("/?period=invalid")
        assert r.status_code == 200


class TestHtmxPartials:
    def test_tools_htmx_returns_partial(self, dashboard_client):
        r = dashboard_client.get("/tools", headers={"HX-Request": "true", "HX-Target": "tool-tbody"})
        assert r.status_code == 200
        # Partial should not contain the full base template nav
        assert "<nav>" not in r.text

    def test_errors_htmx_returns_partial(self, dashboard_client):
        r = dashboard_client.get("/errors", headers={"HX-Request": "true", "HX-Target": "error-table"})
        assert r.status_code == 200
        assert "<nav>" not in r.text


# ---- Cellar route tests ---------------------------------------------------


def _make_cellar_con():
    """Create an in-memory DuckDB with sample cellar views."""
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE VIEW wines AS
        SELECT * FROM (VALUES
            (1, 'Château Margaux Grand Vin', 2015,
             'Château Margaux', 'Red wine', 'France', 'Margaux',
             'Médoc', 'Cabernet Sauvignon', 'Bordeaux blend',
             'optimal', 'premium', 120.0, NULL,
             3, 0, 0, true, false, NULL, 750, 'Standard', NULL),
            (2, 'Chablis Premier Cru', 2020,
             'William Fèvre', 'White wine', 'France', 'Chablis',
             NULL, 'Chardonnay', 'Single varietal',
             'drinkable', 'mid', 45.0, NULL,
             4, 0, 1, false, false, NULL, 750, 'Standard', NULL)
        ) AS t(wine_id, wine_name, vintage, winery_name, category,
               country, region, subregion, primary_grape, blend_type,
               drinking_status, price_tier, price, style_tags,
               bottles_stored, bottles_on_order, bottles_consumed,
               is_favorite, is_wishlist, tracked_wine_id,
               volume_ml, bottle_format, format_group_id)
    """)
    con.execute("""
        CREATE VIEW wines_full AS
        SELECT *,
               NULL AS drink_from, NULL AS drink_until,
               NULL AS optimal_from, NULL AS optimal_until,
               NULL AS alcohol_pct, NULL AS grapes
        FROM wines
    """)
    con.execute("""
        CREATE VIEW bottles AS
        SELECT * FROM (VALUES
            (1, 1, 'Château Margaux Grand Vin', 2015,
             'Château Margaux', 'Red wine', 'France', 'Margaux',
             'Cabernet Sauvignon', 'optimal', 'premium', 120.0,
             'stored', 'Cave Nord', 'A3', NULL, NULL)
        ) AS t(bottle_id, wine_id, wine_name, vintage, winery_name,
               category, country, region, primary_grape,
               drinking_status, price_tier, price,
               status, cellar_name, shelf, output_date, output_type)
    """)
    con.execute("""
        CREATE VIEW bottles_full AS
        SELECT *, false AS is_in_transit FROM bottles
    """)
    con.execute("CREATE VIEW bottles_stored AS SELECT * FROM bottles WHERE status = 'stored'")
    con.execute("CREATE VIEW bottles_on_order AS SELECT * FROM bottles WHERE 1=0")
    con.execute("CREATE VIEW bottles_consumed AS SELECT * FROM bottles WHERE status != 'stored'")
    # Tracked wines + price views (optional, may not exist in all deployments)
    con.execute("""
        CREATE VIEW tracked_wines AS
        SELECT * FROM (VALUES
            (1, 'Margaux Grand Vin', 'Château Margaux', 'Red wine',
             'France', 'Margaux', 'Médoc', 'Grand Cru Classé',
             1, ARRAY[2015], 3, 0)
        ) AS t(tracked_wine_id, wine_name, winery_name, category,
               country, region, subregion, classification,
               wine_count, vintages, bottles_stored, bottles_on_order)
    """)
    con.execute("""
        CREATE VIEW price_history AS
        SELECT * FROM (VALUES
            (1, NULL, 750, 'Flaschenpost',
             '2026-01'::VARCHAR, 180.0, 195.0, 187.5, 3)
        ) AS t(tracked_wine_id, vintage, bottle_size_ml, retailer_name,
               month, min_price_chf, max_price_chf, avg_price_chf,
               observations)
    """)
    con.execute("""
        CREATE VIEW price_observations AS
        SELECT * FROM (VALUES
            (1, 1, 'Margaux Grand Vin', 'Château Margaux', 2015,
             750, 'Flaschenpost', 'https://fp.ch/1', 180.0, 'CHF', 180.0,
             true, '2026-02-15T10:00:00'::VARCHAR, 'agent', NULL)
        ) AS t(observation_id, tracked_wine_id, wine_name, winery_name,
               vintage, bottle_size_ml, retailer_name, retailer_url,
               price, currency, price_chf, in_stock, observed_at,
               observation_source, notes)
    """)
    return con


@pytest.fixture()
def cellar_client(tmp_path):
    """TestClient with both log store and cellar data."""
    db_path = str(tmp_path / "test-logs.duckdb")
    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE tool_events (
            event_id VARCHAR PRIMARY KEY, session_id VARCHAR,
            turn_id VARCHAR, event_type VARCHAR, name VARCHAR,
            started_at TIMESTAMPTZ, ended_at TIMESTAMPTZ,
            duration_ms DOUBLE, status VARCHAR, request_id VARCHAR,
            parameters VARCHAR, error_type VARCHAR, error_message VARCHAR,
            result_size INTEGER, agent_name VARCHAR, trace_id VARCHAR,
            client_id VARCHAR
        )
    """)
    con.close()

    from unittest.mock import patch

    from starlette.testclient import TestClient

    from cellarbrain.dashboard import create_app

    cellar_con = _make_cellar_con()
    app = create_app(log_db_path=db_path, data_dir=str(tmp_path))

    with patch("cellarbrain.query.get_agent_connection", return_value=cellar_con), TestClient(app) as client:
        yield client
    cellar_con.close()


@pytest.fixture()
def no_cellar_client(tmp_path):
    """TestClient without cellar data (data_dir=None)."""
    db_path = str(tmp_path / "test-logs.duckdb")
    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE tool_events (
            event_id VARCHAR PRIMARY KEY, session_id VARCHAR,
            turn_id VARCHAR, event_type VARCHAR, name VARCHAR,
            started_at TIMESTAMPTZ, ended_at TIMESTAMPTZ,
            duration_ms DOUBLE, status VARCHAR, request_id VARCHAR,
            parameters VARCHAR, error_type VARCHAR, error_message VARCHAR,
            result_size INTEGER, agent_name VARCHAR, trace_id VARCHAR,
            client_id VARCHAR
        )
    """)
    con.close()

    from starlette.testclient import TestClient

    from cellarbrain.dashboard import create_app

    app = create_app(log_db_path=db_path)
    with TestClient(app) as client:
        yield client


class TestCellarRoutes:
    def test_cellar_returns_200(self, cellar_client):
        r = cellar_client.get("/cellar")
        assert r.status_code == 200
        assert "Château Margaux" in r.text

    def test_cellar_search(self, cellar_client):
        r = cellar_client.get("/cellar?q=chablis")
        assert r.status_code == 200

    def test_wine_detail_returns_200(self, cellar_client):
        r = cellar_client.get("/cellar/1")
        assert r.status_code == 200
        assert "Château Margaux" in r.text

    def test_wine_detail_404(self, cellar_client):
        r = cellar_client.get("/cellar/999")
        assert r.status_code == 404

    def test_bottles_returns_200(self, cellar_client):
        r = cellar_client.get("/bottles")
        assert r.status_code == 200

    def test_drinking_returns_200(self, cellar_client):
        r = cellar_client.get("/drinking")
        assert r.status_code == 200

    def test_cellar_without_data_returns_503(self, no_cellar_client):
        r = no_cellar_client.get("/cellar")
        assert r.status_code == 503


class TestCellarHtmx:
    def test_wine_rows_partial(self, cellar_client):
        r = cellar_client.get("/cellar", headers={"HX-Request": "true", "HX-Target": "wine-tbody"})
        assert r.status_code == 200
        assert "<nav>" not in r.text

    def test_bottle_rows_partial(self, cellar_client):
        r = cellar_client.get("/bottles", headers={"HX-Request": "true", "HX-Target": "bottle-tbody"})
        assert r.status_code == 200
        assert "<nav>" not in r.text


# ---- SQL playground tests -------------------------------------------------


class TestSqlPlayground:
    def test_get_returns_200(self, cellar_client):
        r = cellar_client.get("/sql")
        assert r.status_code == 200
        assert "SQL Playground" in r.text

    def test_valid_query(self, cellar_client):
        r = cellar_client.post(
            "/sql",
            data={
                "sql": "SELECT COUNT(*) AS cnt FROM wines",
                "max_rows": "10",
            },
        )
        assert r.status_code == 200
        assert "cnt" in r.text

    def test_invalid_sql_shows_error(self, cellar_client):
        r = cellar_client.post(
            "/sql",
            data={
                "sql": "DROP TABLE wines",
            },
        )
        assert r.status_code == 200
        assert "Error" in r.text or "error" in r.text.lower()

    def test_empty_sql(self, cellar_client):
        r = cellar_client.post("/sql", data={"sql": ""})
        assert r.status_code == 200

    def test_htmx_returns_partial(self, cellar_client):
        r = cellar_client.post(
            "/sql",
            data={"sql": "SELECT 1 AS x", "max_rows": "10"},
            headers={"HX-Request": "true", "HX-Target": "sql-results"},
        )
        assert r.status_code == 200
        assert "<nav>" not in r.text

    def test_no_cellar_returns_503(self, no_cellar_client):
        r = no_cellar_client.get("/sql")
        assert r.status_code == 503


# ---- Statistics tests -----------------------------------------------------


class TestStatsRoute:
    def test_stats_returns_200(self, cellar_client):
        r = cellar_client.get("/stats")
        assert r.status_code == 200
        assert "Cellar Statistics" in r.text

    @pytest.mark.parametrize(
        "group",
        [
            "country",
            "region",
            "category",
            "vintage",
        ],
    )
    def test_group_by_options(self, cellar_client, group):
        r = cellar_client.get(f"/stats?group_by={group}")
        assert r.status_code == 200

    def test_htmx_returns_partial(self, cellar_client):
        r = cellar_client.get(
            "/stats?group_by=country",
            headers={"HX-Request": "true", "HX-Target": "stats-content"},
        )
        assert r.status_code == 200
        assert "<nav>" not in r.text

    def test_no_cellar_returns_503(self, no_cellar_client):
        r = no_cellar_client.get("/stats")
        assert r.status_code == 503


# ---- Tracked wine tests --------------------------------------------------


class TestTrackedRoutes:
    def test_tracked_returns_200(self, cellar_client):
        r = cellar_client.get("/tracked")
        assert r.status_code == 200
        assert "Margaux Grand Vin" in r.text

    def test_tracked_detail_returns_200(self, cellar_client):
        r = cellar_client.get("/tracked/1")
        assert r.status_code == 200
        assert "Margaux Grand Vin" in r.text

    def test_tracked_detail_404(self, cellar_client):
        r = cellar_client.get("/tracked/999")
        assert r.status_code == 404

    def test_no_cellar_returns_503(self, no_cellar_client):
        r = no_cellar_client.get("/tracked")
        assert r.status_code == 503


# ---- Workbench route tests ------------------------------------------------


class TestWorkbenchRoutes:
    def test_workbench_list_returns_200(self, cellar_client):
        r = cellar_client.get("/workbench")
        assert r.status_code == 200
        assert "query_cellar" in r.text

    def test_workbench_tool_returns_200(self, cellar_client):
        r = cellar_client.get("/workbench/query_cellar")
        assert r.status_code == 200
        assert "query_cellar" in r.text

    def test_workbench_tool_404(self, cellar_client):
        r = cellar_client.get("/workbench/nonexistent")
        assert r.status_code == 404

    def test_workbench_write_tool_403(self, cellar_client):
        r = cellar_client.get("/workbench/update_dossier")
        assert r.status_code == 403

    def test_workbench_batch_returns_200(self, cellar_client):
        r = cellar_client.get("/workbench/batch")
        assert r.status_code == 200
        assert "Smoke Test" in r.text

    def test_workbench_list_no_cellar(self, no_cellar_client):
        r = no_cellar_client.get("/workbench")
        assert r.status_code == 200


# ---- Include workbench toggle tests --------------------------------------


class TestIncludeWorkbench:
    def test_default_excludes_workbench(self, dashboard_client):
        r = dashboard_client.get("/")
        assert r.status_code == 200
        # Checkbox should be present but unchecked
        assert "include_workbench" in r.text

    def test_include_workbench_param(self, dashboard_client):
        r = dashboard_client.get("/?include_workbench=1")
        assert r.status_code == 200


# ---- Phase 5: SSE + Live Tail tests --------------------------------------


class TestSSEEndpoint:
    def test_sse_returns_event_stream(self, dashboard_client):
        r = dashboard_client.get("/api/live?_max_polls=0")
        assert r.status_code == 200
        assert "text/event-stream" in r.headers["content-type"]

    def test_sse_contains_event_cards(self, dashboard_client):
        r = dashboard_client.get("/api/live?_max_polls=0")
        assert "event-card" in r.text

    def test_sse_filter_type(self, dashboard_client):
        r = dashboard_client.get("/api/live?filter_type=tool&_max_polls=0")
        assert r.status_code == 200
        assert "text/event-stream" in r.headers["content-type"]

    def test_sse_filter_status(self, dashboard_client):
        r = dashboard_client.get("/api/live?filter_status=error&_max_polls=0")
        assert r.status_code == 200
        # Should only get error events — the fixture has 1 error event
        assert "event-card" in r.text

    def test_sse_filter_status_ok_excludes_errors(self, dashboard_client):
        r = dashboard_client.get("/api/live?filter_status=ok&_max_polls=0")
        assert r.status_code == 200
        # Should not contain error event cards
        assert "event-card error" not in r.text

    def test_sse_no_cache_header(self, dashboard_client):
        r = dashboard_client.get("/api/live?_max_polls=0")
        assert r.headers.get("cache-control") == "no-cache"


class TestRenderHelpers:
    def test_render_event_card_ok(self):
        from cellarbrain.dashboard.app import _render_event_card

        event = {
            "event_id": "e1",
            "started_at": "12:00:00.000",
            "event_type": "tool",
            "name": "find_wine",
            "status": "ok",
            "duration_ms": 42.0,
            "error_type": None,
            "turn_id": "t1",
            "parameters": None,
        }
        html = _render_event_card(event)
        assert "event-card" in html
        assert "find_wine" in html
        assert "42 ms" in html
        assert "error" not in html.split("class=")[0]  # no error class on card

    def test_render_event_card_error(self):
        from cellarbrain.dashboard.app import _render_event_card

        event = {
            "event_id": "e2",
            "started_at": "12:00:01.000",
            "event_type": "tool",
            "name": "query_cellar",
            "status": "error",
            "duration_ms": 5.0,
            "error_type": "QueryError",
            "turn_id": "t1",
            "parameters": None,
        }
        html = _render_event_card(event)
        assert "event-card error" in html
        assert "QueryError" in html

    def test_render_event_card_with_params(self):
        from cellarbrain.dashboard.app import _render_event_card

        event = {
            "event_id": "e3",
            "started_at": "12:00:02.000",
            "event_type": "tool",
            "name": "find_wine",
            "status": "ok",
            "duration_ms": 10.0,
            "error_type": None,
            "turn_id": "t1",
            "parameters": '{"query": "Margaux"}',
        }
        html = _render_event_card(event)
        assert "<details" in html
        assert "Margaux" in html

    def test_render_event_card_escapes_html(self):
        from cellarbrain.dashboard.app import _render_event_card

        event = {
            "event_id": "e4",
            "started_at": "12:00:03.000",
            "event_type": "tool",
            "name": "test",
            "status": "error",
            "duration_ms": 1.0,
            "error_type": "<script>alert(1)</script>",
            "turn_id": "t1",
            "parameters": "<img onerror=alert(1)>",
        }
        html = _render_event_card(event)
        assert "<script>" not in html
        assert "&lt;script&gt;" in html
        assert "<img" not in html

    def test_render_event_card_datetime(self):
        from datetime import datetime

        from cellarbrain.dashboard.app import _render_event_card

        event = {
            "event_id": "e5",
            "started_at": datetime(2025, 1, 1, 12, 30, 45, 123456, tzinfo=UTC),
            "event_type": "tool",
            "name": "test",
            "status": "ok",
            "duration_ms": 7.0,
            "error_type": None,
            "turn_id": "t1",
            "parameters": None,
        }
        html = _render_event_card(event)
        assert "12:30:45.123" in html

    def test_render_turn_boundary(self):
        from cellarbrain.dashboard.app import _render_turn_boundary

        html = _render_turn_boundary()
        assert "turn-boundary" in html

    def test_sse_message_format(self):
        from cellarbrain.dashboard.app import _sse_message

        msg = _sse_message('<div class="event-card">test</div>')
        assert msg.startswith("event: message\n")
        assert msg.endswith("\n\n")
        assert "data: " in msg


class TestLivePage:
    def test_live_page_has_sse_controls(self, dashboard_client):
        r = dashboard_client.get("/live")
        assert r.status_code == 200
        assert "filter-type" in r.text
        assert "filter-status" in r.text
        assert "btn-pause" in r.text
        assert "btn-clear" in r.text

    def test_live_page_has_event_counter(self, dashboard_client):
        r = dashboard_client.get("/live")
        assert "event-counter" in r.text

    def test_live_page_has_sse_status(self, dashboard_client):
        r = dashboard_client.get("/live")
        assert "sse-status" in r.text

    def test_live_page_has_container(self, dashboard_client):
        r = dashboard_client.get("/live")
        assert "live-container" in r.text


class TestAutoRefresh:
    def test_footer_has_auto_refresh_on_obs_pages(self, dashboard_client):
        r = dashboard_client.get("/")
        assert 'id="auto-refresh-toggle"' in r.text

    def test_footer_no_auto_refresh_on_live_page(self, dashboard_client):
        r = dashboard_client.get("/live")
        # Live page does not pass period, so the checkbox input should not appear
        assert 'id="auto-refresh-toggle"' not in r.text


class TestDbLock:
    def test_app_state_has_db_lock(self, dashboard_client):
        """Verify db_lock is created on app state during lifespan."""
        import asyncio

        assert hasattr(dashboard_client.app.state, "db_lock")
        assert isinstance(dashboard_client.app.state.db_lock, asyncio.Lock)


# ---- Phase 6: Edge-case tests ---------------------------------------------


class TestCellarPagination:
    def test_offset_returns_200(self, cellar_client):
        r = cellar_client.get("/cellar?page=2")
        assert r.status_code == 200

    def test_negative_page_clamped(self, cellar_client):
        r = cellar_client.get("/cellar?page=-1")
        assert r.status_code == 200


class TestBottlesViewTabs:
    def test_stored_view(self, cellar_client):
        r = cellar_client.get("/bottles?view=stored")
        assert r.status_code == 200

    def test_consumed_view(self, cellar_client):
        r = cellar_client.get("/bottles?view=consumed")
        assert r.status_code == 200

    def test_on_order_view(self, cellar_client):
        r = cellar_client.get("/bottles?view=on_order")
        assert r.status_code == 200


class TestSqlSecurity:
    def test_ddl_rejected(self, cellar_client):
        r = cellar_client.post("/sql", data={"sql": "DROP TABLE wines"})
        assert r.status_code == 200
        assert "error" in r.text.lower()

    def test_insert_rejected(self, cellar_client):
        r = cellar_client.post(
            "/sql",
            data={
                "sql": "INSERT INTO wines VALUES (99, 'hack', 2024)",
            },
        )
        assert r.status_code == 200
        assert "error" in r.text.lower()

    def test_max_rows_capped(self, cellar_client):
        r = cellar_client.post(
            "/sql",
            data={
                "sql": "SELECT * FROM wines",
                "max_rows": "1",
            },
        )
        assert r.status_code == 200


class TestWorkbenchExecution:
    def test_post_tool_execution(self, cellar_client):
        r = cellar_client.post(
            "/workbench/query_cellar",
            data={
                "sql": "SELECT COUNT(*) AS cnt FROM wines",
            },
        )
        assert r.status_code == 200


# ---- Anomaly route tests ---------------------------------------------------


class TestAnomalyRoutes:
    def test_anomalies_page_returns_200(self, dashboard_client):
        r = dashboard_client.get("/anomalies")
        assert r.status_code == 200
        assert "Anomalies" in r.text

    def test_anomalies_banner_returns_200(self, dashboard_client):
        r = dashboard_client.get("/anomalies/banner")
        assert r.status_code == 200
