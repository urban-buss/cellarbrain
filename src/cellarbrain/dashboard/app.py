"""Dashboard application assembly — routes, lifespan, templates."""

from __future__ import annotations

import asyncio
import logging
import pathlib
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import duckdb
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

from . import cellar_queries as cellar_q
from . import queries as obs_q
from . import workbench as wb

if TYPE_CHECKING:
    from cellarbrain.settings import DashboardConfig

_HERE = pathlib.Path(__file__).resolve().parent
_TEMPLATES = Jinja2Templates(directory=str(_HERE / "templates"))
_log = logging.getLogger(__name__)

# ---- Period parsing -------------------------------------------------------

_PERIOD_HOURS = {"1h": 1, "24h": 24, "7d": 168, "30d": 720}


def _period(request: Request) -> tuple[str, int]:
    """Extract period label and hours from query param."""
    p = request.query_params.get("period", "24h")
    return p, _PERIOD_HOURS.get(p, 24)


def _include_wb(request: Request) -> bool:
    """Check if workbench calls should be included in observability views."""
    return request.query_params.get("include_workbench") == "1"


def _wants_partial(request: Request) -> bool:
    """True when HTMX requests a fragment (not a full-page body swap).

    The period-selector links use ``hx-target="body"``; since ``<body>``
    has no ``id``, HTMX omits the ``HX-Target`` header.  Fragment requests
    (e.g. ``hx-target="#wine-tbody"``) always include it.
    """
    return request.headers.get("hx-request") == "true" and request.headers.get("hx-target") is not None


# ---- Lifespan -------------------------------------------------------------


@asynccontextmanager
async def _lifespan(app: Starlette):
    # Observability log store
    app.state.log_db = duckdb.connect(app.state.log_db_path, read_only=True)

    # Cellar data (Parquet-backed agent connection)
    cellar_con = None
    if app.state.data_dir:
        try:
            from cellarbrain import query as q

            cellar_con = q.get_agent_connection(app.state.data_dir)
        except Exception as exc:
            _log.warning("Failed to open cellar connection: %s", exc)
    app.state.cellar_con = cellar_con
    app.state.db_lock = asyncio.Lock()

    yield

    app.state.log_db.close()
    if cellar_con is not None:
        cellar_con.close()


# ---- Route handlers (observability) ---------------------------------------


async def index(request: Request) -> HTMLResponse:
    period, hours = _period(request)
    con = request.app.state.log_db
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        overview = obs_q.get_overview(con, hours, exclude_workbench=excl)
        recent_errors = obs_q.get_errors(con, hours, limit=5, exclude_workbench=excl)
        top_tools = obs_q.get_tool_usage(con, hours, limit=10, exclude_workbench=excl)
    return _TEMPLATES.TemplateResponse(
        request,
        "index.html",
        context={
            "period": period,
            "overview": overview,
            "errors": recent_errors,
            "top_tools": top_tools,
            "include_workbench": _include_wb(request),
        },
    )


async def tools(request: Request) -> HTMLResponse:
    period, hours = _period(request)
    con = request.app.state.log_db
    sort = request.query_params.get("sort", "calls")
    desc = request.query_params.get("dir", "desc") == "desc"
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_tool_usage(con, hours, sort=sort, desc=desc, exclude_workbench=excl)
    template = "partials/tool_rows.html" if _wants_partial(request) else "tools.html"
    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "period": period,
            "tools": data,
        },
    )


async def errors(request: Request) -> HTMLResponse:
    period, hours = _period(request)
    con = request.app.state.log_db
    tool = request.query_params.get("tool")
    error_type = request.query_params.get("error_type")
    offset = int(request.query_params.get("offset", "0"))
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_errors(
            con, hours, tool=tool, error_type=error_type, limit=50, offset=offset, exclude_workbench=excl
        )
        type_summary = obs_q.get_error_type_summary(con, hours, exclude_workbench=excl)
    template = "partials/error_rows.html" if _wants_partial(request) else "errors.html"
    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "period": period,
            "errors": data,
            "type_summary": type_summary,
        },
    )


async def errors_export(request: Request) -> PlainTextResponse:
    """Return an LLM-optimised plain-text error report for clipboard copy."""
    period, hours = _period(request)
    con = request.app.state.log_db
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_errors(con, hours, limit=200, offset=0, exclude_workbench=excl)
        type_summary = obs_q.get_error_type_summary(con, hours, exclude_workbench=excl)
    total = sum(ts["count"] for ts in type_summary)
    lines: list[str] = []
    lines.append(f"# Cellarbrain Error Report — last {period}")
    lines.append(f"Total errors: {total}")
    if type_summary:
        lines.append("")
        lines.append("## Error type breakdown")
        for ts in type_summary:
            lines.append(f"- {ts['error_type'] or 'Unknown'}: {ts['count']}")
    if data:
        lines.append("")
        lines.append(f"## Error details ({len(data)} most recent)")
        for i, e in enumerate(data, 1):
            ts = e["started_at"].strftime("%Y-%m-%d %H:%M:%S") if e["started_at"] else "?"
            lines.append("")
            lines.append(f"### {i}. [{ts}] {e['name']} — {e['error_type'] or 'Unknown'}")
            lines.append(f"Message: {e['error_message'] or '—'}")
            if e.get("duration_ms") is not None:
                lines.append(f"Duration: {e['duration_ms']}ms")
            if e.get("parameters"):
                lines.append(f"Parameters: {e['parameters']}")
    else:
        lines.append("")
        lines.append("No errors recorded in this period.")
    return PlainTextResponse("\n".join(lines))


async def error_detail(request: Request) -> HTMLResponse:
    event_id = request.path_params["event_id"]
    con = request.app.state.log_db
    async with request.app.state.db_lock:
        event = obs_q.get_event_detail(con, event_id)
    return _TEMPLATES.TemplateResponse(
        request,
        "partials/error_detail.html",
        context={
            "event": event,
        },
    )


async def sessions(request: Request) -> HTMLResponse:
    period, hours = _period(request)
    con = request.app.state.log_db
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_sessions(con, hours, exclude_workbench=excl)
    return _TEMPLATES.TemplateResponse(
        request,
        "sessions.html",
        context={
            "period": period,
            "sessions": data,
        },
    )


async def session_detail(request: Request) -> HTMLResponse:
    session_id = request.path_params["session_id"]
    con = request.app.state.log_db
    async with request.app.state.db_lock:
        turns = obs_q.get_session_turns(con, session_id)
    return _TEMPLATES.TemplateResponse(
        request,
        "partials/session_detail.html",
        context={
            "session_id": session_id,
            "turns": turns,
        },
    )


async def turn_events(request: Request) -> HTMLResponse:
    session_id = request.path_params["session_id"]
    turn_id = request.path_params["turn_id"]
    con = request.app.state.log_db
    async with request.app.state.db_lock:
        events = obs_q.get_turn_events(con, session_id, turn_id)
    return _TEMPLATES.TemplateResponse(
        request,
        "partials/turn_events.html",
        context={
            "events": events,
        },
    )


async def latency(request: Request) -> HTMLResponse:
    period, hours = _period(request)
    con = request.app.state.log_db
    tool = request.query_params.get("tool")
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        percentiles = obs_q.get_latency_percentiles(con, hours, tool=tool, exclude_workbench=excl)
        slow = obs_q.get_slow_calls(con, hours, exclude_workbench=excl)
    return _TEMPLATES.TemplateResponse(
        request,
        "latency.html",
        context={
            "period": period,
            "tool_filter": tool,
            "percentiles": percentiles,
            "slow_calls": slow,
        },
    )


async def live(request: Request) -> HTMLResponse:
    return _TEMPLATES.TemplateResponse(request, "live.html")


# ---- Helpers --------------------------------------------------------------


def _int_or_none(val: str | None) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(val)
    except ValueError:
        return None


# ---- Cellar routes --------------------------------------------------------


async def cellar(request: Request) -> HTMLResponse:
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available. Start the dashboard with a valid data directory."},
            status_code=503,
        )

    q = request.query_params.get("q")
    category = request.query_params.get("category")
    region = request.query_params.get("region")
    status = request.query_params.get("status")
    vintage_min = _int_or_none(request.query_params.get("vintage_min"))
    vintage_max = _int_or_none(request.query_params.get("vintage_max"))
    hide_empty = "1" in request.query_params.getlist("hide_empty") if "hide_empty" in request.query_params else True
    sort = request.query_params.get("sort", "wine_name")
    desc = request.query_params.get("dir", "asc") == "desc"
    page = max(1, _int_or_none(request.query_params.get("page")) or 1)
    per_page = 50

    wines, total = cellar_q.get_wines(
        con,
        q=q,
        category=category,
        region=region,
        status=status,
        vintage_min=vintage_min,
        vintage_max=vintage_max,
        hide_empty=hide_empty,
        sort=sort,
        desc=desc,
        limit=per_page,
        offset=(page - 1) * per_page,
    )
    filters = cellar_q.get_filter_options(con)
    quick_stats = cellar_q.get_quick_stats(con)
    total_pages = max(1, (total + per_page - 1) // per_page)

    template = "partials/wine_rows.html" if _wants_partial(request) else "cellar.html"

    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "wines": wines,
            "total": total,
            "page": page,
            "total_pages": total_pages,
            "filters": filters,
            "quick_stats": quick_stats,
            "q": q or "",
            "category": category or "",
            "region": region or "",
            "status": status or "",
            "sort": sort,
            "desc": desc,
            "hide_empty": hide_empty,
        },
    )


async def wine_detail(request: Request) -> HTMLResponse:
    wine_id = int(request.path_params["wine_id"])
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    wine = cellar_q.get_wine_detail(con, wine_id)
    if wine is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": f"Wine #{wine_id} not found."},
            status_code=404,
        )

    bottles = cellar_q.get_wine_bottles(con, wine_id)
    format_siblings = cellar_q.get_format_siblings(
        con,
        wine_id,
        wine.get("format_group_id"),
    )

    # Resolve and render dossier
    dossier_data = None
    data_dir = request.app.state.data_dir
    if data_dir:
        try:
            from cellarbrain.dossier_ops import resolve_dossier_path

            from .dossier_render import render_dossier

            dossier_path = resolve_dossier_path(wine_id, data_dir)
            dossier_data = render_dossier(dossier_path)
        except Exception:
            pass  # Dossier rendering is best-effort

    tab = request.query_params.get("tab", "dossier")

    return _TEMPLATES.TemplateResponse(
        request,
        "wine_detail.html",
        context={
            "wine": wine,
            "bottles": bottles,
            "format_siblings": format_siblings,
            "dossier": dossier_data,
            "tab": tab,
        },
    )


async def cellar_bottles(request: Request) -> HTMLResponse:
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    view = request.query_params.get("view", "stored")
    cellar_filter = request.query_params.get("cellar")
    category = request.query_params.get("category")
    sort = request.query_params.get("sort", "wine_name")
    desc = request.query_params.get("dir", "asc") == "desc"
    page = max(1, _int_or_none(request.query_params.get("page")) or 1)
    per_page = 50

    bottle_list, total = cellar_q.get_bottles(
        con,
        view=view,
        cellar=cellar_filter,
        category=category,
        sort=sort,
        desc=desc,
        limit=per_page,
        offset=(page - 1) * per_page,
    )
    cellar_names = cellar_q.get_cellar_names(con)
    total_pages = max(1, (total + per_page - 1) // per_page)

    template = "partials/bottle_rows.html" if _wants_partial(request) else "bottles.html"

    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "bottles": bottle_list,
            "total": total,
            "page": page,
            "total_pages": total_pages,
            "view": view,
            "cellar_names": cellar_names,
            "cellar_filter": cellar_filter or "",
            "category": category or "",
            "sort": sort,
            "desc": desc,
        },
    )


async def drinking(request: Request) -> HTMLResponse:
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    wines = cellar_q.get_drinking_now(con)
    return _TEMPLATES.TemplateResponse(
        request,
        "drinking.html",
        context={
            "wines": wines,
        },
    )


# ---- Food pairing ---------------------------------------------------------


async def pairing_page(request: Request) -> HTMLResponse:
    """Food pairing interactive page — classify dish, retrieve candidates."""
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    results = None
    params: dict[str, str | None] = {}

    if request.method == "POST":
        form = await request.form()
        params = {
            "dish_description": form.get("dish_description", ""),
            "category": form.get("category") or None,
            "weight": form.get("weight") or None,
            "protein": form.get("protein") or None,
            "cuisine": form.get("cuisine") or None,
            "grapes": form.get("grapes") or None,
        }

        from cellarbrain import pairing

        grape_list = [g.strip() for g in params["grapes"].split(",")] if params["grapes"] else None
        results = pairing.retrieve_candidates(
            con,
            dish_description=params["dish_description"],
            category=params["category"],
            weight=params["weight"],
            protein=params["protein"],
            cuisine=params["cuisine"],
            grapes=grape_list,
            limit=15,
        )

    template = "partials/pairing_results.html" if _wants_partial(request) else "pairing.html"
    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={"results": results, "params": params},
    )


# ---- SQL playground -------------------------------------------------------

_QUICK_QUERIES = [
    {
        "label": "Wines by region",
        "sql": "SELECT country, region, COUNT(*) AS wines, SUM(bottles_stored) AS bottles FROM wines GROUP BY country, region ORDER BY bottles DESC LIMIT 20",
    },
    {
        "label": "Stock by cellar",
        "sql": "SELECT cellar_name, COUNT(*) AS bottles, ROUND(SUM(price), 0) AS value FROM bottles_stored GROUP BY cellar_name ORDER BY bottles DESC",
    },
    {
        "label": "Drinking now",
        "sql": "SELECT wine_name, vintage, winery_name, drinking_status, bottles_stored FROM wines WHERE drinking_status IN ('optimal', 'drinkable') ORDER BY vintage",
    },
    {"label": "Top wines", "sql": "SELECT wine_name, vintage, winery_name FROM wines ORDER BY wine_name LIMIT 20"},
    {
        "label": "Recent purchases",
        "sql": "SELECT wine_name, vintage, purchase_price, purchase_date, provider FROM bottles_full ORDER BY purchase_date DESC LIMIT 20",
    },
    {
        "label": "Price tiers",
        "sql": "SELECT CASE WHEN price < 20 THEN 'Under 20' WHEN price < 50 THEN '20-50' WHEN price < 100 THEN '50-100' ELSE '100+' END AS tier, COUNT(*) AS wines, SUM(bottles_stored) AS bottles FROM wines GROUP BY tier ORDER BY MIN(price)",
    },
    {
        "label": "Consumption history",
        "sql": "SELECT wine_name, vintage, consume_date, consume_reason FROM bottles_full WHERE status = 'consumed' ORDER BY consume_date DESC LIMIT 20",
    },
    {
        "label": "On order",
        "sql": "SELECT wine_name, vintage, provider, purchase_price FROM bottles_full WHERE is_in_transit ORDER BY wine_name",
    },
]


def _get_view_names(con: duckdb.DuckDBPyConnection) -> list[str]:
    """List all views in the agent connection."""
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW' ORDER BY table_name"
    ).fetchall()
    return [r[0] for r in rows]


async def sql_playground(request: Request) -> HTMLResponse:
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    sql = ""
    result = None
    error = None
    elapsed_ms = None
    columns: list[str] = []

    if request.method == "POST":
        form = await request.form()
        sql = form.get("sql", "").strip()
        max_rows = min(int(form.get("max_rows", "500") or "500"), 1000)

        if sql:
            try:
                from cellarbrain.query import validate_sql

                validate_sql(sql)
                import time

                t0 = time.monotonic()
                df = con.execute(sql).fetchdf()
                elapsed_ms = round((time.monotonic() - t0) * 1000, 1)
                if len(df) > max_rows:
                    df = df.head(max_rows)
                columns = list(df.columns)
                result = df.to_dict(orient="records")
            except Exception as exc:
                error = str(exc)

    views = _get_view_names(con)

    template = "partials/sql_results.html" if _wants_partial(request) else "sql.html"

    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "sql": sql,
            "result": result,
            "columns": columns,
            "error": error,
            "elapsed_ms": elapsed_ms,
            "quick_queries": _QUICK_QUERIES,
            "views": views,
            "max_rows": max_rows if request.method == "POST" else 500,
        },
    )


# ---- Statistics dashboard -------------------------------------------------


async def stats(request: Request) -> HTMLResponse:
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    group_by = request.query_params.get("group_by", "country")
    sort = request.query_params.get("sort", "bottles")
    desc = request.query_params.get("dir", "desc") == "desc"

    overview = cellar_q.get_cellar_stats_overview(con)
    grouped = cellar_q.get_cellar_stats_grouped(
        con,
        group_by=group_by,
        sort=sort,
        desc=desc,
    )

    template = "partials/stats_content.html" if _wants_partial(request) else "stats.html"

    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "overview": overview,
            "grouped": grouped,
            "group_by": group_by,
            "sort": sort,
        },
    )


# ---- Tracked wines --------------------------------------------------------


async def tracked(request: Request) -> HTMLResponse:
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    wines = cellar_q.get_tracked_wines(con)
    return _TEMPLATES.TemplateResponse(
        request,
        "tracked.html",
        context={
            "wines": wines,
        },
    )


async def tracked_detail(request: Request) -> HTMLResponse:
    tracked_id = int(request.path_params["id"])
    con = request.app.state.cellar_con
    if con is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": "Cellar data not available."},
            status_code=503,
        )

    wine = cellar_q.get_tracked_wine_detail(con, tracked_id)
    if wine is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": f"Tracked wine #{tracked_id} not found."},
            status_code=404,
        )

    prices_chart = cellar_q.get_price_chart_data(con, tracked_id)
    prices_table = cellar_q.get_price_observations(con, tracked_id)

    # Companion dossier (best-effort)
    companion_data = None
    data_dir = request.app.state.data_dir
    if data_dir:
        try:
            from cellarbrain.companion_markdown import _find_existing_companion

            from .dossier_render import render_dossier

            companion_dir = pathlib.Path(data_dir) / "wines" / "tracked"
            path = _find_existing_companion(tracked_id, companion_dir)
            if path is not None and path.exists():
                companion_data = render_dossier(path)
        except Exception:
            pass

    return _TEMPLATES.TemplateResponse(
        request,
        "tracked_detail.html",
        context={
            "wine": wine,
            "prices_chart": prices_chart,
            "prices_table": prices_table,
            "companion": companion_data,
        },
    )


# ---- JSON API (Chart.js) -------------------------------------------------


async def api_volume(request: Request) -> JSONResponse:
    _, hours = _period(request)
    con = request.app.state.log_db
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_hourly_volume(con, hours, exclude_workbench=excl)
    return JSONResponse(data)


async def api_latency_hist(request: Request) -> JSONResponse:
    _, hours = _period(request)
    con = request.app.state.log_db
    tool = request.query_params.get("tool")
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_latency_histogram(con, hours, tool=tool, exclude_workbench=excl)
    return JSONResponse(data)


async def api_latency_ts(request: Request) -> JSONResponse:
    _, hours = _period(request)
    con = request.app.state.log_db
    tool = request.query_params.get("tool")
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_latency_timeseries(con, hours, tool=tool, exclude_workbench=excl)
    return JSONResponse(data)


async def api_top_tools(request: Request) -> JSONResponse:
    _, hours = _period(request)
    limit = int(request.query_params.get("limit", "10"))
    con = request.app.state.log_db
    excl = not _include_wb(request)
    async with request.app.state.db_lock:
        data = obs_q.get_tool_usage(con, hours, limit=limit, exclude_workbench=excl)
    return JSONResponse(data)


# ---- Refresh endpoint (manual fallback) -----------------------------------


async def api_refresh(request: Request) -> JSONResponse:
    """Reopen the DuckDB log connection as a manual refresh fallback."""
    app = request.app
    async with app.state.db_lock:
        try:
            app.state.log_db.close()
        except Exception:
            pass
        app.state.log_db = duckdb.connect(app.state.log_db_path, read_only=True)
    return JSONResponse({"status": "ok"})


# ---- SSE live tail --------------------------------------------------------


def _sse_message(html: str) -> str:
    """Format an HTML snippet as an SSE message event."""
    lines = html.replace("\n", "")
    return f"event: message\ndata: {lines}\n\n"


def _render_event_card(event: dict) -> str:
    """Render a single event as an HTML snippet for the live tail."""
    status_class = " error" if event["status"] == "error" else ""
    ts = event["started_at"]
    if hasattr(ts, "strftime"):
        ts = ts.strftime("%H:%M:%S.%f")[:-3]
    params_html = ""
    if event.get("parameters"):
        from html import escape

        params_html = (
            '<details style="margin:0;font-size:0.75rem;">'
            f'<summary style="cursor:pointer;">params</summary>'
            f'<pre style="margin:0;white-space:pre-wrap;word-break:break-word;">'
            f"{escape(str(event['parameters']))}</pre></details>"
        )
    error_html = ""
    if event.get("error_type"):
        from html import escape

        error_html = f' <span style="color:var(--pico-del-color);">{escape(str(event["error_type"]))}</span>'
    return (
        f'<div class="event-card{status_class}">'
        f'<span class="ts">{ts}</span> '
        f'<span class="type">{event["event_type"]}</span> '
        f'<span class="name">{event["name"]}</span> '
        f'<span class="status">{event["status"]}</span> '
        f'<span class="ms">{event["duration_ms"]:.0f} ms</span>'
        f"{error_html}{params_html}"
        f"</div>"
    )


def _render_turn_boundary() -> str:
    """Render a turn separator line for the live tail."""
    return '<div class="turn-boundary">── new turn ──</div>'


async def api_live(request: Request) -> StreamingResponse:
    """Server-Sent Events stream of new tool events."""
    con = request.app.state.log_db
    lock = request.app.state.db_lock
    filter_type = request.query_params.get("filter_type") or None
    filter_status = request.query_params.get("filter_status") or None
    excl = not _include_wb(request)
    # _max_polls: testing hook — limits poll iterations (None = infinite)
    _max_polls_raw = request.query_params.get("_max_polls")
    max_polls = int(_max_polls_raw) if _max_polls_raw is not None else None

    async def event_generator():
        last_id = None
        # Initial batch (last 20 events)
        async with lock:
            events = obs_q.get_recent_events(
                con,
                limit=20,
                exclude_workbench=excl,
                event_type=filter_type,
                status=filter_status,
            )
        if events:
            last_id = events[-1]["event_id"]
            prev_turn = None
            for event in events:
                if prev_turn and event["turn_id"] != prev_turn:
                    yield _sse_message(_render_turn_boundary())
                prev_turn = event["turn_id"]
                yield _sse_message(_render_event_card(event))

        # Poll for new events
        polls = 0
        while max_polls is None or polls < max_polls:
            await asyncio.sleep(1)
            if await request.is_disconnected():
                break
            try:
                async with lock:
                    events = obs_q.get_recent_events(
                        con,
                        after_id=last_id,
                        limit=50,
                        exclude_workbench=excl,
                        event_type=filter_type,
                        status=filter_status,
                    )
            except Exception:
                continue
            if not events:
                yield ": keepalive\n\n"
            else:
                prev_turn = None
                for event in events:
                    if prev_turn and event["turn_id"] != prev_turn:
                        yield _sse_message(_render_turn_boundary())
                    prev_turn = event["turn_id"]
                    yield _sse_message(_render_event_card(event))
                last_id = events[-1]["event_id"]
            polls += 1

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ---- Workbench routes -----------------------------------------------------


async def workbench_list(request: Request) -> HTMLResponse:
    settings = request.app.state.settings
    all_tools = wb.discover_tools()
    tools_list = wb.filter_tools(
        all_tools,
        read_only=settings.workbench_read_only,
        allow_list=settings.workbench_allow or None,
    )
    grouped: dict[str, list] = {}
    for t in tools_list:
        grouped.setdefault(t.category, []).append(t)
    return _TEMPLATES.TemplateResponse(
        request,
        "workbench_list.html",
        context={
            "grouped": grouped,
            "total": len(tools_list),
        },
    )


async def workbench_tool(request: Request) -> HTMLResponse:
    tool_name = request.path_params["tool"]
    settings = request.app.state.settings

    tool = wb.get_tool(tool_name)
    if tool is None:
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            context={"message": f"Tool '{tool_name}' not found."},
            status_code=404,
        )

    # Safety check — is this tool allowed?
    if tool.is_write:
        allow = settings.workbench_allow or []
        if settings.workbench_read_only and tool_name not in allow:
            return _TEMPLATES.TemplateResponse(
                request,
                "error.html",
                context={"message": f"Tool '{tool_name}' is a write tool and is not enabled."},
                status_code=403,
            )

    result = None
    if request.method == "POST":
        form = await request.form()
        params = {p["name"]: form.get(p["name"], "") for p in tool.parameters}
        result = await wb.execute_tool(tool, params)

    template = "partials/workbench_response.html" if _wants_partial(request) and result else "workbench_tool.html"

    return _TEMPLATES.TemplateResponse(
        request,
        template,
        context={
            "tool": tool,
            "result": result,
        },
    )


async def workbench_batch(request: Request) -> HTMLResponse:
    results = None
    selected = None

    if request.method == "POST":
        form = await request.form()
        seq_name = form.get("sequence")
        for seq in wb.BATCH_SEQUENCES:
            if seq.name == seq_name:
                selected = seq
                results = await wb.run_batch(seq)
                break

    return _TEMPLATES.TemplateResponse(
        request,
        "workbench_batch.html",
        context={
            "sequences": wb.BATCH_SEQUENCES,
            "selected": selected,
            "results": results,
        },
    )


# ---- Application factory --------------------------------------------------


def build_app(
    log_db_path: str,
    data_dir: str | None = None,
    dashboard_config: DashboardConfig | None = None,
) -> Starlette:
    from cellarbrain.settings import DashboardConfig

    app = Starlette(
        lifespan=_lifespan,
        routes=[
            # Observability pages
            Route("/", index),
            Route("/tools", tools),
            Route("/errors", errors),
            Route("/errors/export", errors_export),
            Route("/errors/{event_id}", error_detail),
            Route("/sessions", sessions),
            Route("/sessions/{session_id}", session_detail),
            Route("/sessions/{session_id}/{turn_id}", turn_events),
            Route("/latency", latency),
            Route("/live", live),
            # Cellar browser
            Route("/cellar", cellar),
            Route("/cellar/{wine_id:int}", wine_detail),
            Route("/bottles", cellar_bottles),
            Route("/drinking", drinking),
            Route("/pairing", pairing_page, methods=["GET", "POST"]),
            Route("/sql", sql_playground, methods=["GET", "POST"]),
            Route("/stats", stats),
            Route("/tracked", tracked),
            Route("/tracked/{id:int}", tracked_detail),
            # Workbench
            Route("/workbench", workbench_list),
            Route("/workbench/batch", workbench_batch, methods=["GET", "POST"]),
            Route("/workbench/{tool}", workbench_tool, methods=["GET", "POST"]),
            # JSON API
            Route("/api/volume", api_volume),
            Route("/api/latency-hist", api_latency_hist),
            Route("/api/latency-ts", api_latency_ts),
            Route("/api/top-tools", api_top_tools),
            Route("/api/refresh", api_refresh, methods=["POST"]),
            Route("/api/live", api_live),
            # Static files
            Mount("/static", StaticFiles(directory=str(_HERE / "static")), name="static"),
        ],
    )
    app.state.log_db_path = log_db_path
    app.state.data_dir = data_dir
    app.state.settings = dashboard_config or DashboardConfig()
    return app
