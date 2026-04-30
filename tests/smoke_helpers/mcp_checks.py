"""MCP server integration tests over stdio transport.

Spawns ``cellarbrain mcp`` as a subprocess, connects via the MCP SDK
client, and exercises all 16 tools + key resources.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from . import CheckResult

# ---------------------------------------------------------------------------
# Tool test definitions
# ---------------------------------------------------------------------------

# (tool_name, kwargs, pass_check_description, pass_fn)
# pass_fn receives the text result and returns True if the check passes.


def _non_error(text: str) -> bool:
    return not text.startswith("Error:")


def _tool_specs() -> list[tuple[str, dict, str, object]]:
    """Return the list of (tool_name, kwargs, description, pass_fn) tuples."""
    return [
        (
            "query_cellar",
            {"sql": "SELECT count(*) AS n FROM wines"},
            "returns count > 0",
            lambda t: t.split("|")[-2].strip() != "0" if "|" in t else _non_error(t),
        ),
        ("cellar_stats", {}, "returns stats", lambda t: "bottle" in t.lower() or "total" in t.lower()),
        ("cellar_churn", {}, "returns churn data", _non_error),
        (
            "find_wine",
            {"query": "wine"},
            "finds at least one wine",
            lambda t: _non_error(t) and ("wine_id" in t.lower() or "|" in t),
        ),
        ("read_dossier", {"wine_id": 1}, "returns dossier with frontmatter", lambda t: "---" in t or "wine_id" in t),
        (
            "update_dossier",
            {
                "wine_id": 1,
                "section": "tasting_notes",
                "content": "[smoke-test] Automated smoke test — safe to overwrite",
                "agent_name": "smoketest",
            },
            "writes agent section",
            lambda t: "updated" in t.lower() or "Updated" in t,
        ),
        (
            "reload_data",
            {"mode": "sync"},
            "ETL sync completes or reports missing CSVs",
            lambda t: "completed" in t.lower() or "csv file not found" in t.lower(),
        ),
        ("pending_research", {"limit": 1}, "returns result", _non_error),
        ("pending_companion_research", {"limit": 1}, "returns result", _non_error),
        ("list_companion_dossiers", {}, "lists companions", _non_error),
    ]


def _resource_specs() -> list[tuple[str, str]]:
    """Return (resource_uri, description) pairs for resource checks."""
    return [
        ("wine://list", "wine list"),
        ("wine://cellar", "cellar wines"),
        ("cellar://stats", "cellar stats"),
        ("etl://last-run", "last ETL run"),
        ("etl://changes", "recent changes"),
        ("schema://views", "view schemas"),
    ]


# ---------------------------------------------------------------------------
# Async helper: connect and run all checks
# ---------------------------------------------------------------------------


async def _run_all_checks(exe_path: Path, output_dir: Path) -> list[CheckResult]:
    """Spawn MCP server, run tool + resource checks, return results."""
    from mcp import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    results: list[CheckResult] = []
    server_params = StdioServerParameters(
        command=str(exe_path),
        args=["-d", str(output_dir), "mcp"],
    )

    try:
        async with stdio_client(server_params) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()

                # --- Discover tracked wine IDs for dynamic tests ---
                tracked_wine_id = None
                try:
                    resp = await asyncio.wait_for(
                        session.call_tool("list_companion_dossiers", {}),
                        timeout=15,
                    )
                    text = _extract_text(resp)
                    # Try to find a tracked_wine_id in the output
                    import re

                    m = re.search(r"(?:tracked_wine_id|^\|\s*)(\d{5,})", text, re.MULTILINE)
                    if m:
                        tracked_wine_id = int(m.group(1))
                except Exception:
                    pass

                # --- Tool checks ---
                for tool_name, kwargs, desc, pass_fn in _tool_specs():
                    result = await _check_tool(session, tool_name, kwargs, desc, pass_fn)
                    results.append(result)

                # Dynamic tracked-wine tools (need a real ID)
                if tracked_wine_id:
                    dynamic_tools = [
                        ("read_companion_dossier", {"tracked_wine_id": tracked_wine_id}, "reads companion", _non_error),
                        (
                            "update_companion_dossier",
                            {
                                "tracked_wine_id": tracked_wine_id,
                                "section": "price_tracker",
                                "content": "[smoke-test] Automated smoke test",
                            },
                            "writes companion section",
                            lambda t: "updated" in t.lower() or "Updated" in t,
                        ),
                        (
                            "log_price",
                            {
                                "tracked_wine_id": tracked_wine_id,
                                "bottle_size_ml": 750,
                                "retailer_name": "Smoke Test Shop",
                                "price": 1.0,
                                "currency": "CHF",
                                "in_stock": True,
                                "vintage": 2020,
                            },
                            "records price",
                            lambda t: "recorded" in t.lower() or "updated" in t.lower(),
                        ),
                        ("tracked_wine_prices", {"tracked_wine_id": tracked_wine_id}, "returns prices", _non_error),
                        ("price_history", {"tracked_wine_id": tracked_wine_id}, "returns history", _non_error),
                    ]
                    for tool_name, kwargs, desc, pass_fn in dynamic_tools:
                        result = await _check_tool(session, tool_name, kwargs, desc, pass_fn)
                        results.append(result)
                else:
                    for name in (
                        "read_companion_dossier",
                        "update_companion_dossier",
                        "log_price",
                        "tracked_wine_prices",
                        "price_history",
                    ):
                        results.append(
                            CheckResult(
                                name=f"MCP tool: {name}",
                                passed=True,
                                details="skipped — no tracked wines in dataset",
                            )
                        )

                # wishlist_alerts (doesn't need tracked_wine_id arg)
                results.append(
                    await _check_tool(
                        session,
                        "wishlist_alerts",
                        {"days": 30},
                        "returns alerts",
                        _non_error,
                    )
                )

                # --- Sommelier tool checks (conditional on trained model) ---
                sommelier_model_exists = (Path(output_dir).parent / "models" / "sommelier" / "model").is_dir()
                sommelier_indexes_exist = (Path(output_dir) / "sommelier" / "wine.index").is_file()
                if sommelier_model_exists and sommelier_indexes_exist:
                    sommelier_tools = [
                        (
                            "suggest_wines",
                            {"food_query": "grilled lamb chops", "limit": 3},
                            "returns ranked wines",
                            lambda t: "| Rank |" in t and "| Score |" in t,
                        ),
                        (
                            "suggest_foods",
                            {"wine_id": 1, "limit": 3},
                            "returns ranked dishes",
                            lambda t: "| Rank |" in t and "| Dish |" in t,
                        ),
                        (
                            "suggest_foods",
                            {"wine_id": 999999, "limit": 1},
                            "returns error for invalid wine_id",
                            lambda t: t.startswith("Error:"),
                        ),
                    ]
                    for tool_name, kwargs, desc, pass_fn in sommelier_tools:
                        suffix = f" ({desc})" if tool_name == "suggest_foods" else ""
                        result = await _check_tool(session, tool_name, kwargs, desc, pass_fn)
                        if suffix:
                            result = CheckResult(
                                name=result.name + suffix, passed=result.passed, details=result.details
                            )
                        results.append(result)
                else:
                    for name in ("suggest_wines", "suggest_foods (happy)", "suggest_foods (error)"):
                        results.append(
                            CheckResult(
                                name=f"MCP tool: {name}",
                                passed=True,
                                details="skipped — sommelier model or indexes not available",
                            )
                        )

                # --- Resource checks ---
                for uri, desc in _resource_specs():
                    result = await _check_resource(session, uri, desc)
                    results.append(result)

    except BaseException as exc:
        # Server failed to start or crashed — catches ExceptionGroup on Python 3.14+
        results.append(
            CheckResult(
                name="MCP server connection",
                passed=False,
                details=f"server error: {exc}",
            )
        )

    return results


async def _check_tool(
    session: object,
    tool_name: str,
    kwargs: dict,
    desc: str,
    pass_fn: object,
) -> CheckResult:
    """Call a single MCP tool and evaluate the result."""
    try:
        resp = await asyncio.wait_for(
            session.call_tool(tool_name, kwargs),
            timeout=30,
        )
        text = _extract_text(resp)
        passed = pass_fn(text)
        detail = desc if passed else f"FAIL ({desc}): {text[:200]}"
        return CheckResult(name=f"MCP tool: {tool_name}", passed=passed, details=detail)
    except TimeoutError:
        return CheckResult(name=f"MCP tool: {tool_name}", passed=False, details="timeout (30s)")
    except Exception as exc:
        return CheckResult(name=f"MCP tool: {tool_name}", passed=False, details=str(exc)[:200])


async def _check_resource(session: object, uri: str, desc: str) -> CheckResult:
    """Read an MCP resource and verify non-empty content."""
    try:
        resp = await asyncio.wait_for(
            session.read_resource(uri),
            timeout=15,
        )
        # Resource responses vary — extract text content
        text = ""
        if hasattr(resp, "contents") and resp.contents:
            for item in resp.contents:
                if hasattr(item, "text"):
                    text += item.text
        elif isinstance(resp, str):
            text = resp
        passed = len(text.strip()) > 0
        detail = desc if passed else f"empty response for {uri}"
        return CheckResult(name=f"MCP resource: {uri}", passed=passed, details=detail)
    except TimeoutError:
        return CheckResult(name=f"MCP resource: {uri}", passed=False, details="timeout (15s)")
    except Exception as exc:
        return CheckResult(name=f"MCP resource: {uri}", passed=False, details=str(exc)[:200])


def _extract_text(resp: object) -> str:
    """Extract plain text from an MCP tool response."""
    if hasattr(resp, "content") and resp.content:
        parts = []
        for item in resp.content:
            if hasattr(item, "text"):
                parts.append(item.text)
        return "\n".join(parts)
    return str(resp)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_mcp_checks(exe_path: Path, output_dir: Path) -> list[CheckResult]:
    """Spawn the MCP server and exercise all tools + resources.

    Returns one ``CheckResult`` per tool/resource checked.
    """
    return asyncio.run(_run_all_checks(exe_path, output_dir))
