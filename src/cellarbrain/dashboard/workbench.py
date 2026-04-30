"""Tool Workbench — introspection, form generation, and execution."""

from __future__ import annotations

import inspect
import logging
import time
from dataclasses import dataclass, field

import anyio

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToolInfo:
    """Metadata for a single MCP tool function."""

    name: str
    func: object  # the callable
    description: str
    category: str
    parameters: list[dict] = field(default_factory=list)
    is_write: bool = False


# Category assignments for the tool list grouping
_TOOL_CATEGORIES: dict[str, str] = {
    "query_cellar": "Query & Search",
    "find_wine": "Query & Search",
    "cellar_info": "Query & Search",
    "cellar_stats": "Query & Search",
    "cellar_churn": "Query & Search",
    "search_synonyms": "Query & Search",
    "server_stats": "Query & Search",
    "read_dossier": "Dossier Management",
    "update_dossier": "Dossier Management",
    "pending_research": "Dossier Management",
    "read_companion_dossier": "Dossier Management",
    "update_companion_dossier": "Dossier Management",
    "list_companion_dossiers": "Dossier Management",
    "pending_companion_research": "Dossier Management",
    "log_price": "Price Tracking",
    "tracked_wine_prices": "Price Tracking",
    "price_history": "Price Tracking",
    "wishlist_alerts": "Price Tracking",
    "suggest_wines": "Sommelier",
    "suggest_foods": "Sommelier",
    "add_pairing": "Sommelier",
    "train_sommelier": "Sommelier",
    "reload_data": "Data Refresh",
}

# Write tools — excluded from workbench in read-only mode
_WRITE_TOOLS = {
    "update_dossier",
    "update_companion_dossier",
    "log_price",
    "search_synonyms",
    "add_pairing",
}


def discover_tools() -> list[ToolInfo]:
    """Introspect mcp_server module and return ToolInfo for known tools."""
    from cellarbrain import mcp_server

    tools: list[ToolInfo] = []
    for name in _TOOL_CATEGORIES:
        obj = getattr(mcp_server, name, None)
        if obj is None or not callable(obj):
            continue

        sig = inspect.signature(obj)
        doc = inspect.getdoc(obj) or ""
        first_line = doc.split("\n")[0] if doc else name

        params: list[dict] = []
        for pname, param in sig.parameters.items():
            if pname == "meta":
                continue  # auto-populated by workbench
            ptype = "str"
            if param.annotation != inspect.Parameter.empty:
                ptype = _type_label(param.annotation)
            default = None
            required = param.default is inspect.Parameter.empty
            if not required:
                default = param.default
            params.append(
                {
                    "name": pname,
                    "type": ptype,
                    "default": default,
                    "required": required,
                }
            )

        tools.append(
            ToolInfo(
                name=name,
                func=obj,
                description=first_line,
                category=_TOOL_CATEGORIES[name],
                parameters=params,
                is_write=name in _WRITE_TOOLS,
            )
        )
    return sorted(tools, key=lambda t: (t.category, t.name))


def _type_label(annotation: object) -> str:
    """Convert a type annotation to a display label."""
    # Handle string annotations (from `from __future__ import annotations`)
    if isinstance(annotation, str):
        s = annotation.strip()
        if s.endswith("| None"):
            base = s[: -len("| None")].strip()
            return base + "?"
        return s
    origin = getattr(annotation, "__origin__", None)
    if origin is not None:
        args = getattr(annotation, "__args__", ())
        if type(None) in args:
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return _type_label(non_none[0]) + "?"
        return str(annotation)
    if annotation is str:
        return "str"
    if annotation is int:
        return "int"
    if annotation is float:
        return "float"
    if annotation is bool:
        return "bool"
    if annotation is list:
        return "list"
    if annotation is dict:
        return "dict"
    return str(annotation)


def get_tool(name: str) -> ToolInfo | None:
    """Find a tool by name."""
    for t in discover_tools():
        if t.name == name:
            return t
    return None


def filter_tools(
    tools: list[ToolInfo],
    *,
    read_only: bool = True,
    allow_list: list[str] | None = None,
) -> list[ToolInfo]:
    """Apply safety rails — exclude write tools unless explicitly allowed."""
    result: list[ToolInfo] = []
    for t in tools:
        if t.is_write and read_only and (allow_list is None or t.name not in allow_list):
            continue
        result.append(t)
    return result


# ---------------------------------------------------------------------------
# Tool execution
# ---------------------------------------------------------------------------


async def execute_tool(tool: ToolInfo, params: dict) -> dict:
    """Execute a tool function and return structured result.

    Returns dict with keys: result, elapsed_ms, error, error_type.
    Async tools are run via anyio.to_thread to avoid event-loop nesting.
    """
    params["meta"] = {"agent_name": "workbench"}
    coerced = _coerce_params(tool, params)

    t0 = time.monotonic()
    try:
        fn = tool.func
        if inspect.iscoroutinefunction(fn):
            result = await fn(**coerced)
        else:
            result = await anyio.to_thread.run_sync(lambda: fn(**coerced))
        elapsed = round((time.monotonic() - t0) * 1000, 1)
        return {
            "result": result,
            "elapsed_ms": elapsed,
            "error": None,
            "error_type": None,
        }
    except Exception as exc:
        elapsed = round((time.monotonic() - t0) * 1000, 1)
        return {
            "result": None,
            "elapsed_ms": elapsed,
            "error": str(exc),
            "error_type": type(exc).__name__,
        }


def _coerce_params(tool: ToolInfo, raw: dict) -> dict:
    """Coerce string form values to the expected Python types."""
    coerced: dict = {}
    param_map = {p["name"]: p for p in tool.parameters}
    for key, value in raw.items():
        if key == "meta":
            coerced["meta"] = value
            continue
        spec = param_map.get(key)
        if spec is None:
            continue
        if value == "" or value is None:
            if not spec["required"]:
                continue  # skip optional empty params
            coerced[key] = value
            continue
        ptype = spec["type"].rstrip("?")
        if ptype == "int":
            coerced[key] = int(value)
        elif ptype == "float":
            coerced[key] = float(value)
        elif ptype == "bool":
            coerced[key] = value in ("true", "True", "1", True)
        elif ptype == "list":
            coerced[key] = [v.strip() for v in value.split(",")]
        else:
            coerced[key] = value
    return coerced


# ---------------------------------------------------------------------------
# Batch sequences
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BatchStep:
    """A single step in a batch sequence."""

    tool_name: str
    params: dict = field(default_factory=dict)
    description: str = ""


@dataclass(frozen=True)
class BatchSequence:
    """A named batch of tool invocations."""

    name: str
    description: str
    steps: list[BatchStep] = field(default_factory=list)


BATCH_SEQUENCES: list[BatchSequence] = [
    BatchSequence(
        name="Basic Smoke Test",
        description="cellar_info → cellar_stats → find_wine → query_cellar",
        steps=[
            BatchStep("cellar_info", {}, "Get cellar info"),
            BatchStep("cellar_stats", {"group_by": "country"}, "Stats by country"),
            BatchStep("find_wine", {"query": "red"}, "Search for red wines"),
            BatchStep("query_cellar", {"sql": "SELECT COUNT(*) AS total FROM wines"}, "Count wines"),
        ],
    ),
    BatchSequence(
        name="Research Workflow",
        description="pending_research → read_dossier for first result",
        steps=[
            BatchStep("pending_research", {"limit": 3}, "Find pending wines"),
            BatchStep("read_dossier", {"wine_id": 1, "sections": ["producer_profile"]}, "Read first dossier"),
        ],
    ),
    BatchSequence(
        name="Search Coverage",
        description="10 diverse find_wine queries",
        steps=[
            BatchStep("find_wine", {"query": "barolo"}, "Search: barolo"),
            BatchStep("find_wine", {"query": "swiss white"}, "Search: swiss white"),
            BatchStep("find_wine", {"query": "under 30"}, "Search: under 30"),
            BatchStep("find_wine", {"query": "ready to drink"}, "Search: ready to drink"),
            BatchStep("find_wine", {"query": "champagne"}, "Search: champagne"),
            BatchStep("find_wine", {"query": "rioja reserva"}, "Search: rioja reserva"),
            BatchStep("find_wine", {"query": "pinot noir"}, "Search: pinot noir"),
            BatchStep("find_wine", {"query": "italy"}, "Search: italy"),
            BatchStep("find_wine", {"query": "rosé"}, "Search: rosé"),
            BatchStep("find_wine", {"query": "2020 vintage"}, "Search: 2020 vintage"),
        ],
    ),
    BatchSequence(
        name="Price Tracking Flow",
        description="wishlist_alerts → tracked_wine_prices → price_history",
        steps=[
            BatchStep("wishlist_alerts", {}, "Check alerts"),
        ],
    ),
]


async def run_batch(sequence: BatchSequence) -> list[dict]:
    """Execute a batch sequence and return per-step results."""
    results: list[dict] = []
    for step in sequence.steps:
        tool = get_tool(step.tool_name)
        if tool is None:
            results.append(
                {
                    "step": step.description,
                    "tool": step.tool_name,
                    "result": None,
                    "elapsed_ms": 0,
                    "error": f"Tool not found: {step.tool_name}",
                    "error_type": "ToolNotFound",
                    "status": "error",
                }
            )
            continue
        outcome = await execute_tool(tool, dict(step.params))
        results.append(
            {
                "step": step.description,
                "tool": step.tool_name,
                "result": outcome["result"],
                "elapsed_ms": outcome["elapsed_ms"],
                "error": outcome["error"],
                "error_type": outcome["error_type"],
                "status": "error" if outcome["error"] else "ok",
            }
        )
    return results
