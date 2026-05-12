"""Typed MCP tool response container.

Provides a str-subclass that carries optional structured ``data`` and
``metadata`` alongside the human-readable text body.  Because it IS a
str, existing consumers (tests, agents, dashboard) work unchanged.

When a ToolResponse has populated ``data`` or ``metadata``, the MCP
wire wrapper converts it into a ``CallToolResult`` with both text
content and ``structuredContent``.
"""

from __future__ import annotations

import json
from typing import Any


class ToolResponse(str):
    """MCP tool result carrying optional structured payload.

    Inherits from ``str`` so all string operations (``in``, ``startswith``,
    ``len``, formatting, equality) work transparently.  The ``data`` and
    ``metadata`` attributes travel alongside the text.

    Examples:
        >>> r = ToolResponse("## Stats\\n...", data={"wines": 42})
        >>> "Stats" in r
        True
        >>> r.data
        {'wines': 42}
    """

    data: dict[str, Any] | None
    metadata: dict[str, Any]

    def __new__(
        cls,
        text: str = "",
        *,
        data: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ToolResponse:
        instance = super().__new__(cls, text)
        instance.data = data
        instance.metadata = metadata or {}
        return instance

    # ------------------------------------------------------------------
    # Convenience constructors
    # ------------------------------------------------------------------

    @classmethod
    def error(
        cls,
        message: str,
        *,
        data: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ToolResponse:
        """Create an error response prefixed with ``Error:``."""
        text = f"Error: {message}" if not message.startswith("Error:") else message
        error_data = data if data is not None else {"error": message}
        return cls(text, data=error_data, metadata=metadata)

    @classmethod
    def text_only(cls, text: str) -> ToolResponse:
        """Create a response with text but no structured payload."""
        return cls(text, data=None, metadata=None)

    # ------------------------------------------------------------------
    # Wire conversion
    # ------------------------------------------------------------------

    @property
    def has_structured_content(self) -> bool:
        """True when this response carries structured data worth serialising."""
        return self.data is not None or bool(self.metadata)


def to_call_tool_result(resp: ToolResponse | str) -> Any:
    """Convert a tool response to a ``CallToolResult`` if it has structured content.

    Returns the original string unchanged when there is nothing to wrap.
    This keeps the FastMCP default text-only serialisation path for tools
    that haven't been migrated or that return plain strings.
    """
    from mcp.types import CallToolResult, TextContent

    if not isinstance(resp, ToolResponse) or not resp.has_structured_content:
        return resp

    structured: dict[str, Any] = {}
    if resp.data is not None:
        structured["data"] = resp.data
    if resp.metadata:
        structured["metadata"] = resp.metadata

    return CallToolResult(
        content=[TextContent(type="text", text=str(resp))],
        structuredContent=structured,
        isError=str(resp).startswith("Error:"),
    )


def data_size(resp: ToolResponse | str) -> int | None:
    """Return JSON-serialised byte count of the structured ``data``, or None."""
    if not isinstance(resp, ToolResponse) or resp.data is None:
        return None
    return len(json.dumps(resp.data, ensure_ascii=False).encode())
