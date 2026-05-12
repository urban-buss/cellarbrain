"""Tests for cellarbrain.mcp_responses — ToolResponse and wire conversion."""

from __future__ import annotations

import json

from cellarbrain.mcp_responses import ToolResponse, data_size, to_call_tool_result

# ---------------------------------------------------------------------------
# ToolResponse basics
# ---------------------------------------------------------------------------


class TestToolResponse:
    """Core ToolResponse behaviour."""

    def test_is_str_subclass(self):
        r = ToolResponse("hello")
        assert isinstance(r, str)
        assert r == "hello"

    def test_string_operations(self):
        r = ToolResponse("## Cellar Stats\n42 wines")
        assert "Stats" in r
        assert r.startswith("##")
        assert len(r) > 0

    def test_data_and_metadata(self):
        r = ToolResponse("text", data={"wines": 42}, metadata={"v": 1})
        assert r.data == {"wines": 42}
        assert r.metadata == {"v": 1}

    def test_defaults_no_data(self):
        r = ToolResponse("text")
        assert r.data is None
        assert r.metadata == {}

    def test_has_structured_content_with_data(self):
        r = ToolResponse("x", data={"k": 1})
        assert r.has_structured_content is True

    def test_has_structured_content_with_metadata_only(self):
        r = ToolResponse("x", metadata={"k": 1})
        assert r.has_structured_content is True

    def test_has_structured_content_false(self):
        r = ToolResponse("x")
        assert r.has_structured_content is False

    def test_equality_ignores_payload(self):
        r1 = ToolResponse("hello", data={"a": 1})
        r2 = ToolResponse("hello", data={"b": 2})
        # As strings they are equal
        assert r1 == r2
        assert r1 == "hello"

    def test_empty_string(self):
        r = ToolResponse("")
        assert r == ""
        assert len(r) == 0


# ---------------------------------------------------------------------------
# Convenience constructors
# ---------------------------------------------------------------------------


class TestToolResponseError:
    """ToolResponse.error() factory."""

    def test_prefixes_error(self):
        r = ToolResponse.error("something went wrong")
        assert r == "Error: something went wrong"
        assert r.data == {"error": "something went wrong"}

    def test_does_not_double_prefix(self):
        r = ToolResponse.error("Error: already prefixed")
        assert r == "Error: already prefixed"

    def test_custom_data(self):
        r = ToolResponse.error("fail", data={"code": 42})
        assert r.data == {"code": 42}

    def test_metadata(self):
        r = ToolResponse.error("fail", metadata={"tool": "x"})
        assert r.metadata == {"tool": "x"}


class TestToolResponseTextOnly:
    """ToolResponse.text_only() factory."""

    def test_no_structured_content(self):
        r = ToolResponse.text_only("just text")
        assert r == "just text"
        assert r.has_structured_content is False

    def test_data_is_none(self):
        r = ToolResponse.text_only("x")
        assert r.data is None
        assert r.metadata == {}


# ---------------------------------------------------------------------------
# Wire conversion
# ---------------------------------------------------------------------------


class TestToCallToolResult:
    """to_call_tool_result() wire conversion."""

    def test_plain_str_passthrough(self):
        result = to_call_tool_result("plain text")
        assert result == "plain text"
        assert isinstance(result, str)

    def test_tool_response_without_payload_passthrough(self):
        r = ToolResponse("just text")
        result = to_call_tool_result(r)
        # No structured content → passthrough
        assert result == "just text"

    def test_tool_response_with_data_converts(self):
        from mcp.types import CallToolResult

        r = ToolResponse("## Result", data={"rows": [1, 2, 3]})
        result = to_call_tool_result(r)
        assert isinstance(result, CallToolResult)
        assert result.content[0].text == "## Result"
        assert result.structuredContent == {"data": {"rows": [1, 2, 3]}}
        assert result.isError is False

    def test_tool_response_with_metadata_converts(self):
        from mcp.types import CallToolResult

        r = ToolResponse("ok", metadata={"row_count": 5})
        result = to_call_tool_result(r)
        assert isinstance(result, CallToolResult)
        assert result.structuredContent == {"metadata": {"row_count": 5}}

    def test_tool_response_with_both(self):
        from mcp.types import CallToolResult

        r = ToolResponse("text", data={"a": 1}, metadata={"b": 2})
        result = to_call_tool_result(r)
        assert isinstance(result, CallToolResult)
        assert result.structuredContent == {"data": {"a": 1}, "metadata": {"b": 2}}

    def test_error_response_sets_is_error(self):
        from mcp.types import CallToolResult

        r = ToolResponse.error("bad query")
        result = to_call_tool_result(r)
        assert isinstance(result, CallToolResult)
        assert result.isError is True

    def test_non_error_response_clears_is_error(self):

        r = ToolResponse("good", data={"ok": True})
        result = to_call_tool_result(r)
        assert result.isError is False


# ---------------------------------------------------------------------------
# data_size helper
# ---------------------------------------------------------------------------


class TestDataSize:
    """data_size() byte counting."""

    def test_plain_str_returns_none(self):
        assert data_size("just text") is None

    def test_no_data_returns_none(self):
        r = ToolResponse("text")
        assert data_size(r) is None

    def test_with_data_returns_bytes(self):
        payload = {"wines": 42, "region": "Bordeaux"}
        r = ToolResponse("text", data=payload)
        expected = len(json.dumps(payload, ensure_ascii=False).encode())
        assert data_size(r) == expected

    def test_unicode_data(self):
        payload = {"name": "Château Léoville"}
        r = ToolResponse("text", data=payload)
        expected = len(json.dumps(payload, ensure_ascii=False).encode())
        assert data_size(r) == expected
        # UTF-8 multi-byte chars mean byte count > character count
        assert expected > len(json.dumps(payload, ensure_ascii=False))
