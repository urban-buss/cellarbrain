"""Tests for the tool workbench module."""

from __future__ import annotations


class TestDiscoverTools:
    def test_finds_tools(self):
        from cellarbrain.dashboard.workbench import discover_tools

        tools = discover_tools()
        assert len(tools) > 0
        names = [t.name for t in tools]
        assert "query_cellar" in names
        assert "find_wine" in names

    def test_all_tools_have_category(self):
        from cellarbrain.dashboard.workbench import discover_tools

        for t in discover_tools():
            assert t.category, f"Tool {t.name} has no category"

    def test_meta_param_excluded(self):
        from cellarbrain.dashboard.workbench import discover_tools

        for t in discover_tools():
            param_names = [p["name"] for p in t.parameters]
            assert "meta" not in param_names, f"Tool {t.name} should not expose 'meta' param"

    def test_write_tools_flagged(self):
        from cellarbrain.dashboard.workbench import discover_tools

        tools = discover_tools()
        write_names = {t.name for t in tools if t.is_write}
        assert "update_dossier" in write_names
        assert "reload_data" not in write_names

    def test_tools_sorted_by_category_then_name(self):
        from cellarbrain.dashboard.workbench import discover_tools

        tools = discover_tools()
        keys = [(t.category, t.name) for t in tools]
        assert keys == sorted(keys)


class TestFilterTools:
    def test_read_only_excludes_writes(self):
        from cellarbrain.dashboard.workbench import discover_tools, filter_tools

        all_tools = discover_tools()
        filtered = filter_tools(all_tools, read_only=True)
        assert all(not t.is_write for t in filtered)

    def test_allow_list_permits_specific_write(self):
        from cellarbrain.dashboard.workbench import discover_tools, filter_tools

        all_tools = discover_tools()
        filtered = filter_tools(all_tools, read_only=True, allow_list=["update_dossier"])
        names = [t.name for t in filtered]
        assert "update_dossier" in names
        # Other write tools still excluded
        assert "log_price" not in names

    def test_read_only_false_allows_all(self):
        from cellarbrain.dashboard.workbench import discover_tools, filter_tools

        all_tools = discover_tools()
        filtered = filter_tools(all_tools, read_only=False)
        assert len(filtered) == len(all_tools)


class TestGetTool:
    def test_existing_tool(self):
        from cellarbrain.dashboard.workbench import get_tool

        tool = get_tool("query_cellar")
        assert tool is not None
        assert tool.name == "query_cellar"

    def test_missing_tool(self):
        from cellarbrain.dashboard.workbench import get_tool

        assert get_tool("nonexistent") is None


class TestToolInfo:
    def test_parameter_types(self):
        from cellarbrain.dashboard.workbench import get_tool

        tool = get_tool("find_wine")
        assert tool is not None
        param_names = [p["name"] for p in tool.parameters]
        assert "query" in param_names
        query_param = next(p for p in tool.parameters if p["name"] == "query")
        assert query_param["required"] is True
        assert query_param["type"] == "str"

    def test_optional_param_has_default(self):
        from cellarbrain.dashboard.workbench import get_tool

        tool = get_tool("find_wine")
        limit_param = next(p for p in tool.parameters if p["name"] == "limit")
        assert limit_param["required"] is False


class TestCoerceParams:
    def test_int_coercion(self):
        from cellarbrain.dashboard.workbench import _coerce_params, get_tool

        tool = get_tool("find_wine")
        result = _coerce_params(tool, {"query": "barolo", "limit": "5"})
        assert result["limit"] == 5

    def test_bool_coercion(self):
        from cellarbrain.dashboard.workbench import _coerce_params, get_tool

        tool = get_tool("find_wine")
        result = _coerce_params(tool, {"query": "barolo", "fuzzy": "true"})
        assert result["fuzzy"] is True

    def test_empty_optional_skipped(self):
        from cellarbrain.dashboard.workbench import _coerce_params, get_tool

        tool = get_tool("find_wine")
        result = _coerce_params(tool, {"query": "barolo", "limit": ""})
        assert "limit" not in result

    def test_meta_passed_through(self):
        from cellarbrain.dashboard.workbench import _coerce_params, get_tool

        tool = get_tool("find_wine")
        result = _coerce_params(tool, {"query": "barolo", "meta": {"agent_name": "wb"}})
        assert result["meta"] == {"agent_name": "wb"}

    def test_unknown_param_ignored(self):
        from cellarbrain.dashboard.workbench import _coerce_params, get_tool

        tool = get_tool("find_wine")
        result = _coerce_params(tool, {"query": "barolo", "unknown_key": "val"})
        assert "unknown_key" not in result


class TestBatchSequences:
    def test_sequences_defined(self):
        from cellarbrain.dashboard.workbench import BATCH_SEQUENCES

        assert len(BATCH_SEQUENCES) > 0
        for seq in BATCH_SEQUENCES:
            assert seq.name
            assert len(seq.steps) > 0

    def test_all_batch_steps_reference_known_tools(self):
        from cellarbrain.dashboard.workbench import (
            _TOOL_CATEGORIES,
            BATCH_SEQUENCES,
        )

        for seq in BATCH_SEQUENCES:
            for step in seq.steps:
                assert step.tool_name in _TOOL_CATEGORIES, (
                    f"Batch step '{step.description}' references unknown tool: {step.tool_name}"
                )


class TestTypeLabel:
    def test_basic_types(self):
        from cellarbrain.dashboard.workbench import _type_label

        assert _type_label(str) == "str"
        assert _type_label(int) == "int"
        assert _type_label(float) == "float"
        assert _type_label(bool) == "bool"

    def test_optional_type(self):
        from cellarbrain.dashboard.workbench import _type_label

        assert _type_label(int | None) == "int?"
        assert _type_label(str | None) == "str?"
