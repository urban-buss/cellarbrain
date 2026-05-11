"""Tests for plain-text (iMessage-friendly) output formatting."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from cellarbrain._query_base import _format_df, _to_plain
from dataset_factory import (
    make_appellation,
    make_bottle,
    make_cellar,
    make_change_log,
    make_etl_run,
    make_grape,
    make_pro_rating,
    make_provider,
    make_tasting,
    make_wine,
    make_wine_grape,
    make_winery,
    write_dataset,
)


def _make_dataset(tmp_path):
    """Minimal dataset for plain format tests."""
    wines = [
        make_wine(
            wine_id=1,
            winery_id=1,
            winery_name="Château Test",
            name="Cuvée Alpha",
            vintage=2020,
            appellation_id=1,
            drinking_status="optimal",
        ),
        make_wine(
            wine_id=2,
            winery_id=2,
            winery_name="Bodega Ejemplo",
            name="Reserva",
            vintage=2018,
            appellation_id=2,
            primary_grape="Tempranillo",
            drinking_status="drinkable",
        ),
    ]
    bottles = [
        make_bottle(1, 1, purchase_date=date(2023, 1, 10), purchase_price=Decimal("25.00")),
        make_bottle(2, 1, shelf="A2", bottle_number=2, purchase_date=date(2023, 6, 1), purchase_price=Decimal("25.00")),
        make_bottle(3, 2, purchase_date=date(2024, 3, 1), purchase_price=Decimal("18.00")),
    ]
    return write_dataset(
        tmp_path,
        {
            "winery": [make_winery(1, name="Château Test"), make_winery(2, name="Bodega Ejemplo")],
            "appellation": [
                make_appellation(1),
                make_appellation(2, country="Spain", region="Rioja"),
            ],
            "grape": [make_grape(1), make_grape(2, name="Tempranillo")],
            "wine": wines,
            "wine_grape": [make_wine_grape(1, 1), make_wine_grape(2, 2)],
            "bottle": bottles,
            "cellar": [make_cellar(name="Main Cellar")],
            "provider": [make_provider(1, name="Wine Shop")],
            "tasting": [make_tasting()],
            "pro_rating": [make_pro_rating()],
            "etl_run": [make_etl_run()],
            "change_log": [make_change_log()],
        },
    )


@pytest.fixture()
def data_dir(tmp_path):
    return _make_dataset(tmp_path)


class TestToPlain:
    """Unit tests for _to_plain()."""

    def test_empty_dataframe(self):
        df = pd.DataFrame({"a": []})
        assert _to_plain(df) == "*No results.*"

    def test_list_style(self):
        df = pd.DataFrame({"wine": ["Barolo", "Chianti"], "bottles": [3, 5]})
        result = _to_plain(df, style="list")
        assert "1. wine: Barolo, bottles: 3" in result
        assert "2. wine: Chianti, bottles: 5" in result

    def test_compact_style(self):
        df = pd.DataFrame({"wine": ["Barolo"], "region": ["Piedmont"], "bottles": [3]})
        result = _to_plain(df, style="compact")
        assert "1. Barolo · Piedmont · 3" in result

    def test_kv_style(self):
        df = pd.DataFrame({"wine": ["Barolo"], "vintage": [2018]})
        result = _to_plain(df, style="kv")
        assert "wine: Barolo" in result
        assert "vintage: 2018" in result

    def test_null_values_omitted(self):
        df = pd.DataFrame({"wine": ["Barolo"], "score": [None]})
        result = _to_plain(df, style="list")
        assert "score" not in result

    def test_no_markdown_pipes(self):
        df = pd.DataFrame({"wine": ["Test"], "bottles": [1]})
        result = _to_plain(df, style="list")
        assert "|" not in result


class TestFormatDf:
    """Tests for _format_df dispatcher."""

    def test_markdown_returns_table(self):
        df = pd.DataFrame({"wine": ["Barolo"], "bottles": [3]})
        result = _format_df(df, fmt="markdown")
        assert "|" in result
        assert "wine" in result

    def test_plain_returns_numbered_list(self):
        df = pd.DataFrame({"wine": ["Barolo"], "bottles": [3]})
        result = _format_df(df, fmt="plain")
        assert "|" not in result
        assert "1." in result

    def test_plain_compact(self):
        df = pd.DataFrame({"a": ["x"], "b": ["y"]})
        result = _format_df(df, fmt="plain", style="compact")
        assert "·" in result


class TestQueryPlainFormat:
    """Tests for query.py functions with fmt='plain'."""

    def test_execute_query_plain(self, data_dir):
        from cellarbrain.query import execute_query, get_agent_connection

        con = get_agent_connection(data_dir)
        result = execute_query(con, "SELECT wine_name, vintage FROM wines LIMIT 2", fmt="plain")
        assert "|" not in result
        assert "1." in result

    def test_cellar_stats_plain(self, data_dir):
        from cellarbrain.query import cellar_stats, get_agent_connection

        con = get_agent_connection(data_dir)
        result = cellar_stats(con, fmt="plain")
        # Should not contain markdown table pipes
        assert "---|" not in result
        # Should contain emoji summary
        assert "🍷" in result or "CELLAR SUMMARY" in result

    def test_cellar_stats_grouped_plain(self, data_dir):
        from cellarbrain.query import cellar_stats, get_agent_connection

        con = get_agent_connection(data_dir)
        result = cellar_stats(con, group_by="category", fmt="plain")
        assert "---|" not in result
        assert "BY CATEGORY" in result

    def test_cellar_churn_plain(self, data_dir):
        from cellarbrain.query import cellar_churn, get_agent_connection

        con = get_agent_connection(data_dir)
        result = cellar_churn(con, fmt="plain")
        assert "---|" not in result
        assert "CELLAR CHURN" in result


class TestSearchPlainFormat:
    """Tests for search.py find_wine with fmt='plain'."""

    def test_find_wine_plain(self, data_dir):
        from cellarbrain.query import find_wine, get_agent_connection

        con = get_agent_connection(data_dir)
        result = find_wine(con, "Test", fmt="plain")
        if "No wines found" not in result:
            assert "|" not in result


class TestPairingPlainFormat:
    """Tests for pairing.py formatters with fmt='plain'."""

    def test_format_table_plain(self):
        from cellarbrain.pairing import PairingCandidate, format_table

        candidates = [
            PairingCandidate(
                wine_id=1,
                wine_name="Test Wine",
                vintage=2020,
                category="red",
                region="Piedmont",
                country="Italy",
                primary_grape="Nebbiolo",
                bottles_stored=3,
                best_pro_score=95.0,
                match_signals=["grape:nebbiolo", "region:piedmont"],
                price=25.0,
                drinking_status="optimal",
            )
        ]
        result = format_table(candidates, fmt="plain")
        assert "|" not in result
        assert "Test Wine" in result
        assert "1." in result

    def test_format_explained_plain(self):
        from cellarbrain.pairing import PairingCandidate, format_explained

        candidates = [
            PairingCandidate(
                wine_id=1,
                wine_name="Test Wine",
                vintage=2020,
                category="red",
                region="Piedmont",
                country="Italy",
                primary_grape="Nebbiolo",
                bottles_stored=3,
                best_pro_score=95.0,
                match_signals=["grape:nebbiolo"],
                price=25.0,
                drinking_status="optimal",
            )
        ]
        result = format_explained(candidates, "grilled steak", fmt="plain")
        # Should not have bold markers
        assert "**" not in result
        assert "Test Wine" in result
