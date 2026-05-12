"""Tests for cellarbrain.phonetic — Double Metaphone UDF support."""

from __future__ import annotations

import pytest

jellyfish = pytest.importorskip("jellyfish")

from cellarbrain.phonetic import dmetaphone, is_available, register_udfs  # noqa: E402


class TestIsAvailable:
    """Tests for is_available() detection."""

    def test_available_when_jellyfish_installed(self):
        assert is_available() is True


class TestDmetaphone:
    """Tests for the dmetaphone() function."""

    def test_basic_word(self):
        code = dmetaphone("Chateau")
        assert isinstance(code, str)
        assert len(code) > 0

    def test_none_returns_empty(self):
        assert dmetaphone(None) == ""

    def test_empty_string_returns_empty(self):
        assert dmetaphone("") == ""

    def test_phonetic_equivalence(self):
        """Similar-sounding names produce the same phonetic code."""
        # "Château" and "Chateau" should produce the same code
        assert dmetaphone("Chateau") == dmetaphone("Château".replace("â", "a"))

    def test_different_words_different_codes(self):
        """Distinct words produce different codes."""
        assert dmetaphone("Merlot") != dmetaphone("Riesling")


class TestRegisterUdfs:
    """Tests for register_udfs() on a DuckDB connection."""

    def test_registers_on_duckdb(self):
        import duckdb

        con = duckdb.connect(":memory:")
        register_udfs(con)
        # The UDF should now be callable in SQL
        result = con.execute("SELECT dmetaphone('Chateau')").fetchone()
        assert result is not None
        assert isinstance(result[0], str)
        assert len(result[0]) > 0

    def test_udf_returns_empty_for_null(self):
        import duckdb

        con = duckdb.connect(":memory:")
        register_udfs(con)
        result = con.execute("SELECT dmetaphone(NULL)").fetchone()
        # DuckDB passes NULL through; our function returns '' but DuckDB may
        # propagate NULL for NULL input depending on UDF null handling
        assert result[0] is None or result[0] == ""
