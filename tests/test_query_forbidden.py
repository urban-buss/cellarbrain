"""Tests for _FORBIDDEN_FUNCTIONS and _FORBIDDEN_KEYWORDS guards in query.validate_sql."""

import pytest

from cellarbrain.exceptions import QueryError
from cellarbrain.query import validate_sql


class TestForbiddenFunctions:
    """validate_sql must reject queries using file-access functions."""

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT * FROM read_parquet('/etc/passwd')",
            "SELECT * FROM read_csv('/tmp/secrets.csv')",
            "SELECT * FROM read_csv_auto('/tmp/data.csv')",
            "SELECT * FROM read_json('/tmp/data.json')",
            "SELECT * FROM read_json_auto('/tmp/data.json')",
            "SELECT * FROM parquet_scan('/data/*.parquet')",
            "SELECT * FROM parquet_metadata('/data/file.parquet')",
            "SELECT * FROM parquet_schema('/data/file.parquet')",
            "WITH x AS (SELECT * FROM read_parquet('x.parquet')) SELECT * FROM x",
            "SELECT * FROM query('SELECT 1')",
        ],
        ids=[
            "read_parquet",
            "read_csv",
            "read_csv_auto",
            "read_json",
            "read_json_auto",
            "parquet_scan",
            "parquet_metadata",
            "parquet_schema",
            "read_parquet_in_cte",
            "query_function",
        ],
    )
    def test_blocked(self, sql: str) -> None:
        with pytest.raises(QueryError, match="Forbidden SQL function"):
            validate_sql(sql)


class TestForbiddenKeywords:
    """validate_sql must reject queries containing dangerous keywords."""

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1; ATTACH DATABASE '/tmp/evil.db' AS evil",
            "SELECT * FROM (SELECT 1) WHERE COPY",
        ],
        ids=["multi_statement_attach", "copy_keyword"],
    )
    def test_blocked_multi_or_keyword(self, sql: str) -> None:
        with pytest.raises(QueryError):
            validate_sql(sql)

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT * FROM wines WHERE PRAGMA_col = 1",  # not word-boundary match
        ],
        ids=["pragma_substring_allowed"],
    )
    def test_substring_not_blocked(self, sql: str) -> None:
        # 'PRAGMA_col' contains PRAGMA as prefix but not as a standalone word
        validate_sql(sql)


class TestValidQueriesStillPass:
    """Ensure the hardening doesn't break legitimate queries."""

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT * FROM wines",
            "SELECT name, vintage FROM wines WHERE vintage > 2015",
            "WITH recent AS (SELECT * FROM wines WHERE vintage >= 2020) SELECT * FROM recent",
            "SELECT COUNT(*) FROM bottles",
        ],
    )
    def test_valid(self, sql: str) -> None:
        validate_sql(sql)
