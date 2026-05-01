"""Validate referential integrity, PK uniqueness, and data quality.

All checks run via DuckDB queries on Parquet files so they are
declarative and easy to extend.
"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass, field

import duckdb


@dataclass
class ValidationResult:
    """Accumulates pass/fail checks."""

    checks: list[dict] = field(default_factory=list)

    def add(self, name: str, passed: bool, detail: str = "") -> None:
        self.checks.append({"name": name, "passed": passed, "detail": detail})

    @property
    def ok(self) -> bool:
        return all(c["passed"] for c in self.checks)

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c["passed"])

    @property
    def failed(self) -> int:
        return sum(1 for c in self.checks if not c["passed"])

    def summary(self) -> str:
        lines = []
        for c in self.checks:
            status = "PASS" if c["passed"] else "FAIL"
            line = f"[{status}] {c['name']}"
            if c["detail"]:
                line += f" — {c['detail']}"
            lines.append(line)
        lines.append(f"\n{self.passed} passed, {self.failed} failed")
        return "\n".join(lines)


def _parquet(d: pathlib.Path, name: str) -> str:
    """Return a DuckDB-friendly path string for a Parquet file."""
    return str(d / f"{name}.parquet").replace("\\", "/")


def validate(output_dir: str | pathlib.Path) -> ValidationResult:
    """Run all validation checks on the Parquet files in *output_dir*."""
    d = pathlib.Path(output_dir)
    result = ValidationResult()
    con = duckdb.connect(":memory:")

    try:
        # Register views for Parquet files that exist
        for name in [
            "wine",
            "tracked_wine",
            "bottle",
            "winery",
            "appellation",
            "grape",
            "wine_grape",
            "tasting",
            "pro_rating",
            "cellar",
            "provider",
            "etl_run",
            "change_log",
        ]:
            pf = d / f"{name}.parquet"
            if pf.exists():
                p = _parquet(d, name)
                con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{p}')")

        # Year-partitioned tables (glob pattern)
        price_files = list(d.glob("price_observation_*.parquet"))
        if price_files:
            glob_path = str(d / "price_observation_*.parquet").replace("\\", "/")
            con.execute(f"CREATE VIEW price_observation AS SELECT * FROM read_parquet('{glob_path}')")

        _run_checks(con, result)
    finally:
        con.close()

    return result


def _run_checks(con: duckdb.DuckDBPyConnection, result: ValidationResult) -> None:
    """Run all validation checks using named views (no raw file paths in SQL)."""

    # ------------------------------------------------------------------
    # Helper: run a check that expects count == 0 for "no violations"
    # ------------------------------------------------------------------
    def check_zero(name: str, sql: str) -> None:
        try:
            count = con.execute(sql).fetchone()[0]
        except duckdb.CatalogException:
            result.add(name, True, "skipped — table not present")
            return
        result.add(name, count == 0, f"{count} violations" if count else "")

    # ------------------------------------------------------------------
    # 1. Referential integrity checks
    # ------------------------------------------------------------------
    fk_checks = [
        (
            "FK wine.winery_id → winery",
            """SELECT count(*) FROM wine w
                WHERE w.winery_id IS NOT NULL
                  AND NOT w.is_deleted
                  AND w.winery_id NOT IN (SELECT winery_id FROM winery)""",
        ),
        (
            "FK wine.appellation_id → appellation",
            """SELECT count(*) FROM wine w
                WHERE w.appellation_id IS NOT NULL
                  AND NOT w.is_deleted
                  AND w.appellation_id NOT IN (SELECT appellation_id FROM appellation)""",
        ),
        (
            "FK wine_grape.wine_id → wine",
            """SELECT count(*) FROM wine_grape wg
                WHERE wg.wine_id NOT IN (SELECT wine_id FROM wine)""",
        ),
        (
            "FK wine_grape.grape_id → grape",
            """SELECT count(*) FROM wine_grape wg
                WHERE wg.grape_id NOT IN (SELECT grape_id FROM grape)""",
        ),
        (
            "FK bottle.wine_id → wine",
            """SELECT count(*) FROM bottle b
                WHERE b.wine_id NOT IN (SELECT wine_id FROM wine)""",
        ),
        (
            "FK bottle.cellar_id → cellar",
            """SELECT count(*) FROM bottle b
                WHERE b.cellar_id IS NOT NULL
                  AND b.cellar_id NOT IN (SELECT cellar_id FROM cellar)""",
        ),
        (
            "FK bottle.provider_id → provider",
            """SELECT count(*) FROM bottle b
                WHERE b.provider_id IS NOT NULL
                  AND b.provider_id NOT IN (SELECT provider_id FROM provider)""",
        ),
        (
            "FK tasting.wine_id → wine",
            """SELECT count(*) FROM tasting t
                WHERE t.wine_id NOT IN (SELECT wine_id FROM wine)""",
        ),
        (
            "FK pro_rating.wine_id → wine",
            """SELECT count(*) FROM pro_rating p
                WHERE p.wine_id NOT IN (SELECT wine_id FROM wine)""",
        ),
        (
            "FK tracked_wine.winery_id → winery",
            """SELECT count(*) FROM tracked_wine tw
                WHERE NOT tw.is_deleted
                  AND tw.winery_id NOT IN (SELECT winery_id FROM winery)""",
        ),
        (
            "FK tracked_wine.appellation_id → appellation",
            """SELECT count(*) FROM tracked_wine tw
                WHERE NOT tw.is_deleted
                  AND tw.appellation_id IS NOT NULL
                  AND tw.appellation_id NOT IN (SELECT appellation_id FROM appellation)""",
        ),
        (
            "FK wine.tracked_wine_id → tracked_wine",
            """SELECT count(*) FROM wine w
                WHERE NOT w.is_deleted
                  AND w.tracked_wine_id IS NOT NULL
                  AND w.tracked_wine_id NOT IN (SELECT tracked_wine_id FROM tracked_wine)""",
        ),
    ]

    for name, sql in fk_checks:
        check_zero(name, sql)

    # ------------------------------------------------------------------
    # 2. Primary key uniqueness / non-null checks
    # ------------------------------------------------------------------
    pk_tables = [
        ("winery", "winery_id"),
        ("appellation", "appellation_id"),
        ("grape", "grape_id"),
        ("wine", "wine_id"),
        ("bottle", "bottle_id"),
        ("cellar", "cellar_id"),
        ("provider", "provider_id"),
        ("tasting", "tasting_id"),
        ("pro_rating", "rating_id"),
        ("tracked_wine", "tracked_wine_id"),
    ]

    for table, pk in pk_tables:
        check_zero(
            f"PK unique: {table}.{pk}",
            f"SELECT count(*) - count(DISTINCT {pk}) FROM {table}",
        )
        check_zero(
            f"PK not null: {table}.{pk}",
            f"SELECT count(*) FROM {table} WHERE {pk} IS NULL",
        )

    # Composite PK for wine_grape
    check_zero(
        "PK unique: wine_grape (wine_id, grape_id)",
        """SELECT count(*) FROM (
            SELECT wine_id, grape_id, count(*) AS cnt
            FROM wine_grape GROUP BY wine_id, grape_id HAVING cnt > 1
        )""",
    )

    # Natural key uniqueness
    uniqueness_checks = [
        ("winery.name unique", "SELECT count(*) - count(DISTINCT name) FROM winery"),
        ("grape.name unique", "SELECT count(*) - count(DISTINCT name) FROM grape"),
        ("cellar.name unique", "SELECT count(*) - count(DISTINCT name) FROM cellar"),
        ("provider.name unique", "SELECT count(*) - count(DISTINCT name) FROM provider"),
        (
            "appellation natural key unique",
            """SELECT count(*) - count(DISTINCT (country, region, subregion, classification))
                FROM appellation""",
        ),
        (
            "tracked_wine natural key unique",
            """SELECT count(*) FROM (
                SELECT winery_id, wine_name, count(*) AS cnt
                FROM tracked_wine WHERE NOT is_deleted
                GROUP BY winery_id, wine_name HAVING cnt > 1
            )""",
        ),
    ]

    for name, sql in uniqueness_checks:
        check_zero(name, sql)

    # ------------------------------------------------------------------
    # 3. Data quality checks
    # ------------------------------------------------------------------

    check_zero(
        "wine.category in allowed values",
        """SELECT count(*) FROM wine
            WHERE category NOT IN ('red', 'white', 'rose', 'sparkling', 'dessert', 'fortified')
              AND category IS NOT NULL""",
    )

    check_zero(
        "bottle.acquisition_type in allowed values",
        """SELECT count(*) FROM bottle
            WHERE acquisition_type NOT IN ('market_price', 'discount_price', 'present', 'free')
              AND acquisition_type IS NOT NULL""",
    )

    check_zero(
        "wine_grape.percentage 0..100",
        """SELECT count(*) FROM wine_grape
            WHERE percentage IS NOT NULL AND (percentage < 0 OR percentage > 100)""",
    )

    check_zero(
        "pro_rating.score <= max_score",
        """SELECT count(*) FROM pro_rating
            WHERE score > max_score""",
    )

    check_zero(
        "wine drink window order",
        """SELECT count(*) FROM wine
            WHERE drink_from IS NOT NULL AND drink_until IS NOT NULL
              AND drink_from > drink_until""",
    )

    # Bottle status / output consistency
    check_zero(
        "bottle.status in allowed values",
        """SELECT count(*) FROM bottle
            WHERE status NOT IN ('stored', 'drunk', 'offered', 'removed')""",
    )

    check_zero(
        "bottle.output_type in allowed values",
        """SELECT count(*) FROM bottle
            WHERE output_type IS NOT NULL
              AND output_type NOT IN ('drunk', 'offered', 'removed')""",
    )

    check_zero(
        "stored bottles have no output_date",
        """SELECT count(*) FROM bottle
            WHERE status = 'stored' AND output_date IS NOT NULL""",
    )

    check_zero(
        "gone bottles have output_date",
        """SELECT count(*) FROM bottle
            WHERE status != 'stored' AND output_date IS NULL""",
    )

    check_zero(
        "bottle.output_date >= purchase_date",
        """SELECT count(*) FROM bottle
            WHERE output_date IS NOT NULL AND purchase_date IS NOT NULL
              AND output_date < purchase_date""",
    )

    check_zero(
        "no stored bottles for deleted wines",
        """SELECT count(*) FROM bottle b
            JOIN wine w ON b.wine_id = w.wine_id
            WHERE w.is_deleted AND b.status = 'stored'""",
    )

    # Currency normalisation checks
    check_zero(
        "purchase_currency uniform (single default)",
        """SELECT GREATEST(count(DISTINCT purchase_currency) - 1, 0) FROM bottle
            WHERE purchase_currency IS NOT NULL""",
    )

    check_zero(
        "list_currency uniform (single default)",
        """SELECT GREATEST(count(DISTINCT list_currency) - 1, 0) FROM wine
            WHERE list_currency IS NOT NULL""",
    )

    # ------------------------------------------------------------------
    # 3b. Price observation checks
    # ------------------------------------------------------------------

    check_zero(
        "FK price_observation.tracked_wine_id → tracked_wine",
        """SELECT count(*) FROM price_observation po
            WHERE po.tracked_wine_id NOT IN (SELECT tracked_wine_id FROM tracked_wine)""",
    )

    check_zero(
        "price_observation.price >= 0",
        """SELECT count(*) FROM price_observation
            WHERE price < 0""",
    )

    check_zero(
        "price_observation.bottle_size_ml > 0",
        """SELECT count(*) FROM price_observation
            WHERE bottle_size_ml <= 0""",
    )

    # ------------------------------------------------------------------
    # 4. Row count sanity
    # ------------------------------------------------------------------
    try:
        wine_count = con.execute("SELECT count(*) FROM wine").fetchone()[0]
        result.add("wine row count > 0", wine_count > 0, f"{wine_count} wines")
    except duckdb.CatalogException:
        result.add("wine row count > 0", False, "wine.parquet not found")

    try:
        bottle_count = con.execute("SELECT count(*) FROM bottle").fetchone()[0]
        result.add("bottle row count > 0", bottle_count > 0, f"{bottle_count} bottles")
    except duckdb.CatalogException:
        result.add("bottle row count > 0", False, "bottle.parquet not found")
