from __future__ import annotations

import pandas as pd


class QueryError(Exception):
    """SQL validation or execution error."""


class DataStaleError(Exception):
    """Parquet files missing or corrupted."""


def _to_md(df: pd.DataFrame) -> str:
    """Render a DataFrame as a Markdown table with NULLs as empty cells."""
    return df.astype(object).where(df.notna(), "").to_markdown(index=False)
