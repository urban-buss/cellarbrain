from __future__ import annotations

import pandas as pd


class QueryError(Exception):
    """SQL validation or execution error."""


class DataStaleError(Exception):
    """Parquet files missing or corrupted."""


def _to_md(df: pd.DataFrame) -> str:
    """Render a DataFrame as a Markdown table with NULLs as empty cells."""
    return df.astype(object).where(df.notna(), "").to_markdown(index=False)


def _to_plain(df: pd.DataFrame, style: str = "list") -> str:
    """Render a DataFrame as plain text (no Markdown) for iMessage delivery.

    Styles:
        list — numbered list, one row per item with key columns on one line
        kv — key-value pairs, one per line for each row
        compact — single line per row with middle-dot separators
    """
    if df.empty:
        return "*No results.*"

    clean = df.astype(object).where(df.notna(), "")
    cols = list(clean.columns)

    if style == "kv":
        lines: list[str] = []
        for _, row in clean.iterrows():
            for col in cols:
                val = row[col]
                if val != "":
                    lines.append(f"{col}: {val}")
            lines.append("")
        return "\n".join(lines).rstrip()

    if style == "compact":
        lines = []
        for i, (_, row) in enumerate(clean.iterrows(), 1):
            parts = [str(row[c]) for c in cols if row[c] != ""]
            lines.append(f"{i}. {' · '.join(parts)}")
        return "\n".join(lines)

    # Default: "list" — numbered with col: val pairs
    lines = []
    for i, (_, row) in enumerate(clean.iterrows(), 1):
        parts = [f"{c}: {row[c]}" for c in cols if row[c] != ""]
        lines.append(f"{i}. {', '.join(parts)}")
    return "\n".join(lines)


def _format_df(df: pd.DataFrame, fmt: str = "markdown", style: str = "list") -> str:
    """Dispatch DataFrame formatting based on format preference."""
    if fmt == "plain":
        return _to_plain(df, style=style)
    return _to_md(df)
