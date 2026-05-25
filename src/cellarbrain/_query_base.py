from __future__ import annotations

import pandas as pd

from .exceptions import DataStaleError, QueryError

__all__ = ["DataStaleError", "QueryError", "_format_df", "_to_md", "_to_plain"]


def _to_md(df: pd.DataFrame) -> str:
    """Render a DataFrame as a Markdown table with NULLs as empty cells."""
    return df.astype(object).where(df.notna(), "").to_markdown(index=False)


def _to_aligned(df: pd.DataFrame) -> str:
    """Render a DataFrame as a space-aligned monospace code block."""
    if df.empty:
        return "*No results.*"

    clean = df.astype(object).where(df.notna(), "")
    cols = list(clean.columns)

    # Compute column widths
    widths = [len(c) for c in cols]
    for _, row in clean.iterrows():
        for i, c in enumerate(cols):
            widths[i] = max(widths[i], len(str(row[c])))

    # Build header + rows
    lines: list[str] = []
    header = "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols))
    lines.append(header)
    for _, row in clean.iterrows():
        line = "  ".join(str(row[c]).ljust(widths[i]) for i, c in enumerate(cols))
        lines.append(line)

    return "```text\n" + "\n".join(lines) + "\n```"


def _to_plain(df: pd.DataFrame, style: str = "list") -> str:
    """Render a DataFrame as plain text (no Markdown) for iMessage delivery.

    Styles:
        list — numbered list, one row per item with key columns on one line
        kv — key-value pairs, one per line for each row
        compact — single line per row with middle-dot separators
        aligned — space-aligned monospace code block
        auto — pick style based on row/column count (iMessage heuristic)
    """
    if df.empty:
        return "*No results.*"

    if style == "auto":
        nrows = len(df)
        ncols = len(df.columns)
        if nrows <= 5:
            style = "kv"
        elif nrows <= 15 and ncols <= 4:
            style = "aligned"
        else:
            # Large table: summary + first rows
            return _to_plain_summary(df)

    if style == "aligned":
        return _to_aligned(df)

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


def _to_plain_summary(df: pd.DataFrame, max_rows: int = 10) -> str:
    """Summarise a large DataFrame: show first N rows + count."""
    total = len(df)
    shown = min(max_rows, total)
    head = df.head(shown)
    body = _to_aligned(head)
    if total > shown:
        body += f"\n\n…and {total - shown} more rows."
    return body


def _format_df(df: pd.DataFrame, fmt: str = "markdown", style: str = "list") -> str:
    """Dispatch DataFrame formatting based on format preference."""
    if fmt == "plain":
        return _to_plain(df, style=style)
    return _to_md(df)
