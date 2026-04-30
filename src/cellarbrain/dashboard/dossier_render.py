"""Dossier Markdown rendering for the web explorer."""

from __future__ import annotations

import pathlib
import re

import markdown


def render_dossier(dossier_path: pathlib.Path) -> dict:
    """Read a dossier .md file and render to structured HTML sections.

    Returns
    -------
    dict with keys:
        frontmatter: dict — parsed YAML frontmatter fields
        sections: list[dict] — [{heading, slug, html, populated}]
        raw: str — original Markdown source
    """
    text = dossier_path.read_text(encoding="utf-8")

    # Split frontmatter
    frontmatter: dict = {}
    body = text
    if text.startswith("---"):
        parts = text.split("---", 2)
        if len(parts) >= 3:
            import yaml

            frontmatter = yaml.safe_load(parts[1]) or {}
            body = parts[2]

    # Split by H2 headings
    section_pattern = re.compile(r"^## (.+)$", re.MULTILINE)
    splits = section_pattern.split(body)

    sections: list[dict] = []
    md = markdown.Markdown(extensions=["tables", "fenced_code", "toc"])

    i = 1  # skip preamble (text before first H2)
    while i < len(splits) - 1:
        heading = splits[i].strip()
        content = splits[i + 1]
        # Strip fence comments
        content = re.sub(r"<!--.*?-->", "", content, flags=re.DOTALL)
        slug = re.sub(r"[^a-z0-9]+", "-", heading.lower()).strip("-")
        html = md.convert(content)
        md.reset()
        sections.append(
            {
                "heading": heading,
                "slug": slug,
                "html": html,
                "populated": bool(html.strip()),
            }
        )
        i += 2

    return {
        "frontmatter": frontmatter,
        "sections": sections,
        "raw": text,
    }
