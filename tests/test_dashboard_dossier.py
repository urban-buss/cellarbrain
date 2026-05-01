"""Tests for dossier Markdown rendering."""

from __future__ import annotations

from cellarbrain.dashboard.dossier_render import render_dossier


class TestRenderDossier:
    def test_basic_rendering(self, tmp_path):
        md = tmp_path / "test.md"
        md.write_text(
            "---\nwine_id: 1\n---\n\n## Identity\nTest wine\n\n## Notes\nGood wine\n",
            encoding="utf-8",
        )
        result = render_dossier(md)
        assert result["frontmatter"]["wine_id"] == 1
        assert len(result["sections"]) == 2
        assert result["sections"][0]["heading"] == "Identity"
        assert result["sections"][0]["populated"] is True
        assert "<p>" in result["sections"][0]["html"]

    def test_empty_section(self, tmp_path):
        md = tmp_path / "test.md"
        md.write_text("## Identity\nContent\n\n## Empty\n\n", encoding="utf-8")
        result = render_dossier(md)
        assert result["sections"][0]["populated"] is True
        assert result["sections"][1]["populated"] is False

    def test_no_frontmatter(self, tmp_path):
        md = tmp_path / "test.md"
        md.write_text("## Section\nContent\n", encoding="utf-8")
        result = render_dossier(md)
        assert result["frontmatter"] == {}
        assert len(result["sections"]) == 1

    def test_fence_comments_stripped(self, tmp_path):
        md = tmp_path / "test.md"
        md.write_text(
            "## Section\n<!-- source: agent:research -->\nContent\n",
            encoding="utf-8",
        )
        result = render_dossier(md)
        assert "source:" not in result["sections"][0]["html"]
        assert "Content" in result["sections"][0]["html"]

    def test_slug_generation(self, tmp_path):
        md = tmp_path / "test.md"
        md.write_text("## Producer Profile\nText\n", encoding="utf-8")
        result = render_dossier(md)
        assert result["sections"][0]["slug"] == "producer-profile"

    def test_raw_preserved(self, tmp_path):
        md = tmp_path / "test.md"
        original = "---\nwine_id: 1\n---\n\n## Section\nContent\n"
        md.write_text(original, encoding="utf-8")
        result = render_dossier(md)
        assert result["raw"] == original

    def test_tables_rendered(self, tmp_path):
        md = tmp_path / "test.md"
        md.write_text(
            "## Data\n\n| A | B |\n|---|---|\n| 1 | 2 |\n",
            encoding="utf-8",
        )
        result = render_dossier(md)
        assert "<table>" in result["sections"][0]["html"]

    def test_multiple_sections(self, tmp_path):
        md = tmp_path / "test.md"
        md.write_text(
            "## Identity\nFirst\n\n## Vintage\nSecond\n\n## Ratings\nThird\n\n## Notes\nFourth\n",
            encoding="utf-8",
        )
        result = render_dossier(md)
        assert len(result["sections"]) == 4
        headings = [s["heading"] for s in result["sections"]]
        assert headings == ["Identity", "Vintage", "Ratings", "Notes"]

    def test_unicode_content(self, tmp_path):
        md = tmp_path / "test.md"
        md.write_text(
            "## 生産者\n日本のワイン\n\n## Château\nVin français élégant\n",
            encoding="utf-8",
        )
        result = render_dossier(md)
        assert len(result["sections"]) == 2
        assert "日本のワイン" in result["sections"][0]["html"]
        assert "élégant" in result["sections"][1]["html"]

    def test_empty_file(self, tmp_path):
        md = tmp_path / "test.md"
        md.write_text("", encoding="utf-8")
        result = render_dossier(md)
        assert result["sections"] == []
        assert result["frontmatter"] == {}
