"""Shared themed-Markdown renderer: escaping is the load-bearing invariant."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analyzer.md_render import code_block_html, md_inline, render_markdown_card


def test_escapes_angle_brackets_and_ampersand():
    # A SQL predicate must not be truncated at '<' or corrupted by '&'.
    out = render_markdown_card("WHERE a < b AND c > d & e")
    assert "&lt;" in out and "&gt;" in out and "&amp;" in out
    assert "a < b" not in out  # raw '<' never leaks


def test_bold_and_inline_code_still_render():
    out = render_markdown_card("Use **VACUUM** on `fact_sales`.")
    assert "<b>VACUUM</b>" in out
    assert "<code" in out and "fact_sales" in out


def test_bullets_render_as_list():
    out = render_markdown_card("- first\n- second")
    assert out.count("<li>") == 2 and "<ul" in out


def test_empty_returns_dash():
    assert render_markdown_card("") == "-"
    assert render_markdown_card(None) == "-"


def test_md_inline_escapes_before_formatting():
    # '<script>' must be inert; bold still applies around it.
    out = md_inline("**<script>alert(1)</script>**")
    assert "<script>" not in out
    assert "&lt;script&gt;" in out
    assert out.startswith("<b>") and out.endswith("</b>")


def test_code_block_escapes_and_boxes():
    out = code_block_html("ALTER TABLE t ALTER SORTKEY (a < b);", label="EXECUTE")
    assert "&lt;" in out
    assert "monospace" in out and "EXECUTE" in out
    assert "border" in out


def test_hostile_content_does_not_truncate():
    # The exact failure the escaping guards against: content after '<' survives.
    payload = "cost ratio x < 0.5 makes the join spill; see rows > 1e9 & skew"
    out = render_markdown_card(payload)
    assert "spill" in out and "skew" in out  # nothing after '<' or '&' dropped
