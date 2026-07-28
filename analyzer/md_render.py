"""Shared themed-Markdown renderer for read-only prose panels.

One implementation so every findings/evidence/recommendation panel renders the
same way the triage detail cards and the fix-script view already do. Colors read
the live PALETTE at render time (so a theme toggle recolors), and ALL text is
HTML-escaped first, so panels never corrupt on content containing <, >, or &.

Do NOT use this for editable inputs, raw SQL the user copies, or data grids -
only read-only prose.
"""
from __future__ import annotations

import html as _html
import re

from .theme import PALETTE


def md_inline(text: str) -> str:
    """Escape HTML, then apply a tiny inline Markdown subset for rich-text
    labels: **bold** and `code`. Deliberately small so it can't emit broken
    markup. Escaping happens FIRST, so <, >, & in prose (e.g. `a < b`) survive."""
    escaped = _html.escape(str(text))
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped)
    escaped = re.sub(
        r"`([^`]+)`",
        lambda m: f'<code style="color:{PALETTE.cyan};">{m.group(1)}</code>',
        escaped,
    )
    return escaped


def render_markdown_card(text: str) -> str:
    """Convert a small Markdown subset to themed HTML for a rich-text QLabel.

    Supports blank-line paragraphs, '- ' bullets, **bold**, and `code`. Every
    line routes through md_inline, so the output is always well-formed and
    escaped. Colors route through PALETTE (never hardcoded hex)."""
    raw = str(text or "").strip()
    if not raw:
        return "-"
    html_parts: list[str] = []
    bullet_open = False

    def _close_bullets() -> None:
        nonlocal bullet_open
        if bullet_open:
            html_parts.append("</ul>")
            bullet_open = False

    for line in raw.split("\n"):
        stripped = line.strip()
        if not stripped:
            _close_bullets()
            continue
        if stripped.startswith("- "):
            if not bullet_open:
                html_parts.append("<ul style='margin:2px 0 2px 14px;'>")
                bullet_open = True
            html_parts.append(f"<li>{md_inline(stripped[2:])}</li>")
        else:
            _close_bullets()
            html_parts.append(f"<div style='margin:2px 0;'>{md_inline(stripped)}</div>")
    _close_bullets()
    return "".join(html_parts)


def apply_markdown(label, text: str) -> None:
    """Render `text` as themed Markdown into a QLabel (sets RichText format and
    keeps the text mouse-selectable). Import Qt lazily so this module stays
    import-cheap and testable without a display."""
    from PySide6.QtCore import Qt

    label.setTextFormat(Qt.RichText)
    label.setTextInteractionFlags(Qt.TextSelectableByMouse)
    label.setWordWrap(True)
    label.setText(render_markdown_card(text))


def code_block_html(code: str, *, accent: str | None = None, label: str = "") -> str:
    """Themed, boxed, monospace HTML for a CODE snippet (SQL/DDL the user reads
    as code, not prose). Escaped; never prosified. Use for recommendation lines
    that are actually statements."""
    border = accent or PALETTE.warn
    safe = _html.escape(str(code or "").strip())
    head = (
        f"<div style='color:{PALETTE.text_2}; font-weight:700; font-size:11px; margin-bottom:2px;'>{_html.escape(label)}</div>"
        if label
        else ""
    )
    return (
        f"<div style='margin:4px 0; padding:6px 10px; background:{PALETTE.bg_3}; "
        f"border:1px solid {border}; border-radius:4px; font-family:Consolas,monospace; "
        f"font-size:12px; color:{PALETTE.text_0};'>{head}{safe}</div>"
    )
