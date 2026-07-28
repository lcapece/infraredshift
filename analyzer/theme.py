"""Design tokens and runtime theme helpers."""
from __future__ import annotations

import re
from dataclasses import dataclass, fields


@dataclass
class Palette:
    bg_0: str = "#0A0E1A"
    bg_1: str = "#111624"
    bg_2: str = "#1A2133"
    bg_3: str = "#232C42"
    border: str = "#2A3448"
    border_strong: str = "#3A4560"

    text_0: str = "#F5F7FF"
    text_1: str = "#C4CCE0"
    text_2: str = "#8691AD"
    text_3: str = "#5A657E"

    accent: str = "#6B8AFF"
    accent_bright: str = "#8BA4FF"
    accent_dim: str = "#3D5BCC"

    cyan: str = "#5FE3F0"
    violet: str = "#B57BFF"
    pink: str = "#FF7AB8"

    ok: str = "#3DD68C"
    warn: str = "#FFB547"
    crit: str = "#FF5D6E"

    scan: str = "#5FE3F0"
    join: str = "#B57BFF"
    aggregate: str = "#FFB547"
    redistribute: str = "#FF7AB8"
    sort: str = "#8BA4FF"
    other: str = "#8691AD"


DARK_PALETTE = Palette()
LIGHT_PALETTE = Palette(
    bg_0="#F6F8FC",
    bg_1="#FFFFFF",
    bg_2="#EDF2F8",
    bg_3="#DDE6F2",
    border="#CBD5E1",
    border_strong="#94A3B8",
    text_0="#182033",
    text_1="#263447",
    text_2="#526176",
    text_3="#718095",
    accent="#2558D8",
    accent_bright="#1746B4",
    accent_dim="#DDE7FF",
    cyan="#007384",
    violet="#6D35B8",
    pink="#A61F62",
    ok="#0F7A3D",
    warn="#8A5200",
    crit="#B42334",
    scan="#007384",
    join="#6D35B8",
    aggregate="#8A5200",
    redistribute="#A61F62",
    sort="#1746B4",
    other="#526176",
)


@dataclass(frozen=True)
class Type:
    family: str = "Inter, 'SF Pro Text', 'Segoe UI Variable', 'Segoe UI', system-ui"
    mono: str = "'JetBrains Mono', 'Cascadia Code', 'Consolas', monospace"
    display: int = 22
    h1: int = 16
    h2: int = 13
    # Body/small floors favor low-vision readability on dense DBA tables.
    body: int = 13
    small: int = 12
    tiny: int = 11


@dataclass(frozen=True)
class Radii:
    sm: int = 6
    md: int = 10
    lg: int = 14
    xl: int = 20


@dataclass(frozen=True)
class Space:
    xs: int = 4
    sm: int = 8
    md: int = 12
    lg: int = 16
    xl: int = 24
    xxl: int = 32


PALETTE = Palette(**LIGHT_PALETTE.__dict__)
TYPE = Type()
RADII = Radii()
SPACE = Space()
_THEME_MODE = "light"


_STEP_COLOR_ROLES = (
    ("scan", "scan"),
    ("seq scan", "scan"),
    ("hash join", "join"),
    ("merge join", "join"),
    ("nested loop", "join"),
    ("join", "join"),
    ("aggregate", "aggregate"),
    ("hash aggregate", "aggregate"),
    ("group aggregate", "aggregate"),
    ("dist", "redistribute"),
    ("bcast", "redistribute"),
    ("redistribute", "redistribute"),
    ("broadcast", "redistribute"),
    ("sort", "sort"),
    ("merge", "sort"),
    ("limit", "other"),
    ("project", "other"),
    ("filter", "other"),
    ("hash", "other"),
)

_STYLE_COLOR_ROLES = {
    "#0A0E1A": "bg_0",
    "#111624": "bg_1",
    "#0F1420": "bg_1",
    "#141A2A": "bg_2",
    "#172033": "bg_2",
    "#1A2133": "bg_2",
    "#111827": "bg_2",
    "#232C42": "bg_3",
    "#2A3448": "border",
    "#3A4560": "border_strong",
    "#F5F7FF": "text_0",
    "#E2E8F7": "text_0",
    "#C4CCE0": "text_1",
    "#AAB4CC": "text_2",
    "#8691AD": "text_2",
    "#5A657E": "text_3",
    "#6B8AFF": "accent",
    "#8BA4FF": "accent_bright",
    "#A5BAFF": "accent_bright",
    "#3D5BCC": "accent_dim",
    "#5FE3F0": "cyan",
    "#B57BFF": "violet",
    "#FF7AB8": "pink",
    "#3DD68C": "ok",
    "#FFB547": "warn",
    "#FF5D6E": "crit",
    "#FF5E6C": "crit",
}

for _palette in (DARK_PALETTE, LIGHT_PALETTE):
    for _field in fields(Palette):
        _STYLE_COLOR_ROLES.setdefault(getattr(_palette, _field.name).upper(), _field.name)

_HEX_RE = re.compile(r"#[0-9a-fA-F]{6}\b")


def normalize_theme_mode(mode: object) -> str:
    text = str(mode or "").strip().lower()
    return "dark" if text == "dark" else "light"


def set_theme_mode(mode: object) -> str:
    global _THEME_MODE
    _THEME_MODE = normalize_theme_mode(mode)
    source = DARK_PALETTE if _THEME_MODE == "dark" else LIGHT_PALETTE
    for field in fields(Palette):
        setattr(PALETTE, field.name, getattr(source, field.name))
    return _THEME_MODE


def get_theme_mode() -> str:
    return _THEME_MODE


def is_light_theme() -> bool:
    return _THEME_MODE == "light"


def retint_stylesheet(stylesheet: str, mode: object | None = None) -> str:
    """Replace known dark/light token literals with the active theme colors."""
    target_mode = normalize_theme_mode(_THEME_MODE if mode is None else mode)
    palette = DARK_PALETTE if target_mode == "dark" else LIGHT_PALETTE

    def replace(match: re.Match[str]) -> str:
        token = match.group(0).upper()
        role = _STYLE_COLOR_ROLES.get(token)
        if not role:
            return match.group(0)
        return getattr(palette, role)

    return _HEX_RE.sub(replace, str(stylesheet or ""))


def color_for_step(step_name: str) -> str:
    if not step_name:
        return PALETTE.other
    key = step_name.strip().lower()
    for prefix, role in _STEP_COLOR_ROLES:
        if prefix in key:
            return getattr(PALETTE, role)
    return PALETTE.other
