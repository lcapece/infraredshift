"""Load packaged Infraredshift logo pixmaps for UI chrome."""
from __future__ import annotations

from importlib import resources

from PySide6.QtCore import Qt
from PySide6.QtGui import QPixmap

from ..brand import LOGO_LOGIN, LOGO_MARK, LOGO_STARTUP_BANNER, LOGO_WORDMARK


def _load_asset(name: str) -> QPixmap:
    try:
        data = resources.files("analyzer.assets").joinpath(name).read_bytes()
    except Exception:
        return QPixmap()
    pix = QPixmap()
    if not pix.loadFromData(data):
        return QPixmap()
    return pix


def _render_svg(name: str, width: int) -> QPixmap:
    """Rasterise an SVG at the requested width.

    QPixmap.loadFromData renders an SVG at its intrinsic size and then any
    scaling is a bitmap resample - which throws away the whole point of
    shipping a vector. Rendering at the target width keeps the edges crisp at
    every panel size and on high-DPI displays.
    """
    try:
        from PySide6.QtGui import QPainter
        from PySide6.QtSvg import QSvgRenderer
    except ImportError:
        return QPixmap()
    try:
        data = resources.files("analyzer.assets").joinpath(name).read_bytes()
    except Exception:
        return QPixmap()
    renderer = QSvgRenderer(data)
    if not renderer.isValid():
        return QPixmap()
    size = renderer.defaultSize()
    if size.width() <= 0 or size.height() <= 0:
        return QPixmap()
    height = max(1, round(width * size.height() / size.width()))
    pix = QPixmap(width, height)
    pix.fill(Qt.transparent)
    painter = QPainter(pix)
    try:
        renderer.render(painter)
    finally:
        painter.end()
    return pix


def wordmark_pixmap(height: int = 40) -> QPixmap:
    pix = _load_asset(LOGO_WORDMARK)
    if pix.isNull() or height <= 0:
        return pix
    return pix.scaledToHeight(height, Qt.SmoothTransformation)


def mark_pixmap(height: int = 28) -> QPixmap:
    pix = _load_asset(LOGO_MARK)
    if pix.isNull() or height <= 0:
        return pix
    return pix.scaledToHeight(height, Qt.SmoothTransformation)


def startup_banner_pixmap(height: int = 56) -> QPixmap:
    pix = _load_asset(LOGO_STARTUP_BANNER)
    if pix.isNull() or height <= 0:
        return pix
    return pix.scaledToHeight(height, Qt.SmoothTransformation)


def login_logo_pixmap(width: int = 0) -> QPixmap:
    """Return the packaged login wordmark, rendered at ``width`` if given."""
    if str(LOGO_LOGIN).lower().endswith(".svg"):
        if width > 0:
            rendered = _render_svg(LOGO_LOGIN, width)
            if not rendered.isNull():
                return rendered
        # Width unknown (the caller measures the panel first) or QtSvg is
        # unavailable: fall back to the intrinsic-size raster so the login
        # screen still shows a mark rather than bare text.
        pix = _load_asset(LOGO_LOGIN)
        if not pix.isNull() and width > 0:
            return pix.scaledToWidth(width, Qt.SmoothTransformation)
        return pix
    pix = _load_asset(LOGO_LOGIN)
    if pix.isNull() or width <= 0:
        return pix
    return pix.scaledToWidth(width, Qt.SmoothTransformation)
