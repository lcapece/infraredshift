"""Render Infraredshift / Infraredshift logo PNGs into analyzer/assets.

Exact wordmark text is drawn with code (not an image model) so spelling is correct.
"""
from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "analyzer" / "assets"

# Brand greens (Citizens-adjacent, product green)
GREEN = (0, 133, 85, 255)       # #008555
GREEN_BRIGHT = (0, 168, 107, 255)
GREEN_DARK = (22, 61, 48, 255)  # #163D30
GREEN_SOFT = (216, 238, 230, 255)
WHITE = (255, 255, 255, 255)
TRANSPARENT = (0, 0, 0, 0)


def _font(size: int, bold: bool = True) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        r"C:\Windows\Fonts\segoeuib.ttf" if bold else r"C:\Windows\Fonts\segoeui.ttf",
        r"C:\Windows\Fonts\arialbd.ttf" if bold else r"C:\Windows\Fonts\arial.ttf",
        r"C:\Windows\Fonts\calibrib.ttf" if bold else r"C:\Windows\Fonts\calibri.ttf",
    ]
    for path in candidates:
        if Path(path).is_file():
            return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()


def _draw_mark(draw: ImageDraw.ImageDraw, cx: int, cy: int, r: int) -> None:
    """Hex-ish node with three inbound flow dots — product mark."""
    # Soft outer ring
    draw.ellipse((cx - r - 4, cy - r - 4, cx + r + 4, cy + r + 4), fill=GREEN_SOFT)
    # Core circle
    draw.ellipse((cx - r, cy - r, cx + r, cy + r), fill=GREEN)
    # Inner highlight
    draw.ellipse((cx - r // 2, cy - r // 2, cx + r // 3, cy + r // 3), fill=GREEN_BRIGHT)
    # Three telemetry dots (producer/consumer flow)
    for i, angle in enumerate((-2.4, -1.8, -1.2)):
        import math

        dx = int(math.cos(angle) * (r + 10))
        dy = int(math.sin(angle) * (r + 10))
        draw.ellipse((cx + dx - 3, cy + dy - 3, cx + dx + 3, cy + dy + 3), fill=GREEN)
        draw.line((cx + dx, cy + dy, cx - 2, cy), fill=(*GREEN[:3], 180), width=2)
    # Center white “6”
    font = _font(max(14, r), bold=True)
    text = "6"
    bbox = draw.textbbox((0, 0), text, font=font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    draw.text((cx - tw / 2, cy - th / 2 - 2), text, font=font, fill=WHITE)


def render_wordmark(path: Path, *, width: int = 720, height: int = 160) -> None:
    img = Image.new("RGBA", (width, height), TRANSPARENT)
    draw = ImageDraw.Draw(img)
    mark_r = 36
    mark_cx, mark_cy = 48, height // 2
    _draw_mark(draw, mark_cx, mark_cy, mark_r)

    # Wordmark: Infraredshift — exact characters
    word = "Infraredshift"
    font = _font(54, bold=True)
    x0 = mark_cx + mark_r + 28
    y0 = height // 2 - 36
    # Draw with slight tracking by character for polish
    x = x0
    for ch in word:
        color = GREEN if ch == "6" else GREEN_DARK
        draw.text((x, y0), ch, font=font, fill=color)
        bbox = draw.textbbox((x, y0), ch, font=font)
        x = bbox[2] + 1

    tag = "PHYSICAL DESIGN INTELLIGENCE"
    tag_font = _font(14, bold=True)
    draw.text((x0, y0 + 58), tag, font=tag_font, fill=(*GREEN[:3], 200))

    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path, "PNG")
    print(f"Wrote {path} ({path.stat().st_size:,} bytes)")


def render_mark_only(path: Path, *, size: int = 128) -> None:
    img = Image.new("RGBA", (size, size), TRANSPARENT)
    draw = ImageDraw.Draw(img)
    _draw_mark(draw, size // 2, size // 2, size // 3)
    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path, "PNG")
    print(f"Wrote {path} ({path.stat().st_size:,} bytes)")


def render_startup_banner(path: Path, *, width: int = 960, height: int = 120) -> None:
    """Wide transparent banner used at the top of the startup dialog."""
    img = Image.new("RGBA", (width, height), TRANSPARENT)
    draw = ImageDraw.Draw(img)
    mark_r = 32
    mark_cx, mark_cy = 44, height // 2
    _draw_mark(draw, mark_cx, mark_cy, mark_r)

    word = "Infraredshift"
    font = _font(42, bold=True)
    x0 = mark_cx + mark_r + 22
    y0 = height // 2 - 28
    x = x0
    for ch in word:
        color = GREEN if ch == "6" else GREEN_DARK
        draw.text((x, y0), ch, font=font, fill=color)
        bbox = draw.textbbox((x, y0), ch, font=font)
        x = bbox[2] + 1

    tag = "PHYSICAL DESIGN INTELLIGENCE  ·  OFFLINE  ·  AIR-GAPPED"
    tag_font = _font(12, bold=True)
    draw.text((x0, y0 + 46), tag, font=tag_font, fill=(*GREEN[:3], 190))

    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path, "PNG")
    print(f"Wrote {path} ({path.stat().st_size:,} bytes)")


def main() -> int:
    render_wordmark(ASSETS / "infraredshift_logo.png")
    render_mark_only(ASSETS / "infraredshift_mark.png", size=128)
    render_startup_banner(ASSETS / "infraredshift_startup_banner.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
