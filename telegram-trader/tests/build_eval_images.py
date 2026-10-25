"""Build explicitly synthetic visual test cards; no channel images are edited."""

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

root = Path(__file__).parent / "assets"
root.mkdir(exist_ok=True)
font = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial.ttf", 48)
for name, lines in {
    "explicit-short": ["INJ / USDT", "OPEN SHORT NOW", "MARKET ENTRY"],
    "commentary": ["MARKET COMMENTARY", "NO TRADE INSTRUCTION", "OBSERVATION ONLY"],
}.items():
    canvas = Image.new("RGB", (1000, 400), "white")
    draw = ImageDraw.Draw(canvas)
    for i, line in enumerate(lines):
        draw.text((40, 50 + i * 100), line, font=font, fill="black")
    canvas.save(root / (name + ".png"))
