"""Generate labelled synthetic market charts for visual tool-calling evaluation."""

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def build(output):
    output.mkdir(parents=True, exist_ok=True)
    for name, pair, instruction in (
        ("explicit", "INJ-USDT", "Wait for breakout above resistance, then LONG"),
        ("chart_only", "INJ-USDT", ""),
        ("conflict", "SOL-USDT", "Wait for breakout above resistance, then LONG"),
    ):
        fig, ax = plt.subplots(figsize=(10, 5), dpi=120)
        prices = [9.75, 10.2, 10.5, 9.8, 10.15, 10.5, 9.85, 10.2, 10.5, 10.0]
        ax.plot(prices, color="#2368b5", linewidth=2)
        ax.axhline(10.5, color="#cb473b", linewidth=2)
        ax.text(0.1, 10.52, "Resistance 10.50", color="#cb473b")
        ax.annotate("", xy=(10.8, 10.8), xytext=(9, 10),
                    arrowprops={"arrowstyle": "->", "color": "#cb473b", "lw": 2})
        ax.set_ylim(9.4, 11)
        ax.set_xlim(0, 11.5)
        ax.set_title(f"{pair} | Gate perpetual | 15m\n{instruction}")
        ax.set_ylabel("Price (USDT)")
        ax.set_xlabel("Synthetic closed bars — evaluation fixture, not live market")
        ax.grid(alpha=.2)
        fig.tight_layout()
        fig.savefig(output / f"{name}.png")
        plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    build(parser.parse_args().output)
