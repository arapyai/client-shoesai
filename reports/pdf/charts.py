"""
Static chart generation helpers used by the PDF renderer.
"""

from __future__ import annotations

import base64
import io
from dataclasses import dataclass
from typing import Dict, Optional

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .styles import ACCENT_ORANGE, GENDER_COLORS, PRIMARY_BLUE, TEXT_DARK, TEXT_MUTED, WHITE

matplotlib.use("Agg")


@dataclass
class ReportCharts:
    brand_distribution: Optional[str] = None
    gender_distribution: Optional[str] = None
    category_distribution: Optional[str] = None


def build_report_charts(
    *,
    metrics: Dict[str, object],
    gender_df: Optional[pd.DataFrame],
    category_df: Optional[pd.DataFrame],
) -> ReportCharts:
    return ReportCharts(
        brand_distribution=_brand_distribution_chart(metrics),
        gender_distribution=_gender_distribution_chart(gender_df),
        category_distribution=_category_distribution_chart(category_df),
    )


def _brand_distribution_chart(metrics: Dict[str, object]) -> Optional[str]:
    brand_dist = metrics.get("brand_distribution") or {}
    if not brand_dist:
        return None

    sorted_items = sorted(brand_dist.items(), key=lambda x: x[1], reverse=True)
    brands = [str(name) for name, _ in sorted_items]
    counts = [int(value) if value is not None else 0 for _, value in sorted_items]
    total = sum(counts)
    percentages = [(count / total) * 100 if total else 0 for count in counts]

    leader_brand = None
    leader_info = metrics.get("leader_brand") or {}
    if isinstance(leader_info, dict):
        leader_brand = leader_info.get("name")

    height = max(2.5, 0.45 * len(brands) + 1.2)
    fig, ax = plt.subplots(figsize=(6.4, height))
    colors = [ACCENT_ORANGE if brand == leader_brand else PRIMARY_BLUE for brand in brands]

    bars = ax.barh(brands, percentages, color=colors)
    ax.invert_yaxis()
    ax.set_xlabel("Participação (%)", color=TEXT_DARK, fontsize=10)
    ax.set_xlim(0, max(55, max(percentages) * 1.15) if percentages else 50)
    ax.set_facecolor(WHITE)
    fig.patch.set_facecolor(WHITE)

    ax.tick_params(axis="both", colors=TEXT_MUTED, labelsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(TEXT_MUTED)
    ax.spines["bottom"].set_color(TEXT_MUTED)

    for bar, count, pct in zip(bars, counts, percentages):
        width = bar.get_width()
        label = f"{pct:.1f}% ({count})"
        ax.text(
            width + 1,
            bar.get_y() + bar.get_height() / 2,
            label,
            va="center",
            ha="left",
            fontsize=8.5,
            color=TEXT_DARK,
        )

    ax.set_title("Participação por Marca", loc="left", color=TEXT_DARK, fontsize=12, pad=6)
    fig.tight_layout()
    return _fig_to_data_uri(fig)


def _gender_distribution_chart(df: Optional[pd.DataFrame]) -> Optional[str]:
    if df is None or df.empty:
        return None

    working = df.copy()
    if "gender" not in working.columns or "shoe_brand" not in working.columns:
        return None

    working["gender"] = working["gender"].astype(str).str.upper()
    pivot_counts = working.pivot_table(
        index="shoe_brand", columns="gender", values="count", aggfunc="sum", fill_value=0
    )
    totals = pivot_counts.sum(axis=1)
    pivot_counts = pivot_counts[totals > 0]
    if pivot_counts.empty:
        return None

    column_order = [col for col in ["MASCULINO", "FEMININO", "OUTRO"] if col in pivot_counts.columns]
    other_columns = [col for col in pivot_counts.columns if col not in column_order]
    ordered_columns = column_order + other_columns

    ordered_brands = totals.sort_values(ascending=False).head(8).index
    pivot_counts = pivot_counts.loc[ordered_brands]
    percentages = pivot_counts.div(pivot_counts.sum(axis=1), axis=0).fillna(0.0) * 100

    brands = list(ordered_brands)
    height = max(2.5, 0.45 * len(brands) + 1.2)
    fig, ax = plt.subplots(figsize=(6.4, height))
    left = np.zeros(len(brands))

    for gender in ordered_columns:
        values = percentages.get(gender, pd.Series(data=0, index=brands))
        color = GENDER_COLORS.get(gender, PRIMARY_BLUE)
        bars = ax.barh(brands, values, left=left, label=gender.title(), color=color)
        left += values.values

        for bar, value in zip(bars, values):
            if value < 8:
                continue
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_y() + bar.get_height() / 2,
                f"{value:.0f}%",
                va="center",
                ha="center",
                fontsize=8,
                color=WHITE,
            )

    ax.invert_yaxis()
    ax.set_xlim(0, 100)
    ax.set_xlabel("Distribuição por gênero (%)", color=TEXT_DARK, fontsize=10)
    ax.set_facecolor(WHITE)
    fig.patch.set_facecolor(WHITE)
    ax.tick_params(axis="both", colors=TEXT_MUTED, labelsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(TEXT_MUTED)
    ax.spines["bottom"].set_color(TEXT_MUTED)
    ax.legend(loc="lower right", fontsize=8)
    ax.set_title("Participação por gênero e marca", loc="left", color=TEXT_DARK, fontsize=12, pad=6)
    fig.tight_layout()
    return _fig_to_data_uri(fig)


def _category_distribution_chart(df: Optional[pd.DataFrame]) -> Optional[str]:
    if df is None or df.empty:
        return None

    if "run_category" not in df.columns or "shoe_brand" not in df.columns or "count" not in df.columns:
        return None

    working = df.copy()
    working["run_category"] = working["run_category"].astype(str)
    grouped_totals = working.groupby("run_category")["count"].sum().sort_values(ascending=False)

    if grouped_totals.empty:
        return None

    top_categories = list(grouped_totals.head(4).index)
    filtered = working[working["run_category"].isin(top_categories)]

    subplot_count = len(top_categories)
    fig_height = max(3.0, 2.1 * subplot_count)
    fig, axes = plt.subplots(subplot_count, 1, figsize=(6.4, fig_height), sharex=True)
    if subplot_count == 1:
        axes = [axes]  # type: ignore[assignment]

    for ax, category in zip(axes, top_categories):
        category_df = filtered[filtered["run_category"] == category].copy()
        category_df = category_df.sort_values("count", ascending=False).head(6)
        total = category_df["count"].sum()
        if total <= 0:
            continue

        category_df["percentage"] = category_df["count"] / total * 100
        brands = list(category_df["shoe_brand"])
        percentages = list(category_df["percentage"])

        colors = [ACCENT_ORANGE if idx == 0 else PRIMARY_BLUE for idx in range(len(brands))]
        bars = ax.barh(brands, percentages, color=colors)
        ax.invert_yaxis()
        ax.set_facecolor(WHITE)
        ax.tick_params(axis="both", colors=TEXT_MUTED, labelsize=8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color(TEXT_MUTED)
        ax.spines["bottom"].set_color(TEXT_MUTED)
        ax.set_title(category, loc="left", color=TEXT_DARK, fontsize=11, pad=4)

        for bar, pct in zip(bars, percentages):
            ax.text(
                bar.get_width() + 1,
                bar.get_y() + bar.get_height() / 2,
                f"{pct:.1f}%",
                va="center",
                ha="left",
                fontsize=8,
                color=TEXT_DARK,
            )

    axes[-1].set_xlabel("Participação por marca (%)", color=TEXT_DARK, fontsize=10)
    plt.tight_layout()
    return _fig_to_data_uri(fig)


def _fig_to_data_uri(fig: matplotlib.figure.Figure) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    buffer.seek(0)
    encoded = base64.b64encode(buffer.read()).decode("ascii")
    return f"data:image/png;base64,{encoded}"
