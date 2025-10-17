"""
Data preparation and formatting utilities for the PDF report.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import pandas as pd


@dataclass
class GenderTable:
    headers: Tuple[str, ...]
    rows: List[Tuple[str, ...]]


def format_date(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%d/%m/%Y")

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
            return parsed.strftime("%d/%m/%Y")
        except ValueError:
            return value

    return "Data não informada"


def format_distance(distance: Any) -> str:
    if isinstance(distance, (int, float)):
        return f"{float(distance):.1f} km"
    if distance:
        return f"{distance} km"
    return "Distância não informada"


def format_integer(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"{int(value):,}".replace(",", ".")
    try:
        numeric = int(str(value))
        return f"{numeric:,}".replace(",", ".")
    except (TypeError, ValueError):
        return "0"


def prepare_gender_table(df: pd.DataFrame) -> GenderTable:
    working = df.copy()
    if "gender" not in working.columns or "shoe_brand" not in working.columns:
        return GenderTable(headers=(), rows=[])

    working["gender"] = working["gender"].astype(str).str.upper()
    pivot = (
        working.pivot_table(
            index="shoe_brand", columns="gender", values="count", aggfunc="sum", fill_value=0
        )
        .assign(Total=lambda p: p.sum(axis=1))
        .sort_values("Total", ascending=False)
        .head(8)
    )

    if pivot.empty:
        return GenderTable(headers=(), rows=[])

    ordered_columns = [col for col in ["MASCULINO", "FEMININO", "OUTRO"] if col in pivot.columns]
    dynamic_columns = [col for col in pivot.columns if col not in ordered_columns and col != "Total"]
    columns = ordered_columns + dynamic_columns

    headers = ["Marca"] + [col.title() for col in columns] + ["Total"]
    rows: List[Tuple[str, ...]] = []

    for brand, row in pivot.iterrows():
        total = row["Total"] or 0
        formatted = [str(brand)]
        for col in columns:
            share = row[col] / total if total else 0
            formatted.append(f"{share * 100:.0f}%")
        formatted.append(str(int(total)))
        rows.append(tuple(formatted))

    return GenderTable(headers=tuple(headers), rows=rows)


def prepare_category_rows(df: pd.DataFrame) -> List[Tuple[str, str, str, str]]:
    if "run_category" not in df.columns or "count" not in df.columns:
        return []

    grouped = df.groupby("run_category", as_index=False)["count"].sum()
    if grouped.empty:
        return []

    rows: List[Tuple[str, str, str, str]] = []
    for _, row in grouped.sort_values("count", ascending=False).iterrows():
        category = str(row["run_category"])
        total = int(row["count"])
        filtered = df[df["run_category"] == row["run_category"]]
        top_brand_row = filtered.sort_values("count", ascending=False).iloc[0]
        top_brand = str(top_brand_row["shoe_brand"])
        dominance = f"{(top_brand_row['count'] / total) * 100:.0f}%" if total else "-"
        rows.append((category, str(total), top_brand, dominance))

    return rows[:8]


def prepare_top_finishers_rows(
    runners: Iterable[Dict[str, Any]]
) -> List[Tuple[str, str, str, str, str]]:
    sortable: List[Tuple[int, Dict[str, Any]]] = []
    for runner in runners:
        position = runner.get("position")
        if position is None:
            continue
        try:
            sortable.append((int(position), runner))
        except (TypeError, ValueError):
            continue

    sortable.sort(key=lambda item: item[0])
    clipped = sortable[:5]

    rows: List[Tuple[str, str, str, str, str]] = []
    for position, runner in clipped:
        bib = runner.get("bib") or "-"
        brand = runner.get("shoe_brand") or "-"
        category = runner.get("run_category") or "-"
        gender = runner.get("gender") or "-"
        rows.append((str(position), str(bib), str(brand), str(category), str(gender)))
    return rows
