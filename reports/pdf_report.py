"""
PDF report builder for marathon analytics.

Exposes a single function `generate_marathon_pdf_report` that receives
pre-computed metrics from the database layer and returns PDF bytes ready
to be streamed to the user.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from fpdf import FPDF

# Palette inspired by the existing dashboard look & feel
ACCENT_COLOR = (41, 128, 185)
PRIMARY_DARK = (33, 45, 64)
TEXT_COLOR = (45, 52, 54)
SOFT_BACKGROUND = (245, 247, 250)


class MarathonReportPDF(FPDF):
    """Small extension that applies the project visual identity."""

    def __init__(self, title: str, subtitle: str):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.title_text = title
        self.subtitle_text = subtitle
        self.set_left_margin(15)
        self.set_right_margin(15)
        self.set_top_margin(20)
        self.set_auto_page_break(auto=True, margin=20)
        self.alias_nb_pages()

    # The FPDF hooks below run automatically for every page
    def header(self) -> None:  # noqa: D401
        self.set_fill_color(*PRIMARY_DARK)
        self.rect(0, 0, self.w, 32, "F")
        self.set_text_color(255, 255, 255)

        self.set_xy(15, 12)
        self.set_font("Helvetica", "B", 18)
        self.cell(0, 0, self.title_text, ln=1)

        if self.subtitle_text:
            self.set_xy(15, 20)
            self.set_font("Helvetica", "", 11)
            self.cell(0, 0, self.subtitle_text, ln=1)

        self.set_y(36)
        self.set_text_color(*TEXT_COLOR)

    def footer(self) -> None:  # noqa: D401
        self.set_y(-15)
        self.set_font("Helvetica", "I", 9)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Página {self.page_no()}/{{nb}}", align="R")


# --- Public API ----------------------------------------------------------------


def generate_marathon_pdf_report(
    marathon_metrics: Dict[str, Any],
    *,
    gender_distribution: Optional[pd.DataFrame] = None,
    category_distribution: Optional[pd.DataFrame] = None,
    runners: Optional[Iterable[Dict[str, Any]]] = None,
    generated_by: Optional[str] = None,
    generated_at: Optional[datetime] = None,
) -> bytes:
    """
    Build a PDF report summarising insights for a single marathon.

    Args:
        marathon_metrics: Dictionary returned by `get_individual_marathon_metrics`.
        gender_distribution: DataFrame returned by `get_gender_brand_distribution`.
        category_distribution: DataFrame returned by `get_category_brand_distribution`.
        runners: Iterable of runner dictionaries (as returned by `get_marathon_runners`).
        generated_by: Optional string to identify who triggered the export.
        generated_at: Optional datetime for the footer timestamp.

    Returns:
        PDF file encoded as bytes ready for download.
    """
    if not marathon_metrics:
        raise ValueError("Marathon metrics are required to build the PDF report.")

    generated_at = generated_at or datetime.now()
    marathon_name = marathon_metrics.get("marathon_name", "Prova")
    subtitle = "Relatório analítico de participação e marcas"

    pdf = MarathonReportPDF(title=f"Relatório • {marathon_name}", subtitle=subtitle)
    pdf.add_page()

    _add_overview(pdf, marathon_metrics, generated_at, generated_by)
    _add_key_metrics(pdf, marathon_metrics)
    _add_brand_distribution(pdf, marathon_metrics)
    _add_gender_breakdown(pdf, gender_distribution)
    _add_category_summary(pdf, category_distribution)
    _add_top_finishers(pdf, runners)

    pdf_output = pdf.output(dest="S")
    if isinstance(pdf_output, bytes):
        return pdf_output
    return pdf_output.encode("latin-1")


# --- Section builders -----------------------------------------------------------


def _add_overview(
    pdf: MarathonReportPDF,
    metrics: Dict[str, Any],
    generated_at: datetime,
    generated_by: Optional[str],
) -> None:
    _section_title(pdf, "Visão Geral")

    event_date = metrics.get("event_date") or "Data não informada"
    location = metrics.get("location") or "Local não informado"
    distance = metrics.get("distance_km")
    distance_text = (
        f"{float(distance):.1f} km"
        if isinstance(distance, (int, float))
        else (f"{distance} km" if distance else "Distância não informada")
    )

    info_rows: Sequence[Tuple[str, str]] = [
        ("Prova", metrics.get("marathon_name", "Não informado")),
        ("Data", _format_date(event_date)),
        ("Local", location),
        ("Distância", distance_text),
        ("Gerado em", generated_at.strftime("%d/%m/%Y %H:%M")),
    ]
    if generated_by:
        info_rows += (("Responsável", generated_by),)

    for label, value in info_rows:
        _key_value_row(pdf, label, value)

    pdf.ln(2)


def _add_key_metrics(pdf: MarathonReportPDF, metrics: Dict[str, Any]) -> None:
    cards = [
        ("Participantes", f"{metrics.get('total_participants', 0):,}".replace(",", ".")),
        ("Marcas identificadas", str(metrics.get("total_brands", 0))),
        (
            "Marca líder",
            f"{metrics.get('leader_brand', {}).get('name', 'N/A')} "
            f"({metrics.get('leader_brand', {}).get('percentage', 0):.1f}%)",
        ),
    ]
    _metric_cards(pdf, cards)


def _add_brand_distribution(pdf: MarathonReportPDF, metrics: Dict[str, Any]) -> None:
    _section_title(pdf, "Distribuição de Marcas")

    brand_dist: Dict[str, int] = metrics.get("brand_distribution") or {}
    if not brand_dist:
        _empty_state(pdf, "Nenhuma marca disponível para compor este relatório.")
        return

    total_participants = max(metrics.get("total_participants", 0), 1)
    rows = [
        (
            brand,
            f"{count}",
            f"{(count / total_participants) * 100:.1f}%",
        )
        for brand, count in sorted(brand_dist.items(), key=lambda x: x[1], reverse=True)
    ]
    headers = ("Marca", "Participantes", "Participação")
    _render_table(pdf, headers, rows, col_widths=[70, 40, 40])


def _add_gender_breakdown(pdf: MarathonReportPDF, gender_df: Optional[pd.DataFrame]) -> None:
    _section_title(pdf, "Equilíbrio de Gênero")

    if gender_df is None or gender_df.empty:
        _empty_state(pdf, "Importe dados de gênero para visualizar esta seção.")
        return

    prepared = _prepare_gender_rows(gender_df)
    if not prepared.rows:
        _empty_state(pdf, "Não há distribuição de gênero suficiente para análise.")
        return

    _render_table(pdf, prepared.headers, prepared.rows, col_widths=prepared.widths)


def _add_category_summary(pdf: MarathonReportPDF, category_df: Optional[pd.DataFrame]) -> None:
    _section_title(pdf, "Categorias em Destaque")

    if category_df is None or category_df.empty:
        _empty_state(pdf, "Nenhuma categoria cadastrada para a prova selecionada.")
        return

    rows = _prepare_category_rows(category_df)
    if not rows:
        _empty_state(pdf, "As categorias cadastradas não possuem dados suficientes.")
        return

    headers = ("Categoria", "Total", "Marca líder", "Domínio")
    _render_table(pdf, headers, rows, col_widths=[55, 25, 55, 30])


def _add_top_finishers(pdf: MarathonReportPDF, runners: Optional[Iterable[Dict[str, Any]]]) -> None:
    _section_title(pdf, "Top 5 Colocados")

    if not runners:
        _empty_state(pdf, "Importe posições para listar os corredores em destaque.")
        return

    rows = _prepare_top_finishers_rows(runners)
    if not rows:
        _empty_state(pdf, "Nenhum corredor com posição definida foi encontrado.")
        return

    headers = ("Posição", "Peito", "Marca", "Categoria", "Gênero")
    _render_table(pdf, headers, rows, col_widths=[25, 25, 55, 45, 25])


# --- Formatting helpers --------------------------------------------------------


def _section_title(pdf: MarathonReportPDF, text: str) -> None:
    pdf.ln(6)
    pdf.set_text_color(*ACCENT_COLOR)
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 8, text, ln=1)

    x = pdf.get_x()
    y = pdf.get_y()
    pdf.set_draw_color(*ACCENT_COLOR)
    pdf.set_line_width(0.5)
    pdf.line(x, y, x + 50, y)
    pdf.set_text_color(*TEXT_COLOR)
    pdf.ln(4)


def _key_value_row(pdf: MarathonReportPDF, label: str, value: str) -> None:
    pdf.set_fill_color(*SOFT_BACKGROUND)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(40, 8, f"{label}:", border=0, fill=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 8, value, border=0, ln=1, fill=True)


def _metric_cards(pdf: MarathonReportPDF, cards: Sequence[Tuple[str, str]]) -> None:
    if not cards:
        return

    available_width = pdf.w - pdf.l_margin - pdf.r_margin
    gap = 5
    columns = len(cards)
    card_width = (available_width - (columns - 1) * gap) / columns
    card_height = 24
    start_y = pdf.get_y()

    for idx, (label, value) in enumerate(cards):
        x = pdf.l_margin + idx * (card_width + gap)
        pdf.set_xy(x, start_y)
        pdf.set_fill_color(234, 240, 246)
        pdf.set_draw_color(222, 226, 230)
        pdf.rect(x, start_y, card_width, card_height, style="DF")

        pdf.set_xy(x + 6, start_y + 6)
        pdf.set_text_color(103, 128, 159)
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(card_width - 12, 5, label.upper(), ln=2)

        pdf.set_text_color(*TEXT_COLOR)
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(card_width - 12, 8, value, ln=2)

    pdf.set_y(start_y + card_height + 4)
    pdf.set_text_color(*TEXT_COLOR)


def _render_table(
    pdf: MarathonReportPDF,
    headers: Sequence[str],
    rows: Sequence[Sequence[str]],
    *,
    col_widths: Optional[Sequence[float]] = None,
) -> None:
    if not rows:
        _empty_state(pdf, "Dados insuficientes para exibir a tabela.")
        return

    widths = _resolve_column_widths(pdf, headers, col_widths)

    pdf.set_font("Helvetica", "B", 10)
    pdf.set_fill_color(*PRIMARY_DARK)
    pdf.set_text_color(255, 255, 255)
    for header, width in zip(headers, widths):
        pdf.cell(width, 8, header, border=0, align="L", fill=True)
    pdf.ln(8)

    pdf.set_text_color(*TEXT_COLOR)
    pdf.set_font("Helvetica", "", 10)
    alternate = False
    for row in rows:
        pdf.set_fill_color(*(SOFT_BACKGROUND if alternate else (255, 255, 255)))
        for cell_text, width in zip(row, widths):
            pdf.cell(width, 7, cell_text, border=0, align="L", fill=True)
        pdf.ln(7)
        alternate = not alternate

    pdf.ln(4)


def _empty_state(pdf: MarathonReportPDF, message: str) -> None:
    pdf.set_font("Helvetica", "I", 10)
    pdf.set_text_color(120, 120, 120)
    pdf.cell(0, 8, message, ln=1)
    pdf.set_text_color(*TEXT_COLOR)
    pdf.ln(2)


def _resolve_column_widths(
    pdf: MarathonReportPDF, headers: Sequence[str], supplied: Optional[Sequence[float]]
) -> List[float]:
    if supplied:
        return list(supplied)
    available_width = pdf.w - pdf.l_margin - pdf.r_margin
    base = available_width / len(headers)
    return [base for _ in headers]


def _prepare_gender_rows(df: pd.DataFrame) -> "_GenderTable":
    working = df.copy()
    if "gender" not in working.columns or "shoe_brand" not in working.columns:
        return _GenderTable(headers=(), rows=[], widths=[])

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
        return _GenderTable(headers=(), rows=[], widths=[])

    ordered_columns = [col for col in ["MASCULINO", "FEMININO", "OUTRO"] if col in pivot.columns]
    dynamic_columns = [col for col in pivot.columns if col not in ordered_columns and col != "Total"]
    columns = ordered_columns + dynamic_columns

    headers = ["Marca"] + [col.title() for col in columns] + ["Total"]
    rows: List[List[str]] = []

    for brand, row in pivot.iterrows():
        total = row["Total"] or 0
        formatted = [brand]
        for col in columns:
            share = row[col] / total if total else 0
            formatted.append(f"{share * 100:.0f}%")
        formatted.append(str(int(total)))
        rows.append(formatted)

    # Column widths: give first column more room, others equal share
    # We'll compute when rendering (need pdf instance). Instead, provide ratios.
    return _GenderTable(headers=headers, rows=rows, widths=_gender_column_widths(len(columns)))


def _gender_column_widths(num_gender_columns: int) -> List[float]:
    base_widths = [60]  # Brand column
    if num_gender_columns <= 0:
        return base_widths + [30]
    per_gender = max(20, 40 / max(1, num_gender_columns))
    base_widths.extend([per_gender] * num_gender_columns)
    base_widths.append(30)  # Total column
    return base_widths


def _prepare_category_rows(df: pd.DataFrame) -> List[Tuple[str, str, str, str]]:
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


def _prepare_top_finishers_rows(runners: Iterable[Dict[str, Any]]) -> List[Tuple[str, str, str, str, str]]:
    sortable: List[Tuple[int, Dict[str, Any]]] = []
    for runner in runners:
        position = runner.get("position")
        if position is None:
            continue
        sortable.append((int(position), runner))

    sortable.sort(key=lambda item: item[0])
    clipped = sortable[:5]

    rows: List[Tuple[str, str, str, str, str]] = []
    for position, runner in clipped:
        bib = runner.get("bib") or "-"
        brand = runner.get("shoe_brand") or "-"
        category = runner.get("run_category") or "-"
        gender = runner.get("gender") or "-"
        rows.append((str(position), str(bib), brand, category, gender))
    return rows


def _format_date(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%d/%m/%Y")

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
            return parsed.strftime("%d/%m/%Y")
        except ValueError:
            pass
        return value

    return "Data não informada"


# --- Lightweight data containers ----------------------------------------------


class _GenderTable:
    def __init__(self, headers: Sequence[str], rows: Sequence[Sequence[str]], widths: Sequence[float]):
        self.headers = tuple(headers)
        self.rows = [tuple(row) for row in rows]
        self.widths = list(widths)

# End of file
