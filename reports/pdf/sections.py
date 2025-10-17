"""
HTML section builders for the marathon PDF report.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, Optional, Sequence, Tuple

import pandas as pd

from .charts import ReportCharts
from .styles import TEXT_MUTED
from .utils import (
    GenderTable,
    format_date,
    format_distance,
    format_integer,
    prepare_category_rows,
    prepare_gender_table,
    prepare_top_finishers_rows,
)


def build_sections(
    *,
    metrics: Dict[str, object],
    gender_distribution: Optional[pd.DataFrame],
    category_distribution: Optional[pd.DataFrame],
    runners: Optional[Iterable[Dict[str, object]]],
    generated_at: datetime,
    generated_by: Optional[str],
    charts: ReportCharts,
) -> str:
    sections = [
        _overview_section(metrics, generated_at, generated_by),
        _key_metrics_section(metrics),
        _brand_distribution_section(metrics, charts.brand_distribution),
        _gender_section(gender_distribution, charts.gender_distribution),
        _category_section(category_distribution, charts.category_distribution),
        _top_finishers_section(runners),
    ]
    return "".join(part for part in sections if part)


def _overview_section(
    metrics: Dict[str, object],
    generated_at: datetime,
    generated_by: Optional[str],
) -> str:
    event_date = metrics.get("event_date") or "Data não informada"
    location = metrics.get("location") or "Local não informado"
    distance = metrics.get("distance_km")

    info_rows: Sequence[Tuple[str, str]] = [
        ("Prova", str(metrics.get("marathon_name", "Não informado"))),
        ("Data", format_date(event_date)),
        ("Local", str(location)),
        ("Distância", format_distance(distance)),
        ("Gerado em", generated_at.strftime("%d/%m/%Y %H:%M")),
    ]
    if generated_by:
        info_rows += (("Responsável", str(generated_by)),)

    items_html = "".join(
        f"<div class='info-item'><span>{label}</span><strong>{value}</strong></div>"
        for label, value in info_rows
    )

    return f"""
    <section class="section">
        <h2>Visão Geral</h2>
        <div class="info-grid">
            {items_html}
        </div>
    </section>
    """


def _key_metrics_section(metrics: Dict[str, object]) -> str:
    total_participants = format_integer(metrics.get("total_participants", 0))
    total_brands = format_integer(metrics.get("total_brands", 0))

    leader_brand = metrics.get("leader_brand") or {}
    leader_name = leader_brand.get("name", "N/A") if isinstance(leader_brand, dict) else "N/A"
    leader_percentage = leader_brand.get("percentage") if isinstance(leader_brand, dict) else None
    leader_percentage_text = f"{leader_percentage:.1f}%" if isinstance(leader_percentage, (int, float)) else "-"

    cards = [
        ("Participantes", total_participants),
        ("Marcas identificadas", total_brands),
        ("Marca líder", f"{leader_name} ({leader_percentage_text})"),
    ]

    cards_html = "".join(
        f"<div class='metric-card'><span>{label}</span><strong>{value}</strong></div>"
        for label, value in cards
    )

    return f"""
    <section class="section">
        <h2>Indicadores-chave</h2>
        <div class="metric-cards">
            {cards_html}
        </div>
    </section>
    """


def _brand_distribution_section(
    metrics: Dict[str, object],
    chart_uri: Optional[str],
) -> str:
    brand_dist = metrics.get("brand_distribution") or {}

    if not brand_dist:
        return _section_with_message(
            "Distribuição de Marcas",
            "Nenhuma marca disponível para compor este relatório.",
        )

    total_participants_raw = metrics.get("total_participants")
    total_participants = (
        max(int(total_participants_raw), 1) if isinstance(total_participants_raw, (int, float)) else 1
    )
    rows = [
        (
            str(brand),
            format_integer(count),
            f"{(count / total_participants) * 100:.1f}%",
        )
        for brand, count in sorted(brand_dist.items(), key=lambda item: item[1], reverse=True)
    ]

    chart_html = _chart_html(chart_uri, "Gráfico de distribuição de marcas") if chart_uri else ""
    table_html = _table_html(("Marca", "Participantes", "Participação"), rows)

    return f"""
    <section class="section">
        <h2>Distribuição de Marcas</h2>
        {chart_html}
        {table_html}
    </section>
    """


def _gender_section(
    gender_df: Optional[pd.DataFrame],
    chart_uri: Optional[str],
) -> str:
    if gender_df is None or gender_df.empty:
        return _section_with_message(
            "Equilíbrio de Gênero",
            "Importe dados de gênero para visualizar esta seção.",
        )

    prepared: GenderTable = prepare_gender_table(gender_df)

    if not prepared.rows:
        return _section_with_message(
            "Equilíbrio de Gênero",
            "Não há distribuição de gênero suficiente para análise.",
        )

    chart_html = _chart_html(chart_uri, "Distribuição de gênero por marca") if chart_uri else ""
    table_html = _table_html(prepared.headers, prepared.rows)

    return f"""
    <section class="section">
        <h2>Equilíbrio de Gênero</h2>
        {chart_html}
        {table_html}
    </section>
    """


def _category_section(
    category_df: Optional[pd.DataFrame],
    chart_uri: Optional[str],
) -> str:
    if category_df is None or category_df.empty:
        return _section_with_message(
            "Categorias em Destaque",
            "Nenhuma categoria cadastrada para a prova selecionada.",
        )

    rows = prepare_category_rows(category_df)
    if not rows:
        return _section_with_message(
            "Categorias em Destaque",
            "As categorias cadastradas não possuem dados suficientes.",
        )

    chart_html = _chart_html(chart_uri, "Participação de marcas por categoria") if chart_uri else ""
    table_html = _table_html(
        ("Categoria", "Total", "Marca líder", "Domínio"),
        rows,
    )

    return f"""
    <section class="section">
        <h2>Categorias em Destaque</h2>
        {chart_html}
        {table_html}
    </section>
    """


def _top_finishers_section(runners: Optional[Iterable[Dict[str, object]]]) -> str:
    if not runners:
        return _section_with_message(
            "Top 5 Colocados",
            "Importe posições para listar os corredores em destaque.",
        )

    rows = prepare_top_finishers_rows(runners)
    if not rows:
        return _section_with_message(
            "Top 5 Colocados",
            "Nenhum corredor com posição definida foi encontrado.",
        )

    headers = ("Posição", "Peito", "Marca", "Categoria", "Gênero")
    table_html = _table_html(headers, rows)
    note = "<p class='section-note'>Considera até cinco corredores com posição válida.</p>"

    return f"""
    <section class="section">
        <h2>Top 5 Colocados</h2>
        {table_html}
        {note}
    </section>
    """


def _table_html(
    headers: Sequence[str],
    rows: Sequence[Sequence[str]],
) -> str:
    head_cells = "".join(f"<th scope='col'>{header}</th>" for header in headers)
    body_rows = []
    for row in rows:
        cells = "".join(f"<td>{cell}</td>" for cell in row)
        body_rows.append(f"<tr>{cells}</tr>")
    body_html = "".join(body_rows)
    return f"""
    <table>
        <thead>
            <tr>{head_cells}</tr>
        </thead>
        <tbody>
            {body_html}
        </tbody>
    </table>
    """


def _section_with_message(title: str, message: str) -> str:
    return f"""
    <section class="section">
        <h2>{title}</h2>
        <p class="empty-state">{message}</p>
    </section>
    """


def _chart_html(chart_uri: str, alt_text: str) -> str:
    return f"""
    <div class="chart">
        <img src="{chart_uri}" alt="{alt_text}" />
        <p class="section-note" style="color:{TEXT_MUTED};">Cores alinhadas à experiência do dashboard web.</p>
    </div>
    """
