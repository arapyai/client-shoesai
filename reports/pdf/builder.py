"""
Public builder that orchestrates PDF rendering using WeasyPrint.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, Optional

import pandas as pd
from weasyprint import HTML

from .charts import build_report_charts
from .document import render_document
from .sections import build_sections
from .styles import DEFAULT_GENERATED_BY


def generate_marathon_pdf_report(
    marathon_metrics: Dict[str, object],
    *,
    gender_distribution: Optional[pd.DataFrame] = None,
    category_distribution: Optional[pd.DataFrame] = None,
    runners: Optional[Iterable[Dict[str, object]]] = None,
    generated_by: Optional[str] = None,
    generated_at: Optional[datetime] = None,
) -> bytes:
    """
    Build a PDF report summarising insights for a single marathon using WeasyPrint.

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
        raise ValueError("Marathon metrics são obrigatórias para gerar o relatório.")

    generated_at = generated_at or datetime.now()
    marathon_name = str(marathon_metrics.get("marathon_name", "Prova"))

    charts = build_report_charts(
        metrics=marathon_metrics,
        gender_df=gender_distribution,
        category_df=category_distribution,
    )

    sections_html = build_sections(
        metrics=marathon_metrics,
        gender_distribution=gender_distribution,
        category_distribution=category_distribution,
        runners=runners,
        generated_at=generated_at,
        generated_by=generated_by,
        charts=charts,
    )

    generator_label = generated_by or DEFAULT_GENERATED_BY
    generated_text = f"Gerado em {generated_at.strftime('%d/%m/%Y %H:%M')} por {generator_label}"
    marathon_title = f"Relatório • {marathon_name}"

    document = render_document(
        marathon_title=marathon_title,
        sections_html=sections_html,
        generated_text=generated_text,
    )

    return HTML(string=document).write_pdf()

