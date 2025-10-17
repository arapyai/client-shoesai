"""
Styling constants shared across PDF rendering modules.
"""

PRIMARY_BLUE = "#3498db"
ACCENT_ORANGE = "#e74c3c"
TEXT_DARK = "#1f2a44"
TEXT_MUTED = "#64748b"
SOFT_BACKGROUND = "#f5f7fa"
SECTION_DIVIDER = "#dbe4f3"
TABLE_BORDER = "#e3e8ef"
CARD_GRADIENT_START = "#f2f8fc"
CARD_GRADIENT_END = "#eaf2fb"
WHITE = "#ffffff"

GENDER_COLORS = {
    "MASCULINO": "#1f77b4",
    "FEMININO": "#990785",
    "OUTRO": "#7f8c8d",
}

TITLE_SUBTITLE = "Relatório analítico de participação e marcas"
DEFAULT_GENERATED_BY = "ShoesAI Dashboard"

BASE_CSS = f"""
@page {{
    size: A4;
    margin: 20mm 18mm 25mm 18mm;
    @bottom-right {{
        content: "Página " counter(page) " de " counter(pages);
        font-size: 9pt;
        color: {TEXT_MUTED};
    }}
}}

body {{
    font-family: "Helvetica Neue", Arial, sans-serif;
    color: {TEXT_DARK};
    background: {WHITE};
    font-size: 11pt;
    line-height: 1.45;
}}

header.hero {{
    padding: 6mm 0 10mm;
    border-bottom: 3px solid {PRIMARY_BLUE};
}}

header.hero h1 {{
    margin: 0;
    font-size: 22pt;
    color: {TEXT_DARK};
}}

header.hero p {{
    margin: 6px 0 0;
    color: {TEXT_MUTED};
    font-size: 11pt;
}}

main {{
    margin-top: 12mm;
}}

.section {{
    margin-bottom: 18mm;
}}

.section + .section {{
    border-top: 1px solid {SECTION_DIVIDER};
    padding-top: 14mm;
}}

.section h2 {{
    font-size: 15pt;
    margin: 0 0 6px;
    color: {PRIMARY_BLUE};
    letter-spacing: 0.2px;
}}

.section h2::after {{
    content: "";
    display: block;
    width: 52px;
    height: 3px;
    margin-top: 4px;
    background: {PRIMARY_BLUE};
    opacity: 0.35;
}}

.info-grid {{
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin-top: 12px;
}}

.info-item {{
    flex: 1 1 180px;
    background: {SOFT_BACKGROUND};
    padding: 12px 16px;
    border-radius: 10px;
    border: 1px solid {SECTION_DIVIDER};
}}

.info-item span {{
    display: block;
    font-size: 9pt;
    color: {TEXT_MUTED};
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 4px;
}}

.info-item strong {{
    font-size: 12pt;
    color: {TEXT_DARK};
    font-weight: 700;
}}

.metric-cards {{
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    margin-top: 16px;
}}

.metric-card {{
    flex: 1 1 160px;
    background: linear-gradient(145deg, {CARD_GRADIENT_START}, {CARD_GRADIENT_END});
    border: 1px solid {SECTION_DIVIDER};
    border-radius: 12px;
    padding: 14px 18px;
}}

.metric-card span {{
    display: block;
    font-size: 9pt;
    color: {TEXT_MUTED};
    text-transform: uppercase;
    letter-spacing: 0.4px;
    margin-bottom: 4px;
}}

.metric-card strong {{
    font-size: 15pt;
    color: {TEXT_DARK};
}}

.chart {{
    margin-top: 14px;
}}

.chart img {{
    width: 100%;
    border-radius: 12px;
    border: 1px solid {SECTION_DIVIDER};
    background: {WHITE};
    padding: 10px;
    box-sizing: border-box;
}}

table {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 12px;
    font-size: 10pt;
}}

thead tr {{
    background: {TEXT_DARK};
    color: {WHITE};
}}

th, td {{
    padding: 8px 10px;
    text-align: left;
    border: 1px solid {TABLE_BORDER};
}}

tbody tr:nth-child(even) {{
    background: {SOFT_BACKGROUND};
}}

.empty-state {{
    margin: 12px 0 0;
    font-style: italic;
    color: {TEXT_MUTED};
}}

.section-note {{
    margin: 8px 0 0;
    font-size: 9pt;
    color: {TEXT_MUTED};
}}

footer {{
    margin-top: 20mm;
    padding-top: 8px;
    border-top: 1px solid {SECTION_DIVIDER};
    font-size: 9.5pt;
    color: {TEXT_MUTED};
}}
"""

