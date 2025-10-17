"""
Document scaffolding for the marathon PDF report.
"""

from __future__ import annotations

from .styles import BASE_CSS, TITLE_SUBTITLE


def render_document(
    *,
    marathon_title: str,
    sections_html: str,
    generated_text: str,
) -> str:
    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="utf-8" />
    <title>{marathon_title}</title>
    <style>{BASE_CSS}</style>
</head>
<body>
    <header class="hero">
        <h1>{marathon_title}</h1>
        <p>{TITLE_SUBTITLE}</p>
    </header>
    <main>
        {sections_html}
    </main>
    <footer>
        <p>{generated_text}</p>
    </footer>
</body>
</html>
"""

