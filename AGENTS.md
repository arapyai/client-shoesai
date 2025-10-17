# Repository Guidelines

## Project Structure & Module Organization
- `app.py` is the Streamlit entry point and handles login flow before redirecting to report pages.
- `pages/` contains multipage dashboards (reports, importer, profile) named with the `X_Emoji_Title.py` pattern required by Streamlit.
- `database_abstraction.py` and `database_config.py` expose the SQLAlchemy layer, while `.env` selects SQLite, Postgres, or MySQL targets.
- `assets/` holds static images for the UI; `sample/` keeps example CSVs and seed data for local experiments.
- `scripts/` includes ad-hoc maintenance helpers (e.g., `checkshoes.py`, `fix_sequences.py`); review them before adding new CLIs.
- Shared helpers live in `ui_components.py`, `utils.py`, and `csv_processing.py`, and exploratory database checks sit in `test_db_data.py`.

## Build, Test, and Development Commands
- `python -m venv .venv && source .venv/bin/activate` creates the local virtual environment (Windows: `.venv\Scripts\activate`).
- `pip install -r requirements.txt` installs Streamlit, SQLAlchemy, and database drivers.
- `streamlit run app.py` starts the web UI on localhost; confirm `.env` matches your target database.
- `python manage_db.py user add --email you@example.com --password pass123` seeds credentials before testing login flows.
- `docker compose up -d` provisions the Postgres + pgAdmin stack defined in `docker-compose.yml`.
- `pytest -s` (or `python test_db_data.py`) exercises database reads and prints inspection output—seed marathon data first.

## Coding Style & Naming Conventions
Follow PEP 8 with 4-space indentation and descriptive snake_case for Python identifiers. Streamlit pages rely on the filename prefix to order sidebar entries, so keep new pages inside `pages/` using the existing numbering. Prefer lightweight helpers in `utils.py`, and add brief comments only when layout or SQL logic is non-obvious.

## Testing Guidelines
`pytest` discovers `test_db_data.py`; run it against a populated database to validate marathon metrics pipelines. Name new tests `test_*` and store fixtures under `sample/` or inline factories. Verify both SQLite (`DB_TYPE=sqlite`) and Postgres (`docker compose` stack) when modifying persistence logic.

## Commit & Pull Request Guidelines
Recent history uses short, imperative subjects (e.g., “Remove error message…”). Mirror that style, add context in the body when behavior changes, and reference ticket IDs if available. PRs should describe user-visible impact, list the commands you ran, and attach screenshots or GIFs for UI changes. Call out schema migrations or manual DB steps in the checklist so reviewers can reproduce them.
