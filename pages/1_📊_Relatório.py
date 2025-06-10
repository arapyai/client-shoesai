import streamlit as st
import pandas as pd
from data_processing import process_queried_data_for_report, process_multiple_marathons_efficiently
from ui_components import (
    page_header_with_logout,
    report_page_content_main,
    render_pdf_preview_modal,
    render_marathon_info_cards,
    render_brand_distribution_chart,
    render_gender_by_brand,
    render_top_brands_table,
    render_race_by_brand,
    render_marathon_comparison_chart,
    render_individual_marathon_column
)
from database import get_marathon_list_from_db, get_data_for_selected_marathons_db

# --- Page Config ---
st.set_page_config(layout="wide", page_title="CourtShoes AI - Relatórios")

# --- Authentication Check ---
if not st.session_state.get("logged_in", False):
    st.warning("Por favor, faça login para acessar esta página.")
    st.link_button("Ir para Login", "/")
    st.stop()

# --- Fetch Marathon List from DB for Selector ---
@st.cache_data # Cache the list of marathons
def fetch_marathon_options_from_db_cached():
    return get_marathon_list_from_db()

if 'MARATHON_OPTIONS_DB_CACHED' not in st.session_state:
    st.session_state.MARATHON_OPTIONS_DB_CACHED = fetch_marathon_options_from_db_cached()

MARATHON_NAMES_LIST = [m['name'] for m in st.session_state.MARATHON_OPTIONS_DB_CACHED]
MARATHON_ID_MAP = {m['name']: m['id'] for m in st.session_state.MARATHON_OPTIONS_DB_CACHED}


# --- Session State Initialization for this page (DB oriented) ---
if 'selected_marathon_names_ui' not in st.session_state:
    st.session_state.selected_marathon_names_ui = MARATHON_NAMES_LIST[:1] if MARATHON_NAMES_LIST else []

# Initialize processed_report_data if it doesn't exist or if selections might have changed
# This initial load ensures some data is present when the page first loads or after an import
if 'processed_report_data' not in st.session_state or \
   (st.session_state.selected_marathon_names_ui and not st.session_state.processed_report_data.get("total_images_selected", -1) > -1) : # -1 to force initial load
    
    initial_marathon_ids = [MARATHON_ID_MAP[name] for name in st.session_state.selected_marathon_names_ui if name in MARATHON_ID_MAP]
    # Filter out None values
    initial_marathon_ids = [mid for mid in initial_marathon_ids if mid is not None]
    
    if initial_marathon_ids:
        # Use pre-computed metrics for initial load too
        from database import get_precomputed_marathon_metrics
        st.session_state.processed_report_data = get_precomputed_marathon_metrics(initial_marathon_ids)
    else: # No marathons selected or available yet
        from data_processing import process_queried_data_for_report
        st.session_state.processed_report_data = process_queried_data_for_report(pd.DataFrame(), pd.DataFrame())

if 'show_report_content_db' not in st.session_state:
    st.session_state.show_report_content_db = bool(st.session_state.selected_marathon_names_ui) and \
                                            st.session_state.processed_report_data.get("total_images_selected", 0) > 0
if 'show_pdf_preview_db' not in st.session_state:
    st.session_state.show_pdf_preview_db = False

# --- Helper Functions for Optimized Processing ---
@st.cache_data
def get_individual_marathon_data_cached(marathon_id: int) -> dict:
    """
    Cache individual marathon data to avoid reprocessing.
    Returns processed data for a single marathon using pre-computed metrics.
    """
    from database import get_precomputed_marathon_metrics
    return get_precomputed_marathon_metrics([marathon_id])

def preprocess_individual_marathons(marathon_names: list) -> dict:
    """
    Efficiently preprocess data for multiple marathons using pre-computed metrics.
    Returns a dict mapping marathon_name -> processed_data.
    """
    # Get marathon IDs and filter out None values
    marathon_ids = [MARATHON_ID_MAP.get(name) for name in marathon_names if MARATHON_ID_MAP.get(name) is not None]
    # Filter out any None values to ensure we have only int values
    marathon_ids = [mid for mid in marathon_ids if mid is not None]
    
    if not marathon_ids:
        return {}
    
    # Use the new efficient individual metrics function
    with st.spinner("Carregando dados pré-calculados das provas..."):
        from database import get_individual_marathon_metrics
        
        # Get all individual marathon metrics in a single database call
        individual_data = get_individual_marathon_metrics(marathon_ids)
    
    return individual_data

def render_individual_marathon_column(marathon_name: str, marathon_data: dict):
    """
    Render a single marathon's data in a column.
    Reusable function for individual marathon visualization.
    """
    st.subheader(f"📊 {marathon_name}")
    
    # Create cards for this marathon
    marathon_cards_data = {
        marathon_name: marathon_data.get("marathon_specific_data_for_cards", {}).get(marathon_name, {})
    }
    
    # Display marathon info card
    render_marathon_info_cards(
        [marathon_name], 
        marathon_cards_data,
        st.session_state.get("MARATHON_OPTIONS_DB_CACHED", [])
    )
    
    # Only show charts if there's meaningful data
    has_brand_data = not marathon_data["brand_counts_all_selected"].empty
    has_gender_data = not marathon_data["gender_brand_distribution"].empty
    has_race_data = not marathon_data["race_brand_distribution"].empty
    
    if has_brand_data:
        # Brand distribution
        with st.expander("📊 Distribuição de Marcas", expanded=True):
            render_brand_distribution_chart(
                marathon_data["brand_counts_all_selected"],
                highlight=marathon_data.get("highlight_brands", ["Olympikus", "Mizuno"])
            )
        
        # Gender analysis
        if has_gender_data:
            with st.expander("👥 Análise por Gênero"):
                render_gender_by_brand(marathon_data["gender_brand_distribution"], min_percentage_for_display=5.0)
        
        # Race analysis
        if has_race_data:
            with st.expander("🌍 Análise por Raça"):
                render_race_by_brand(marathon_data["race_brand_distribution"], min_percentage_for_display=5.0)
        
        # Marathon comparison chart
        if len(st.session_state.selected_marathon_names_ui) > 1:
            with st.expander("📈 Comparação de Provas"):
                render_marathon_comparison_chart(
                    marathon_data["brand_counts_by_marathon"], highlight=marathon_data.get("highlight_brands", ["Olympikus", "Mizuno"]),
                )
        # Top brands table
        with st.expander("🏆 Top Marcas"):
            render_top_brands_table(marathon_data["top_brands_all_selected"])
    else:
        st.info("📋 Nenhum dado de marcas disponível para esta prova.")

def render_multiple_marathons_view(selected_marathons: list):
    """
    Efficiently render multiple marathons with different visualization modes.
    """
    st.subheader("🏃‍♂️ Análise por Prova")
    
    # Add visualization mode selector for multiple marathons
    if len(selected_marathons) > 1:
        viz_mode = st.radio(
            "Modo de Visualização:",
            options=["columns", "timeline"],
            format_func=lambda x: "📊 Lado a Lado" if x == "columns" else "📈 Evolução Temporal",
            horizontal=True,
            key="marathon_viz_mode"
        )
    else:
        viz_mode = "columns"  # Default for single marathon
    
    if viz_mode == "columns":
        st.caption("Visualize os dados específicos de cada prova selecionada lado a lado")
        render_columns_view(selected_marathons)
    else:
        st.caption("Visualize a evolução das marcas ao longo do tempo nas provas selecionadas")
        render_timeline_view(selected_marathons)

def render_columns_view(selected_marathons: list):
    """
    Render marathons in side-by-side columns (original approach).
    """
    # Preprocess all marathon data efficiently
    individual_data = preprocess_individual_marathons(selected_marathons)
    
    # Create columns and render each marathon
    cols = st.columns(len(selected_marathons))
    
    for i, marathon_name in enumerate(selected_marathons):
        if marathon_name in individual_data:
            with cols[i]:
                render_individual_marathon_column(marathon_name, individual_data[marathon_name])

def render_timeline_view(selected_marathons: list):
    """
    Render marathons in a timeline view with line charts showing brand evolution.
    """
    from ui_components import render_brand_timeline_chart
    
    # Get all marathon data
    individual_data = preprocess_individual_marathons(selected_marathons)
    
    if not individual_data:
        st.warning("Nenhum dado disponível para as provas selecionadas.")
        return
    
    # Prepare timeline data
    timeline_data = prepare_timeline_data(individual_data, selected_marathons)
    
    if timeline_data.empty:
        st.warning("Não há dados suficientes para gerar a visualização temporal.")
        return
    
    # Render the timeline chart
    render_brand_timeline_chart(timeline_data)

def prepare_timeline_data(individual_data: dict, selected_marathons: list) -> pd.DataFrame:
    """
    Prepare data for timeline visualization by combining marathon data with dates.
    
    Args:
        individual_data: Dictionary containing processed data for each marathon
        selected_marathons: List of selected marathon names
        
    Returns:
        DataFrame with columns: marathon_name, event_date, brand, percentage
    """
    timeline_records = []
    
    # Get marathon metadata for dates
    marathon_metadata = st.session_state.get("MARATHON_OPTIONS_DB_CACHED", [])
    
    for marathon_name in selected_marathons:
        if marathon_name not in individual_data:
            continue
            
        marathon_data = individual_data[marathon_name]
        
        # Get event date for this marathon
        marathon_meta = next((m for m in marathon_metadata if m['name'] == marathon_name), None)
        event_date = marathon_meta.get('event_date') if marathon_meta else None
        
        if not event_date:
            # Skip marathons without dates
            continue
            
        # Parse the event date (assuming format YYYY-MM-DD)
        try:
            event_date_parsed = pd.to_datetime(event_date)
        except:
            # If date parsing fails, skip this marathon
            continue
        
        # Get brand counts for this marathon
        brand_counts = marathon_data.get("brand_counts_all_selected", pd.Series())
        
        if brand_counts.empty:
            continue
            
        # Calculate percentages
        total_shoes = brand_counts.sum()
        
        for brand, count in brand_counts.items():
            percentage = (count / total_shoes) * 100 if total_shoes > 0 else 0
            
            timeline_records.append({
                'marathon_name': marathon_name,
                'event_date': event_date_parsed,
                'brand': brand,
                'count': count,
                'percentage': percentage
            })
    
    # Convert to DataFrame
    timeline_df = pd.DataFrame(timeline_records)
    
    if timeline_df.empty:
        return timeline_df
    
    # Sort by date
    timeline_df = timeline_df.sort_values('event_date')
    
    return timeline_df

# --- Main Report Page UI ---
def report_page_db():
    # Use the reusable page header component
    page_header_with_logout("📊 Análise de Provas", 
                           "Aqui você pode gerar relatórios e exportá-los. Selecione as provas que gostaria de analisar.",
                           key_suffix="reports")

    cols_actions = st.columns([3, 1, 1, 1.5]) # Adjusted for button text
    with cols_actions[0]:
        current_selection_in_ui = list(st.session_state.selected_marathon_names_ui) # Make a copy
        
        selected_marathon_names_from_multiselect = st.multiselect(
            "Selecione quais \"provas\" (datasets) analisar:",
            options=MARATHON_NAMES_LIST,
            default=current_selection_in_ui,
            key="marathon_selector_db_page"
        )
    
    # Detect if selection changed in multiselect to update session state and rerun
    if selected_marathon_names_from_multiselect != current_selection_in_ui:
        st.session_state.selected_marathon_names_ui = selected_marathon_names_from_multiselect
        st.session_state.show_report_content_db = False # Important: reset to force regeneration
        st.rerun()

    disable_generate_button = not st.session_state.selected_marathon_names_ui
    disable_export_buttons = not st.session_state.show_report_content_db

    with cols_actions[1]:
        if st.button("Gerar relatório", type="primary", use_container_width=True,
                      disabled=disable_generate_button, key="generate_report_db_btn_main"):
            if st.session_state.selected_marathon_names_ui:
                selected_ids = [MARATHON_ID_MAP[name] for name in st.session_state.selected_marathon_names_ui if name in MARATHON_ID_MAP]
                if selected_ids:
                    with st.spinner("Buscando dados pré-calculados do banco..."):
                        # Try to use pre-computed metrics first
                        from database import get_precomputed_marathon_metrics
                        st.session_state.processed_report_data = get_precomputed_marathon_metrics(selected_ids)
                        st.session_state.show_report_content_db = True
                    st.rerun() 
                else:
                    st.warning("Nenhum ID de maratona válido encontrado para a seleção.")
                    st.session_state.show_report_content_db = False
            else:
                st.warning("Por favor, selecione ao menos uma prova/pasta.")
                st.session_state.show_report_content_db = False
    
    csv_data_str = ""
    can_export_csv_flag = False
    if st.session_state.show_report_content_db and \
       "top_brands_all_selected" in st.session_state.processed_report_data and \
       not st.session_state.processed_report_data["top_brands_all_selected"].empty:
        csv_data_str = st.session_state.processed_report_data["top_brands_all_selected"].to_csv(index=False).encode('utf-8')
        can_export_csv_flag = False

    with cols_actions[2]: #disabled=disable_export_buttons desabilitado temporariamente
        if st.button("Exportar PDF", use_container_width=True,
                      disabled=True, key="export_pdf_db_btn_main"):
            st.session_state.show_pdf_preview_db = True

    with cols_actions[3]:
        st.download_button(
            "Exportar Top Marcas (CSV)",
            data=csv_data_str if can_export_csv_flag else "Nenhum dado para exportar.",
            file_name="top_marcas_report.csv",
            mime="text/csv",
            use_container_width=True,
            disabled=not can_export_csv_flag,
            key="export_csv_db_btn_main"
        )
    st.markdown("---")

    # --- Display Content or Modal ---
    # Data for cards needs to be p
    #if debug=True in url parameters, show raw dataassed specifically for selected marathons
    # `processed_report_data` already contains `marathon_specific_data_for_cards`
    
    if st.session_state.show_pdf_preview_db and st.session_state.show_report_content_db:
        # Pass the already processed data for the selected marathons
        render_pdf_preview_modal(st.session_state.processed_report_data, 
                                 st.session_state.processed_report_data.get("marathon_specific_data_for_cards", {}))
    elif st.session_state.show_report_content_db and st.session_state.selected_marathon_names_ui:
        selected_marathons = st.session_state.selected_marathon_names_ui
        
        # Always use the optimized column-based rendering
        render_multiple_marathons_view(selected_marathons)
    else:
        st.markdown("""
            <div style="text-align: center; padding: 50px;">
                <span style="font-size: 80px;">🏃‍♂️</span>
                <h3>Escolha sua prova/pasta</h3>
                <p>Para iniciar selecione uma ou mais provas/pastas e clique em 'Gerar relatório'</p>
            </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    if not st.session_state.get("logged_in", False):
        st.warning("Por favor, faça login para acessar esta página.")
        st.link_button("Ir para Login", "/")
        st.stop()
    else:
        report_page_db()