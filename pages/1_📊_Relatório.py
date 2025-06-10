import streamlit as st
import pandas as pd
from data_processing import process_queried_data_for_report # Updated function name
from ui_components import (
    page_header_with_logout,
    report_page_content_main, # This will be called with processed_metrics
    render_pdf_preview_modal,   # This will also be called with processed_metrics
    render_marathon_info_cards,
    render_brand_distribution_chart,
    render_gender_by_brand,
    render_top_brands_table,
    render_race_by_brand,
    render_marathon_comparison_chart
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
    if initial_marathon_ids:
        df_flat, df_raw_reconstructed = get_data_for_selected_marathons_db(initial_marathon_ids)
        st.session_state.processed_report_data = process_queried_data_for_report(df_flat, df_raw_reconstructed)
    else: # No marathons selected or available yet
        st.session_state.processed_report_data = process_queried_data_for_report(pd.DataFrame(), pd.DataFrame())

if 'show_report_content_db' not in st.session_state:
    st.session_state.show_report_content_db = bool(st.session_state.selected_marathon_names_ui) and \
                                            st.session_state.processed_report_data.get("total_images_selected", 0) > 0
if 'show_pdf_preview_db' not in st.session_state:
    st.session_state.show_pdf_preview_db = False

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
                    with st.spinner("Buscando e processando dados do banco..."):
                        df_flat_from_db, df_raw_reconstructed_from_db = get_data_for_selected_marathons_db(selected_ids)
                        st.session_state.processed_report_data = process_queried_data_for_report(df_flat_from_db, df_raw_reconstructed_from_db)
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
        # Get each individual marathon data and display in columns
        selected_marathons = st.session_state.selected_marathon_names_ui
        
        if len(selected_marathons) == 1:
            # If only one marathon is selected, display it normally
            report_page_content_main(st.session_state.processed_report_data, 
                                    st.session_state.processed_report_data.get("marathon_specific_data_for_cards", {}))
        else:
            # If multiple marathons are selected, create columns for each one
            st.subheader("Análise Individual por Prova")
            st.caption("Visualize os dados específicos de cada prova selecionada")
            
            # Create columns for each marathon
            cols = st.columns(len(selected_marathons))
            
            # Process each marathon and display in its own column
            for i, marathon_name in enumerate(selected_marathons):
                marathon_id = MARATHON_ID_MAP.get(marathon_name)
                
                if marathon_id:
                    with cols[i]:                        
                        # Get data for this specific marathon only
                        with st.spinner(f"Carregando dados da prova {marathon_name}..."):
                            df_flat, df_raw = get_data_for_selected_marathons_db([marathon_id])
                            marathon_data = process_queried_data_for_report(df_flat, df_raw)
                        
                        # Create cards for just this marathon
                        marathon_cards_data = {
                            marathon_name: marathon_data.get("marathon_specific_data_for_cards", {}).get(marathon_name, {})
                        }
                        
                        # Display this marathon's data
                        render_marathon_info_cards(
                            [marathon_name], 
                            marathon_cards_data,
                            st.session_state.get("MARATHON_OPTIONS_DB_CACHED", [])
                        )
                        
                        # Display brand distribution for this marathon
                        if not marathon_data["brand_counts_all_selected"].empty:
                            render_brand_distribution_chart(
                                marathon_data["brand_counts_all_selected"],
                                highlight=marathon_data.get("highlight_brands", ["Olympikus", "Mizuno"])
                            )
                            
                        # Display gender data if available
                        if not marathon_data["gender_brand_distribution"].empty:
                            render_gender_by_brand(marathon_data["gender_brand_distribution"], min_percentage_for_display=5.0)
                        
                        st.markdown("---")
                        
                        render_race_by_brand(marathon_data["race_brand_distribution"], min_percentage_for_display=5.0)

                        st.markdown("---")
                        render_marathon_comparison_chart(marathon_data["brand_counts_by_marathon"],
                                                                highlight=marathon_data.get("highlight_brands", ["Olympikus", "Mizuno"]))

                        render_top_brands_table(marathon_data["top_brands_all_selected"])
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