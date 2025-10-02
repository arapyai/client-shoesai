import streamlit as st
import pandas as pd
from ui_components import (
    page_header_with_logout,
    render_brand_distribution_chart,
    render_marathon_info_cards,
    render_demographic_analysis,
    render_category_distribution_analysis,
    check_auth
)
from database_abstraction import db

# --- Page Config ---
st.set_page_config(layout="wide", page_title="Shoes AI - Relatórios")

# --- Authentication Check ---
user_id = check_auth()

# --- Fetch Marathon List from DB for Selector ---
@st.cache_data
def fetch_marathon_options_from_db_cached():
    return db.get_marathon_list_from_db()

marathon_options = fetch_marathon_options_from_db_cached()
MARATHON_NAMES_LIST = [m['name'] for m in marathon_options]
MARATHON_ID_MAP = {m['name']: m['id'] for m in marathon_options}
HIGHLIGHT_BRANDS = ["Olympikus", "Mizuno"]

# --- Page Header and Marathon Selection ---
page_header_with_logout("📊 Análise de Provas", 
                        "Selecione as provas que gostaria de analisar. Os relatórios são gerados automaticamente.", 
                        key_suffix="reports")

if not MARATHON_NAMES_LIST:
    st.warning("⚠️ Nenhuma prova encontrada no sistema. Importe dados na página de Importação primeiro.")
    if st.button("📥 Ir para Importação"):
        st.switch_page("pages/3_📥_Importador_de_Dados.py")
    st.stop()

# Simple marathon selector
selected_marathon = st.multiselect(
    "Escolha uma prova para análise:",
    options=MARATHON_NAMES_LIST,
    key="selected_marathon_simple"
)

if selected_marathon:
    selected_ids = [MARATHON_ID_MAP[name] for name in selected_marathon]
    
    # Determine layout based on number of selected marathons
    num_marathons = len(selected_ids)
    
    if num_marathons <= 3:
        # Display side by side without scrollbar
        cols = st.columns(num_marathons)
    else:
        # Display with horizontal scrolling using columns
        st.markdown("""
        <style>
        .scrollable-row {
            display: flex;
            overflow-x: auto;
            gap: 1rem;
        }
        .scrollable-row > div {
            min-width: 400px;
            flex-shrink: 0;
        }
        </style>
        """, unsafe_allow_html=True)
        cols = st.columns(num_marathons)
    
    # Display each marathon in its column
    for idx, (marathon_id, marathon_name) in enumerate(zip(selected_ids, selected_marathon)):
        with cols[idx]:
            # Get marathon data
            with st.spinner(f"🔄 Carregando {marathon_name}..."):
                marathon_data = db.get_individual_marathon_metrics(marathon_id)
            
            if marathon_data:
                # Marathon info card
                st.subheader(f"📊 {marathon_data.get('marathon_name')}")

                with st.container(border=True):
                    render_marathon_info_cards(marathon_data)
                
                # Brand distribution chart
                with st.expander("Distribuição de Marcas", expanded=True):
                    render_brand_distribution_chart(marathon_data, highlight=HIGHLIGHT_BRANDS)

                # Demographic analysis
                with st.expander("Presença de marcas por gênero", expanded=True):
                    gender_data = db.get_gender_brand_distribution(marathon_id)
                    render_demographic_analysis(gender_data)
                
                with st.expander("Presença de marcas por distância.", expanded=True):
                    category_data = db.get_category_brand_distribution(marathon_id)
                    render_category_distribution_analysis(category_data, highlight=HIGHLIGHT_BRANDS)
            else:
                st.error(f"❌ Não foi possível carregar os dados da prova '{marathon_name}'.")
    
    # Export options (shown after all marathons)
    if num_marathons == 1:
        marathon_id = selected_ids[0]
        st.markdown("---")
        st.subheader("📤 Opções de Exportação")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📊 Exportar Dados (CSV)", use_container_width=True):
                # Create CSV from marathon data
                try:
                    runners_data = db.get_marathon_runners(marathon_id)
                    if runners_data:
                        df = pd.DataFrame(runners_data)
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="⬇️ Download CSV",
                            data=csv,
                            file_name=f"{selected_marathon[0]}_dados.csv",
                            mime="text/csv"
                        )
                    else:
                        st.warning("Nenhum dado encontrado para exportação.")
                except Exception as e:
                    st.error(f"Erro ao exportar: {e}")
        
        if st.button("📈 Exportar Gráficos", use_container_width=True):
            st.info("🚧 Funcionalidade em desenvolvimento")
        
        if st.button("📄 Gerar Relatório PDF", use_container_width=True):
            st.info("🚧 Funcionalidade em desenvolvimento")
    
    # Debug info (for development)
    if st.sidebar.checkbox("🔧 Mostrar Info de Debug"):
        st.sidebar.subheader("Debug Info")
        st.sidebar.write(f"Provas disponíveis: {len(MARATHON_NAMES_LIST)}")
        st.sidebar.write(f"Provas selecionadas: {selected_marathon}")
        st.sidebar.write(f"IDs das provas: {selected_ids}")
    else:
        st.error(f"❌ Não foi possível carregar os dados da prova '{selected_marathon}'.")
else:
    st.info("👆 Selecione uma prova acima para visualizar o relatório.")
