import streamlit as st
import pandas as pd
from ui_components import (
    page_header_with_logout,
    render_brand_distribution_chart,
    render_demographic_analysis,
    render_marathon_info_cards,
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

# --- Page Header and Marathon Selection ---
page_header_with_logout("📊 Relatórios de Análise", 
                        "Visualize e analise os dados das provas importadas.", 
                        key_suffix="reports")

# Marathon selection
st.subheader("🏃‍♂️ Seleção de Provas")

if not MARATHON_NAMES_LIST:
    st.warning("⚠️ Nenhuma prova encontrada no sistema. Importe dados na página de Importação primeiro.")
    if st.button("📥 Ir para Importação"):
        st.switch_page("pages/3_📥_Importador_de_Dados.py")
    st.stop()

# Simple marathon selector
selected_marathon = st.selectbox(
    "Escolha uma prova para análise:",
    options=MARATHON_NAMES_LIST,
    key="selected_marathon_simple"
)

if selected_marathon and selected_marathon in MARATHON_ID_MAP:
    marathon_id = MARATHON_ID_MAP[selected_marathon]
    
    # Get marathon data
    with st.spinner("🔄 Carregando dados da prova..."):
        marathon_data = db.get_individual_marathon_metrics(marathon_id)
    
    if marathon_data:
        # Marathon info card
        render_marathon_info_cards(marathon_data)
        
        # Brand distribution chart
        render_brand_distribution_chart(marathon_data)

        # Demographic analysis
        render_demographic_analysis(marathon_data)    

        # Export options
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
                            file_name=f"{selected_marathon}_dados.csv",
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
            st.sidebar.write(f"Prova selecionada: {selected_marathon}")
            st.sidebar.write(f"ID da prova: {MARATHON_ID_MAP.get(selected_marathon, 'N/A')}")
            st.sidebar.json(marathon_data, expanded=False)
    else:
        st.error(f"❌ Não foi possível carregar os dados da prova '{selected_marathon}'.")
else:
    st.info("👆 Selecione uma prova acima para visualizar o relatório.")
