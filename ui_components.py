import streamlit as st
import pandas as pd

# --- UI Components for Main App ---

def display_header():
    st.markdown(f"## 🏃 Análise de Provas (Pastas)")
    st.caption("Aqui você pode gerar relatórios e exportá-los. Selecione as \"provas\" (pastas de imagens) que gostaria de analisar.")

def render_marathon_info_cards(selected_marathon_names, marathon_specific_data_for_cards, db_marathon_metadata_list):
    """
    Renders cards for each selected marathon.
    marathon_specific_data_for_cards: dict from processed_metrics with counts per marathon
    db_marathon_metadata_list: list of dicts from get_marathon_list_from_db, containing metadata like date, location
    """
    if not selected_marathon_names:
        return
    
    cols_needed = len(selected_marathon_names)
    if cols_needed == 0: return

    cols = st.columns(cols_needed)
    for i, marathon_name in enumerate(selected_marathon_names):
        card_data = marathon_specific_data_for_cards.get(marathon_name, {})
        
        # Find metadata for this marathon from the list fetched from DB
        marathon_meta = next((m for m in db_marathon_metadata_list if m['name'] == marathon_name), None)
        event_date = marathon_meta.get('event_date', "XX/XX/XXXX") if marathon_meta else "XX/XX/XXXX"
        location = marathon_meta.get('location', "Local Desconhecido") if marathon_meta else "Local Desconhecido"
        # distance = marathon_meta.get('distance_km', "Distância Desconhecida") if marathon_meta else "Distância Desconhecida"


        with cols[i]:
            with st.container(border=True):
                st.subheader(marathon_name)
                st.caption(f"🗓️ {event_date} | 📍 {location}")
                # st.caption(f"📏 {distance} km") # You can add distance if it's in your Marathons table and fetched
                st.caption(f"🖼️ {card_data.get('images_count', 'N/A')} Imagens")
                st.caption(f"👟 {card_data.get('shoes_count','N/A')} Tênis Detectados")
                st.caption(f"👥 {card_data.get('persons_count', 'N/A')} Pessoas com Demografia")


def render_executive_summary(data):
    st.subheader("📝 Resumo Executivo (Agregado)")
    
    leader_info = data["leader_brand_info"]
    cols = st.columns(3)
    with cols[0]:
        st.metric(label="Marca Líder", value=leader_info["name"], help=f"{leader_info['count']} tênis desta marca encontrados no total.", border=True)
    with cols[1]:
        st.metric(label="Participação da Marca Líder",
                  value=f"{leader_info['percentage']:.1f}%",
                  help=f"{data['unique_brands_count']} marcas diferentes encontradas no total." , border=True)
    with cols[2]:
        st.metric(label="Cobertura Amostral (Pessoas)",
                  value=f"{data['persons_analyzed_count']} Pessoas",
                  help=f"Total de pessoas com dados demográficos analisados nas provas selecionadas.",
                  border=True)

    cols2 = st.columns(2)
    with cols2[0]:
         st.metric(label="Marcas Reconhecidas",
                   value=f"{data['unique_brands_count']} Marcas",
                   help=f"{data['total_shoes_detected']} tênis no total analisados nas provas selecionadas.",
                   border=True)
    with cols2[1]:
        st.metric(label="Intervalo de Confiança",
                  value="98% (mock)",
                  help="Nível de confiança e margem de erro calculada (mock)",
                  border=True)

    st.markdown("---")
    insight_text = "Nenhum insight gerado (sem dados suficientes)."
    if leader_info["name"] != "N/A" and leader_info['percentage'] > 0:
        insight_text = f"""
        ✨ **Insight-Chave (Agregado)**
        
        A marca **{leader_info["name"]}** demonstra uma presença significativa, capturando **{leader_info['percentage']:.1f}%** do share entre os tênis identificados nas provas selecionadas.
        A análise detalhada por prova e demografia pode revelar nuances importantes.
        """
    st.info(insight_text)
    st.markdown("---")

def render_processing_stats(data):
    st.subheader("⚙️ Estatísticas de Processamento (Agregado)")
    cols = st.columns(3)
    with cols[0]:
        st.metric("Total de Imagens Analisadas", str(data["total_images_selected"]), border=True)
    with cols[1]:
        st.metric("Modelo Utilizado", "Versão AI 3.1 (mock)", help="Acurácia estimada: 92% (mock)", border=True)
    with cols[2]:
        st.metric("Data/Hora Processamento", "04/05/2025 22:00 (mock)", help="Tempo total: 8 horas (mock)", border=True)
    st.markdown("---")

def render_brand_distribution_chart(brand_counts):
    st.subheader("📊 Distribuição de Marcas (Global nas Provas Selecionadas)")
    col1, col2 = st.columns([3,1])
    with col1:
        if not brand_counts.empty:
            st.bar_chart(brand_counts)
        else:
            st.caption("Nenhuma marca detectada para gerar o gráfico.")
    with col2:
        st.metric(label="Nível de Confiança", value="98% (mock)", help="Nível de confiança e margem de erro calculada (mock)", border=True)
        st.metric(label="Margem de Erro", value="± 2% (mock)", help="Nível de confiança e margem de erro calculada (mock)", border=True)

def render_segmentation_chart(data_dist, title, demographic_col_name):
    st.subheader(title)
    if not data_dist.empty:
        # Reshape for st.bar_chart: index=brand, columns=demographic_value
        # st.bar_chart expects data where columns are series.
        # If data_dist is already like:
        # shoe_brand    Gender1 Gender2
        # Nike          10      15
        # Adidas        8       12
        # then it's fine. If it's multi-indexed, it might need reshaping.
        # The current `groupby().size().unstack()` should produce this.
        st.bar_chart(data_dist)
    else:
        st.caption(f"Não há dados suficientes de {demographic_col_name} e marca para este gráfico.")

def render_marathon_comparison_chart(brand_counts_by_marathon):
    st.subheader("🏁 Marcas por Prova/Pasta (Comparativo)")
    if not brand_counts_by_marathon.empty:
        # Transpose for better readability if marathons are few and brands many
        # Or keep as is if brands are few and marathons many
        st.bar_chart(brand_counts_by_marathon.T, height=max(400, len(brand_counts_by_marathon.columns) * 30))
    else:
         st.caption("Não há dados de marcas por prova/pasta para este gráfico.")

def render_top_brands_table(top_brands_df):
    st.subheader("👟 Top Marcas de Tênis (Agregado nas Provas Selecionadas)")
    if not top_brands_df.empty:
        st.dataframe(top_brands_df, use_container_width=True, hide_index=True)
    else:
        st.caption("Nenhuma marca detectada para exibir o top.")


# --- Report Page Content (Main function to call others) ---
def report_page_content_main(processed_metrics, marathon_specific_data_for_cards): # marathon_specific_data_for_cards is now redundant if it's inside processed_metrics
    """
    Main function to render all sections of the report.
    processed_metrics: Dictionary of calculated metrics.
    """
    if processed_metrics.get("total_images_selected", 0) == 0: # Check if key exists
        st.warning("Nenhuma imagem nos dados selecionados para gerar o relatório.")
        return

    # Get the list of selected marathon names from the keys of marathon_specific_data_for_cards
    selected_marathon_names = list(processed_metrics.get("marathon_specific_data_for_cards", {}).keys())
    
    # Fetch full marathon metadata for cards (this might be cached upstream in the page)
    # For simplicity, assuming MARATHON_OPTIONS_DB_CACHED is available in session_state if needed,
    # or better, pass it down. For now, we'll use what's in processed_metrics.
    
    render_marathon_info_cards(
        selected_marathon_names, 
        processed_metrics.get("marathon_specific_data_for_cards", {}),
        st.session_state.get("MARATHON_OPTIONS_DB_CACHED", []) # Pass the cached list of marathon metadata
    )
    st.markdown("---")
    render_executive_summary(processed_metrics)
    render_processing_stats(processed_metrics)
    render_brand_distribution_chart(processed_metrics["brand_counts_all_selected"])
    
    st.markdown("---")
    render_segmentation_chart(processed_metrics["gender_brand_distribution"], 
                              "🚻 Segmentação por Gênero (Marcas)", "gênero")
    
    st.markdown("---")
    render_segmentation_chart(processed_metrics["race_brand_distribution"], 
                              "🧑🏾‍🤝‍🧑🏼 Segmentação por Raça/Etnia (Marcas)", "raça/etnia")

    st.markdown("---")
    render_marathon_comparison_chart(processed_metrics["brand_counts_by_marathon"])

    st.markdown("---")
    render_top_brands_table(processed_metrics["top_brands_all_selected"])


def render_pdf_preview_modal(processed_metrics, marathon_specific_data_for_cards):
    with st.dialog("Preview Relatório (Simulado)", width='large'):
        st.caption(f"Total de páginas: X (estimado)")
        with st.container(height=600):
             report_page_content_main(processed_metrics, marathon_specific_data_for_cards)
        st.markdown("---")
        btn_cols = st.columns([1,5,1])
        with btn_cols[0]:
            if st.button("Exportar Relatório (PDF - Mock)", type="primary", use_container_width=True, key="pdf_export_confirm_modal_db"):
                st.toast("Funcionalidade de exportação PDF não implementada.")
                st.session_state.show_pdf_preview_db = False
                st.rerun()
        with btn_cols[2]:
            if st.button("Fechar Preview", use_container_width=True, key="pdf_close_preview_modal_db"):
                st.session_state.show_pdf_preview_db = False
                st.rerun()