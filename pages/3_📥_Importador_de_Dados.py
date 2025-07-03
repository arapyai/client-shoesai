import streamlit as st
import json
import pandas as pd
from database_abstraction import db
from ui_components import check_auth
from csv_processing import load_and_validate_csv, generate_statistics, create_summary_dataframe
st.set_page_config(layout="wide", page_title="Shoes AI - Importador")


user_id = check_auth(admin_only=True)

# Import the page header component
from ui_components import page_header_with_logout

# Display page header with logout button
page_header_with_logout("📥 Importador de Dados de Provas", 
                      "Faça o upload de um arquivo CSV contendo os dados da prova e preencha os metadados.",
                      key_suffix="importer")

with st.form("marathon_import_form", clear_on_submit=False): # Keep values on submit for now
    st.subheader("Metadados da Prova")
    marathon_name = st.text_input("Nome da Prova/Evento*", help="Nome único para identificar esta prova no sistema.")
    event_date_input = st.date_input("Data do Evento", value=None, key="event_date_importer")
    location = st.text_input("Localização (Cidade, Estado)", placeholder="Ex: São Paulo, SP")
    distance_km_input = st.number_input("Distância (km)", min_value=0.0, value=0.0, step=0.1, format="%.1f", key="distance_importer")
    description = st.text_area("Descrição Adicional (Opcional)")

    st.subheader("Arquivo de Dados CSV")
    st.info("📋 **Formato esperado do CSV:**\n"
           "- `bib`: número do peito\n"
           "- `position`: posição na categoria (? para não posicionado)\n"
           "- `gender`: MASCULINO/FEMININO\n"
           "- `run_category`: categoria da prova (5K, 10K, 21K, 42K, etc.)\n"
           "- `shoe_brand`: marca do tênis\n"
           "- `confidence`: nível de confiança da detecção")
    
    uploaded_file = st.file_uploader("Escolha um arquivo CSV com os dados da prova*", type=["csv"], key="csv_uploader")

    submitted_import = st.form_submit_button("Importar Dados da Prova")
        
    if submitted_import:
        if not marathon_name:
            st.error("O nome da prova é obrigatório.")
        elif not uploaded_file:
            st.error("Por favor, faça o upload de um arquivo CSV.")
        else:
            progress_bar = st.progress(0, text="Iniciando importação...")
            try:
                progress_bar.progress(10, text="Validando e processando arquivo CSV...")
                
                # Carrega e valida o CSV
                df_race_data = load_and_validate_csv(uploaded_file)
                
                progress_bar.progress(30, text="Gerando estatísticas...")
                
                # Gera estatísticas diretamente dos dados
                race_statistics = generate_statistics(df_race_data, marathon_name)
                
                progress_bar.progress(50, text="Salvando metadados da prova...")
                
                # Salva apenas os metadados da prova no banco
                marathon_id = db.add_marathon_metadata(
                    name=marathon_name,
                    event_date=str(event_date_input) if event_date_input else None,
                    location=location,
                    distance_km=float(distance_km_input) if distance_km_input > 0 else None,
                    description=description,
                    user_id=user_id
                )

                if marathon_id:
                    progress_bar.progress(60, text="Salvando dados dos corredores...")
                    
                    # Prepara os dados dos corredores para inserção em lote
                    runners_data = []
                    for _, row in df_race_data.iterrows():
                        runner_data = {
                            'bib': row.get('bib'),
                            'position': row.get('position') if row.get('position') != '?' else None,
                            'shoe_brand': row.get('shoe_brand'),
                            'shoe_model': row.get('shoe_model'),
                            'gender': row.get('gender'),
                            'run_category': row.get('run_category'),
                            'confidence': row.get('confidence')
                        }
                        runners_data.append(runner_data)
                    
                    # Insere os dados dos corredores em lote
                    successful_runners, failed_runners = db.add_marathon_runners_bulk(marathon_id, runners_data)
                    
                    progress_bar.progress(80, text="Salvando estatísticas calculadas...")
                    
                    # Salva as estatísticas pré-calculadas no banco
                    stats_saved = db.save_race_statistics(marathon_id, race_statistics)
                    
                    if stats_saved:
                        progress_bar.progress(100, text="Importação concluída!")
                        
                        # Exibe resumo da importação
                        st.success(f"✅ Prova '{marathon_name}' importada com sucesso! ID da Prova: {marathon_id}")
                        
                        # Mostra informações sobre a importação dos corredores
                        if failed_runners > 0:
                            st.warning(f"⚠️ {successful_runners} corredores importados com sucesso, {failed_runners} falharam.")
                        else:
                            st.info(f"📊 {successful_runners} corredores importados com sucesso!")
                        
                        # Mostra estatísticas resumidas
                        with st.expander("📊 Resumo das Estatísticas Importadas", expanded=True):
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                st.metric("Total de Participantes", race_statistics['total_participants'])
                            with col2:
                                st.metric("Total de Marcas", race_statistics['total_brands'])
                            with col3:
                                st.metric("Marca Líder", race_statistics['leader_brand']['name'])
                            with col4:
                                st.metric("Confiança Média", f"{race_statistics['avg_confidence']:.2f}")
                            
                            # Mostra preview dos dados
                            st.subheader("Preview dos Dados")
                            st.dataframe(df_race_data.head(10), use_container_width=True)
                            
                            # Top 5 marcas
                            st.subheader("Top 5 Marcas")
                            top_brands_data = []
                            for i, (brand, count) in enumerate(list(race_statistics['top_brands'].items())[:5], 1):
                                percentage = round((count / race_statistics['total_participants']) * 100, 2)
                                top_brands_data.append({
                                    'Posição': i,
                                    'Marca': brand,
                                    'Participantes': count,
                                    'Percentual': f"{percentage}%"
                                })
                            st.dataframe(pd.DataFrame(top_brands_data), use_container_width=True, hide_index=True)
                        
                        # Clear relevant session states to force reload on report page
                        for key_to_clear in ['df_all_marathons_raw', 'df_flat_detections', 'processed_report_data', 
                                             'selected_marathon_names_ui', 'MARATHON_OPTIONS_DB_CACHED']:
                            if key_to_clear in st.session_state:
                                del st.session_state[key_to_clear]
                        st.cache_data.clear()
                    else:
                        st.error("Erro ao salvar estatísticas no banco de dados.")
                        progress_bar.empty()
                else:
                    st.error(f"Falha ao adicionar metadados da prova. A prova '{marathon_name}' já pode existir ou ocorreu um erro no banco de dados.")
                    progress_bar.empty()

            except ValueError as ve:
                st.error(f"Erro de validação: {ve}")
                progress_bar.empty()
            except Exception as e:
                st.error(f"Ocorreu um erro durante a importação: {e}")
                import traceback
                st.error(traceback.format_exc())
                progress_bar.empty()

# --- Provas Existentes Section ---
st.markdown("---")
st.subheader("🗂️ Provas Existentes no Sistema")
st.caption("Gerencie as provas já importadas no sistema")

# Get existing marathons from database
from database_abstraction import db
with db.get_connection() as conn_view:
    existing_marathons_df = pd.read_sql_query("""
        SELECT marathon_id, name, event_date, location, upload_timestamp 
        FROM marathons 
        ORDER BY upload_timestamp DESC
    """, conn_view)

if not existing_marathons_df.empty:
    # Display marathons in an organized way with delete buttons
    for index, marathon in existing_marathons_df.iterrows():
        with st.container(border=True):
            col_info, col_remove = st.columns([5, 1])
            
            with col_info:
                st.write(f"**{marathon['name']}**")
                
                # Format the information in a nice way
                details = []
                if marathon['event_date']:
                    details.append(f"🗓️ {marathon['event_date']}")
                if marathon['location']:
                    details.append(f"📍 {marathon['location']}")
                if marathon['upload_timestamp']:
                    upload_date = str(marathon['upload_timestamp']).split()[0]
                    details.append(f"📥 Importado em: {upload_date}")
                
                # Get summary metrics from stored statistics
                try:
                    metrics = db.get_precomputed_marathon_metrics([marathon['marathon_id']])
                    if metrics:
                        details.append(f"📊 Participantes: {metrics.get('total_shoes_detected', 0)} | "
                                       f"Marcas: {metrics.get('unique_brands_count', 0)} | "
                                       f"Marca Líder: {metrics.get('leader_brand_name', 'N/A')}")
                    else:
                        details.append("📊 Métricas não disponíveis.")
                except:
                    details.append("📊 Erro ao carregar métricas.")
                
                if details:
                    st.caption(" | ".join(details))
            
            with col_remove:
                # Add delete button for each marathon
                if st.button(
                    "🗑️ Excluir", 
                    key=f"delete_marathon_{marathon['marathon_id']}", 
                    help="Excluir esta prova e todos os dados associados",
                    type="secondary",
                    use_container_width=True
                ):
                    # Show confirmation dialog
                    if f"confirm_delete_{marathon['marathon_id']}" not in st.session_state:
                        st.session_state[f"confirm_delete_{marathon['marathon_id']}"] = True
                        st.rerun()
            
            # Handle deletion confirmation
            if st.session_state.get(f"confirm_delete_{marathon['marathon_id']}", False):
                st.warning(f"⚠️ Tem certeza que deseja excluir a prova **{marathon['name']}**? Esta ação não pode ser desfeita!")
                
                col_confirm, col_cancel = st.columns(2)
                with col_confirm:
                    if st.button(
                        "✅ Sim, excluir", 
                        key=f"confirm_yes_{marathon['marathon_id']}", 
                        type="primary",
                        use_container_width=True
                    ):
                        # Perform deletion
                        if db.delete_marathon_by_id(marathon['marathon_id']):
                            st.success(f"Prova '{marathon['name']}' foi excluída com sucesso!")
                            
                            # Clear session states to force reload
                            for key_to_clear in ['df_all_marathons_raw', 'df_flat_detections', 'processed_report_data', 
                                               'selected_marathon_names_ui', 'MARATHON_OPTIONS_DB_CACHED']:
                                if key_to_clear in st.session_state:
                                    del st.session_state[key_to_clear]
                            st.cache_data.clear()
                            
                            # Clear confirmation state
                            if f"confirm_delete_{marathon['marathon_id']}" in st.session_state:
                                del st.session_state[f"confirm_delete_{marathon['marathon_id']}"]
                            
                            st.rerun()
                        else:
                            st.error("Erro ao excluir a prova. Tente novamente.")
                
                with col_cancel:
                    if st.button(
                        "❌ Cancelar", 
                        key=f"confirm_no_{marathon['marathon_id']}",
                        use_container_width=True
                    ):
                        # Cancel deletion
                        if f"confirm_delete_{marathon['marathon_id']}" in st.session_state:
                            del st.session_state[f"confirm_delete_{marathon['marathon_id']}"]
                        st.rerun()
else:
    st.info("📋 Nenhuma prova importada ainda. Use o formulário acima para importar sua primeira prova!")