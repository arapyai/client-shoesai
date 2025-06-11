import streamlit as st
import json
import pandas as pd
from database_abstraction import db

st.set_page_config(layout="wide", page_title="Shoes AI - Importador")

# --- Authentication Check ---
if not st.session_state.get("logged_in", False):
    st.warning("Por favor, faça login para acessar esta página.")
    st.link_button("Ir para Login", "/")
    st.stop()

#Only if user is admin in user_admin["is_admin"] field
if not st.session_state.get("user_info", {}).get("is_admin", False):
    st.error("Acesso negado. Esta página é restrita a administradores.")
    st.link_button("Ir para Relatório", "/pages/1_%F0%9F%93%A5_Relat%C3%B3rio.py")
    st.stop()

if "user_info" not in st.session_state or not st.session_state.user_info.get("user_id"):
    st.error("Informações do usuário não encontradas. Por favor, faça login novamente.")
    st.link_button("Ir para Login", "/")
    st.stop()

user_id = st.session_state.user_info["user_id"]

# Import the page header component
from ui_components import page_header_with_logout

# Display page header with logout button
page_header_with_logout("📥 Importador de Dados de Provas", 
                      "Faça o upload de um arquivo JSON contendo os dados da prova e preencha os metadados.",
                      key_suffix="importer")

with st.form("marathon_import_form", clear_on_submit=False): # Keep values on submit for now
    st.subheader("Metadados da Prova")
    marathon_name = st.text_input("Nome da Prova/Evento*", help="Nome único para identificar esta prova no sistema.")
    event_date_input = st.date_input("Data do Evento", value=None, key="event_date_importer")
    location = st.text_input("Localização (Cidade, Estado)", placeholder="Ex: São Paulo, SP")
    distance_km_input = st.number_input("Distância (km)", min_value=0.0, value=0.0, step=0.1, format="%.1f", key="distance_importer")
    description = st.text_area("Descrição Adicional (Opcional)")

    st.subheader("Arquivo de Dados JSON")
    uploaded_file = st.file_uploader("Escolha um arquivo JSON com os dados da prova*", type=["json"], key="json_uploader")

    submitted_import = st.form_submit_button("Importar Dados da Prova")

    if submitted_import:
        if not marathon_name:
            st.error("O nome da prova é obrigatório.")
        elif not uploaded_file:
            st.error("Por favor, faça o upload de um arquivo JSON.")
        else:
            progress_bar = st.progress(0, text="Iniciando importação...")
            try:
                # Forgiving JSON loading for files that might not be perfectly UTF-8
                try:
                    json_content = uploaded_file.read().decode('utf-8-sig') # Try utf-8-sig first for BOM
                except UnicodeDecodeError:
                    uploaded_file.seek(0) # Reset file pointer
                    json_content = uploaded_file.read().decode('latin-1') # Fallback

                json_data_raw = json.loads(json_content)
                
                # The JSON is a dict of dicts, needs to be list of dicts
                # Example: {'col1': {'0':'a', '1':'b'}, 'col2': {'0':1, '1':2}}
                # to [{'col1':'a', 'col2':1}, {'col1':'b', 'col2':2}]
                df_temp = pd.DataFrame(json_data_raw)
                image_data_list_for_db = df_temp.to_dict(orient='records')

                progress_bar.progress(10, text="Metadados da prova sendo salvos...")
                marathon_id = db.add_marathon_metadata(
                    name=marathon_name,
                    event_date=str(event_date_input) if event_date_input else None,
                    location=location,
                    distance_km=float(distance_km_input) if distance_km_input > 0 else None,
                    description=description,
                    original_json_filename=uploaded_file.name,
                    user_id=user_id
                )

                if marathon_id:
                    progress_bar.progress(30, text=f"Metadados salvos (ID: {marathon_id}). Processando imagens...")
                    db.insert_parsed_json_data(marathon_id, image_data_list_for_db) # This function now handles batching internally
                    progress_bar.progress(100, text="Importação concluída!")
                    st.success(f"Prova '{marathon_name}' e seus dados importados com sucesso! ID da Prova: {marathon_id}")
                    
                    # Clear relevant session states to force reload on report page
                    for key_to_clear in ['df_all_marathons_raw', 'df_flat_detections', 'processed_report_data', 
                                         'selected_marathon_names_ui', 'MARATHON_OPTIONS_DB_CACHED']:
                        if key_to_clear in st.session_state:
                            del st.session_state[key_to_clear]
                    # Also clear Streamlit's function caches if you have them on data loading functions
                    st.cache_data.clear() # Clears all @st.cache_data

                    # To reset form fields after successful submission:
                    # This is a bit hacky, but can work by forcing a rerun and clearing specific states
                    # st.session_state.marathon_import_form_submitted_once = True 
                    # This requires more complex state management to truly clear the form if clear_on_submit=False
                    # For now, the user can manually clear or just knows it's submitted.
                else:
                    st.error(f"Falha ao adicionar metadados da prova. A prova '{marathon_name}' já pode existir ou ocorreu um erro no banco de dados.")
                    progress_bar.empty()

            except json.JSONDecodeError:
                st.error("Arquivo JSON inválido. Por favor, verifique o formato do arquivo.")
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
            col_info, col_calculate, col_remove = st.columns([4, 1,1])
            
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
                
                #add summary of metrics get from database
                metrics = db.get_precomputed_marathon_metrics([marathon['marathon_id']])
                if metrics:
                    details.append(f"📊 Imagens: {metrics.get('total_images_selected', 0)} | "
                                   f"Calçados Detectados: {metrics.get('total_shoes_detected', 0)} | "
                                   f"Pessoas Analisadas: {metrics.get('persons_analyzed_count', 0)}")
                else:
                    details.append("📊 Métricas não calculadas ainda.")
                
                if details:
                    st.caption(" | ".join(details))
            
            with col_calculate:
                #recalcular as métrocas
                if st.button(
                    "🔄 Recalcular Métricas", 
                    key=f"recalculate_metrics_{marathon['marathon_id']}", 
                    help="Recalcular as métricas para esta prova",
                    type="primary",
                    use_container_width=True
                ):
                    # Trigger recalculation logic
                    try:
                        db.calculate_and_store_marathon_metrics(marathon['marathon_id'])
                        st.success(f"Métricas calculadas com sucesso para a prova '{marathon['name']}'!")
                    except Exception as e:
                        st.error(f"Erro ao recalcular métricas: {e}")
                        import traceback
                        st.error(traceback.format_exc())
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