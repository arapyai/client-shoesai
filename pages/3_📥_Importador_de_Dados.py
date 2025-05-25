import streamlit as st
import json
import pandas as pd
from database import add_marathon_metadata, insert_parsed_json_data, get_db_connection # Ensure this path is correct

st.set_page_config(layout="wide", page_title="CourtShoes AI - Importador")

# --- Authentication Check ---
if not st.session_state.get("logged_in", False):
    st.warning("Por favor, faça login para acessar esta página.")
    st.link_button("Ir para Login", "/")
    st.stop()

if "user_info" not in st.session_state or not st.session_state.user_info.get("user_id"):
    st.error("Informações do usuário não encontradas. Por favor, faça login novamente.")
    st.link_button("Ir para Login", "/")
    st.stop()

user_id = st.session_state.user_info["user_id"]

st.title("📥 Importador de Dados de Provas")
st.markdown("Faça o upload de um arquivo JSON contendo os dados da prova e preencha os metadados.")

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
                marathon_id = add_marathon_metadata(
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
                    insert_parsed_json_data(marathon_id, image_data_list_for_db) # This function now handles batching internally
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

# Add a section to view existing marathons
st.sidebar.markdown("---")
st.sidebar.subheader("Provas Existentes")
conn_view = get_db_connection()
existing_marathons_df = pd.read_sql_query("SELECT marathon_id, name, event_date, location FROM Marathons ORDER BY upload_timestamp DESC", conn_view)
conn_view.close()
if not existing_marathons_df.empty:
    st.sidebar.dataframe(existing_marathons_df, hide_index=True)
else:
    st.sidebar.caption("Nenhuma prova importada ainda.")