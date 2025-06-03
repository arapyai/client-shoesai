import streamlit as st

st.set_page_config(layout="wide", page_title="CourtShoes AI - Sobre")

if not st.session_state.get("logged_in", False):
    st.warning("Por favor, faça login para acessar esta página.")
    st.link_button("Ir para Login", "/")
    st.stop()

# Import the page header component
from ui_components import page_header_with_logout

# Display page header with logout button
page_header_with_logout("ℹ️ Sobre a plataforma CourtShoes AI", key_suffix="about")
st.markdown("""
Bem-vindo à plataforma de análise de maratonas da CourtShoes AI!

**Nossa Missão:**
Fornecer insights detalhados e acionáveis sobre o uso de calçados em eventos de corrida, ajudando marcas, atletas e organizadores a tomar decisões mais informadas.

**O que fazemos:**
Utilizamos inteligência artificial para processar imagens de maratonas, identificando:
- Marcas e modelos de tênis mais utilizados.
- Tendências de uso por gênero e etnia.
- Distribuição de marcas ao longo do percurso.
- E muito mais!

**Tecnologia:**
Nossa plataforma é construída com as mais recentes tecnologias de visão computacional e machine learning, garantindo alta precisão e relatórios abrangentes.
Os dados apresentados neste demo são carregados a partir de arquivos JSON de exemplo.

**Contato:**
Para mais informações, entre em contato conosco em: contato@courtshoes.ai
""")

st.markdown("---")
st.subheader("Arquivos de Dados Utilizados (Exemplo):")
# This is just for demo, you might not want to expose file paths in a real app
data_files_info = st.session_state.get("MARATHON_DATA_FILES", 
                                       {"Prova Exemplo": "prova1505-dataset.json"}) # Fallback
for name, path in data_files_info.items():
    st.write(f"- **{name}**: `{path}`")