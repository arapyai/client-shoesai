import streamlit as st
from ui_components import check_auth

st.set_page_config(layout="wide", page_title="Shoes AI - Sobre")

user_id = check_auth()

# Import the page header component
from ui_components import page_header_with_logout

# Display page header with logout button
page_header_with_logout("ℹ️ Sobre a plataforma Shoes AI", key_suffix="about")
st.markdown("""
Bem-vindo à plataforma de análise de maratonas da Shoes AI!

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
Para mais informações, entre em contato conosco em: contato@talkinc.com.br
""")