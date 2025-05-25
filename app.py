# courtshoes_app.py
import streamlit as st
from database import verify_user, create_tables # Ensure create_tables is callable if run directly

# Page config should be the first Streamlit command
st.set_page_config(page_title="CourtShoes AI Login", initial_sidebar_state="collapsed")

def login_page():
    st.markdown("""
        <style>
            .login-container { max-width: 900px; margin: auto; padding-top: 5vh; }
            .login-form-column { padding: 30px; background-color: #f8f9fa; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }
            .logo-text { font-size: 36px; font-weight: bold; letter-spacing: 5px; text-align: left; margin-bottom: 30px; color: #333; }
            .stButton>button { width: 100%; background-color: #555; color: white; border-radius: 5px; }
            .stTextInput>div>div>input { border-radius: 5px; }
        </style>
    """, unsafe_allow_html=True)

    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    if 'user_info' not in st.session_state:
        st.session_state.user_info = None


    if st.session_state.logged_in and st.session_state.user_info:
        st.switch_page("pages/1_📊_Relatório.py")
        # st.stop() # No need for st.stop() after st.switch_page

    st.markdown("<div class='login-container'>", unsafe_allow_html=True)
    main_cols = st.columns([0.8, 1.2], gap="large")

    with main_cols[0]:
        st.image("https://storage.googleapis.com/proudcity/mebanenc/uploads/2021/03/placeholder-image.png", 
                 caption="CourtShoes AI", use_container_width=True)

    with main_cols[1]:
        st.markdown("<div class='login-form-column'>", unsafe_allow_html=True)
        st.markdown("<div class='logo-text'>T<span style='visibility:hidden;'>A</span>L<span style='visibility:hidden;'>L</span>K</div>", unsafe_allow_html=True)
        st.subheader("Bem vindo")
        st.caption("Para ter acesso ao painel, você precisa entrar com suas credenciais")

        with st.form("login_form_main_page"):
            email = st.text_input("Email", placeholder="Digite seu email", key="login_email_main_form")
            password = st.text_input("Senha", type="password", placeholder="Digite sua senha", key="login_password_main_form")
            login_button = st.form_submit_button("Entrar")

            if login_button:
                if not email or not password:
                    st.error("Por favor, preencha email e senha.")
                else:
                    user = verify_user(email, password)
                    if user:
                        st.session_state.logged_in = True
                        st.session_state.user_info = user # STORE USER INFO HERE
                        st.success("Login bem-sucedido!")
                        st.switch_page("pages/1_📊_Relatório.py")
                        # st.rerun() # st.switch_page handles the rerun
                    else:
                        st.error("Email ou senha inválidos.")
        
        st.markdown("<a href='#' style='text-decoration: none; color: #007bff; display: block; text-align: center; margin-top: 10px;'>Esqueceu a senha?</a>", unsafe_allow_html=True)
        st.markdown("<hr style='margin: 20px 0;'>", unsafe_allow_html=True)
        st.caption("© Copyright 2024 Talk Digital. Todos os direitos reservados.")
        st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    create_tables() # Ensure tables exist when running the app for the first time or directly
    login_page()