import streamlit as st
import re
from ui_components import page_header_with_logout
from database import get_db_connection, hasher, update_user_email as db_update_email, update_user_password as db_update_password

# --- Page Config ---
st.set_page_config(layout="wide", page_title="CourtShoes AI - Perfil do Usuário")

# --- Authentication Check ---
if not st.session_state.get("logged_in", False):
    st.warning("Por favor, faça login para acessar esta página.")
    st.link_button("Ir para Login", "/")
    st.stop()

if "user_info" not in st.session_state or not st.session_state.user_info.get("user_id"):
    st.error("Informações do usuário não encontradas. Por favor, faça login novamente.")
    st.link_button("Ir para Login", "/")
    st.stop()

# Display page header with logout button
page_header_with_logout("👤 Perfil do Usuário", 
                        "Gerencie suas informações de conta", 
                        key_suffix="profile")

# Function to update user email with validation
def update_user_email_with_validation(user_id, new_email):
    if not is_valid_email(new_email):
        return False, "Email inválido. Por favor, forneça um email válido."
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Check if email already exists
        cursor.execute("SELECT user_id FROM Users WHERE email = ? AND user_id != ?", (new_email, user_id))
        if cursor.fetchone():
            return False, "Este email já está em uso por outra conta."
        
        if db_update_email(user_id, new_email):
            return True, "Email atualizado com sucesso!"
        else:
            return False, "Erro ao atualizar email. Por favor, tente novamente."
    except Exception as e:
        return False, f"Erro ao atualizar email: {e}"
    finally:
        conn.close()

# Function to update user password with validation
def update_user_password_with_validation(user_id, current_password, new_password):
    if len(new_password) < 6:
        return False, "A nova senha deve ter pelo menos 6 caracteres."
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Verify current password
        cursor.execute("SELECT hashed_password FROM Users WHERE user_id = ?", (user_id,))
        user_record = cursor.fetchone()
        if not user_record or not hasher.verify(current_password, user_record['hashed_password']):
            return False, "Senha atual incorreta."
        
        # Use database function to update password
        if db_update_password(user_id, new_password):
            return True, "Senha atualizada com sucesso!"
        else:
            return False, "Erro ao atualizar senha. Por favor, tente novamente."
    except Exception as e:
        return False, f"Erro ao atualizar senha: {e}"
    finally:
        conn.close()

# Email validation function
def is_valid_email(email):
    pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    return bool(re.match(pattern, email))

# Get current user info
user_id = st.session_state.user_info["user_id"]
user_email = st.session_state.user_info["email"]
is_admin = st.session_state.user_info.get("is_admin", False)

# Create two columns for profile sections
col1, col2 = st.columns([1, 1], gap="large")

with col1:
    st.subheader("Suas Informações")
    st.info(f"**Email:** {user_email}")
    st.info(f"**Tipo de Conta:** {'Administrador' if is_admin else 'Usuário Padrão'}")
    
    # Email update form
    with st.expander("Atualizar Email", expanded=False):
        with st.form("update_email_form"):
            new_email = st.text_input("Novo Email", value=user_email)
            submit_email = st.form_submit_button("Atualizar Email")
            
            if submit_email:
                if new_email == user_email:
                    st.info("O novo email é igual ao atual.")
                else:
                    success, message = update_user_email_with_validation(user_id, new_email)
                    if success:
                        st.success(message)
                        # Update session state
                        st.session_state.user_info["email"] = new_email
                        # Rerun to show updated info
                        st.rerun()
                    else:
                        st.error(message)

with col2:
    st.subheader("Segurança")
    
    # Password change form
    with st.form("change_password_form"):
        st.write("Alterar Senha")
        current_password = st.text_input("Senha Atual", type="password")
        new_password = st.text_input("Nova Senha", type="password")
        confirm_password = st.text_input("Confirmar Nova Senha", type="password")
        submit_password = st.form_submit_button("Alterar Senha")
        
        if submit_password:
            if not current_password or not new_password or not confirm_password:
                st.error("Por favor, preencha todos os campos.")
            elif new_password != confirm_password:
                st.error("A nova senha e a confirmação não correspondem.")
            else:
                success, message = update_user_password_with_validation(user_id, current_password, new_password)
                if success:
                    st.success(message)
                else:
                    st.error(message)

# Display activity information
st.subheader("Atividade da Conta")

# Get marathons uploaded by this user
conn = get_db_connection()
cursor = conn.cursor()
cursor.execute("""
    SELECT name, event_date, location, upload_timestamp 
    FROM Marathons 
    WHERE uploaded_by_user_id = ? 
    ORDER BY upload_timestamp DESC
""", (user_id,))
user_marathons = cursor.fetchall()
conn.close()

if user_marathons:
    st.write(f"Você importou {len(user_marathons)} provas:")
    
    for marathon in user_marathons:
        with st.container(border=True):
            col_info, col_date = st.columns([3, 1])
            with col_info:
                st.write(f"**{marathon['name']}**")
                if marathon['location']:
                    st.caption(f"📍 {marathon['location']}")
            with col_date:
                upload_date = str(marathon['upload_timestamp']).split()[0] if marathon['upload_timestamp'] else "Data desconhecida"
                st.caption(f"Importado em: {upload_date}")
else:
    st.info("Você ainda não importou nenhuma prova.")

# Footer with system information
st.markdown("---")
st.caption("CourtShoes AI - Sistema de Análise de Provas v1.0")
