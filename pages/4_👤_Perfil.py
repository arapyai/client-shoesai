import streamlit as st
import re
from ui_components import page_header_with_logout
from database_abstraction import db, hasher
from sqlalchemy import text
from ui_components import check_auth
# --- Page Config ---
st.set_page_config(layout="wide", page_title="Shoes AI - Perfil do Usuário")

user_id = check_auth(admin_only=True)

# Display page header with logout button
page_header_with_logout("👤 Perfil do Usuário", 
                        "Gerencie suas informações de conta", 
                        key_suffix="profile")

# Function to update user email with validation
def update_user_email_with_validation(user_id, new_email):
    if not is_valid_email(new_email):
        return False, "Email inválido. Por favor, forneça um email válido."
    
    with db.get_connection() as conn:
        try:
            # Check if email already exists - update table name to use new schema
            result = conn.execute(text("SELECT user_id FROM users WHERE email = :email AND user_id != :user_id"), 
                                {"email": new_email, "user_id": user_id}).fetchone()
            if result:
                return False, "Este email já está em uso por outra conta."
            
            if db.update_user_email(user_id, new_email):
                return True, "Email atualizado com sucesso!"
            else:
                return False, "Erro ao atualizar email. Por favor, tente novamente."
        except Exception as e:
            return False, f"Erro ao atualizar email: {e}"

# Function to update user password with validation
def update_user_password_with_validation(user_id, current_password, new_password):
    if len(new_password) < 6:
        return False, "A nova senha deve ter pelo menos 6 caracteres."
    
    with db.get_connection() as conn:
        try:
            # Verify current password - update table name to use new schema
            result = conn.execute(text("SELECT hashed_password FROM users WHERE user_id = :user_id"), 
                                {"user_id": user_id}).fetchone()
            if not result or not hasher.verify(current_password, result.hashed_password):
                return False, "Senha atual incorreta."
            
            # Use database function to update password
            if db.update_user_password(user_id, new_password):
                return True, "Senha atualizada com sucesso!"
            else:
                return False, "Erro ao atualizar senha. Por favor, tente novamente."
        except Exception as e:
            return False, f"Erro ao atualizar senha: {e}"

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
with db.get_connection() as conn:
    result = conn.execute(text("""
        SELECT name, event_date, location, upload_timestamp 
        FROM marathons 
        WHERE uploaded_by_user_id = :user_id 
        ORDER BY upload_timestamp DESC
    """), {"user_id": user_id}).fetchall()
    user_marathons = [dict(row._mapping) for row in result]

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

# Admin section - only visible to admins
if is_admin:
    st.markdown("---")
    st.subheader("🛠️ Administração de Usuários")
    st.caption("Esta seção é visível apenas para administradores.")
    
    # Create tabs for different admin functions
    tab_list, tab_add, tab_manage = st.tabs(["👥 Lista de Usuários", "➕ Adicionar Usuário", "⚙️ Gerenciar Usuários"])
    
    with tab_list:
        st.write("**Usuários cadastrados no sistema:**")
        
        # Get all users
        all_users = db.get_all_users()
        
        if all_users:
            for user in all_users:
                with st.container(border=True):
                    col_info, col_role, col_actions = st.columns([2, 1, 1])
                    
                    with col_info:
                        st.write(f"**{user['email']}**")
                        st.caption(f"ID: {user['user_id']}")
                    
                    with col_role:
                        role_text = "🔧 Admin" if user['is_admin'] else "👤 Usuário"
                        st.write(role_text)
                    
                    with col_actions:
                        if user['user_id'] != user_id:  # Don't allow deleting self
                            if st.button(f"🗑️", key=f"delete_user_{user['user_id']}", 
                                       help="Excluir usuário"):
                                if f"confirm_delete_user_{user['user_id']}" not in st.session_state:
                                    st.session_state[f"confirm_delete_user_{user['user_id']}"] = True
                                    st.rerun()
        else:
            st.info("Nenhum usuário encontrado.")
    
    with tab_add:
        st.write("**Adicionar novo usuário:**")
        
        with st.form("add_user_form"):
            new_user_email = st.text_input("Email do novo usuário")
            new_user_password = st.text_input("Senha temporária", type="password")
            new_user_is_admin = st.checkbox("Tornar administrador")
            submit_add_user = st.form_submit_button("Adicionar Usuário")
            
            if submit_add_user:
                if not new_user_email or not new_user_password:
                    st.error("Por favor, preencha todos os campos.")
                elif not is_valid_email(new_user_email):
                    st.error("Email inválido.")
                elif len(new_user_password) < 6:
                    st.error("A senha deve ter pelo menos 6 caracteres.")
                else:
                    if db.add_user(new_user_email, new_user_password, new_user_is_admin):
                        role_text = "administrador" if new_user_is_admin else "usuário"
                        st.success(f"Usuário {role_text} '{new_user_email}' adicionado com sucesso!")
                        st.rerun()
                    else:
                        st.error("Erro ao adicionar usuário. O email pode já estar em uso.")
    
    with tab_manage:
        st.write("**Gerenciar usuários existentes:**")
        
        all_users = db.get_all_users()
        
        if all_users:
            # Filter out current user from management
            other_users = [user for user in all_users if user['user_id'] != user_id]
            
            if other_users:
                selected_user_emails = [f"{user['email']} (ID: {user['user_id']})" for user in other_users]
                selected_user_display = st.selectbox("Selecionar usuário para gerenciar:", 
                                                    ["Selecione um usuário..."] + selected_user_emails)
                
                if selected_user_display != "Selecione um usuário...":
                    # Extract user_id from selection
                    selected_user_id = int(selected_user_display.split("ID: ")[1].split(")")[0])
                    selected_user = next(user for user in other_users if user['user_id'] == selected_user_id)
                    
                    st.write(f"**Gerenciando:** {selected_user['email']}")
                    
                    col_role_mgmt, col_delete_mgmt = st.columns([1, 1])
                    
                    with col_role_mgmt:
                        st.write("**Alterar função:**")
                        current_role = "Administrador" if selected_user['is_admin'] else "Usuário"
                        st.info(f"Função atual: {current_role}")
                        
                        new_role = st.selectbox("Nova função:", 
                                               ["Usuário", "Administrador"],
                                               index=1 if selected_user['is_admin'] else 0)
                        
                        if st.button("Atualizar Função", key=f"update_role_{selected_user_id}"):
                            new_is_admin = (new_role == "Administrador")
                            if new_is_admin != selected_user['is_admin']:
                                if db.update_user_role(selected_user_id, new_is_admin):
                                    st.success(f"Função atualizada para {new_role}!")
                                    st.rerun()
                                else:
                                    st.error("Erro ao atualizar função.")
                            else:
                                st.info("A função selecionada é igual à atual.")
                    
                    with col_delete_mgmt:
                        st.write("**Excluir usuário:**")
                        st.warning("⚠️ Esta ação é irreversível!")
                        
                        if st.button("🗑️ Excluir Usuário", key=f"delete_mgmt_{selected_user_id}"):
                            st.session_state[f"confirm_delete_user_{selected_user_id}"] = True
                            st.rerun()
            else:
                st.info("Não há outros usuários para gerenciar.")
        else:
            st.info("Nenhum usuário encontrado.")
    
    # Handle deletion confirmations
    for user_key in list(st.session_state.keys()):
        if isinstance(user_key, str) and user_key.startswith("confirm_delete_user_"):
            delete_user_id = int(user_key.replace("confirm_delete_user_", ""))
            
            # Find user email for confirmation
            all_users = db.get_all_users()
            delete_user_email = next((u['email'] for u in all_users if u['user_id'] == delete_user_id), "Usuário desconhecido")
            
            st.error(f"⚠️ **Confirmação necessária**: Tem certeza que deseja excluir o usuário '{delete_user_email}'?")
            
            col_confirm, col_cancel = st.columns([1, 1])
            
            with col_confirm:
                if st.button("✅ Confirmar Exclusão", key=f"confirm_yes_user_{delete_user_id}"):
                    if db.delete_user(delete_user_id):
                        st.success(f"Usuário '{delete_user_email}' excluído com sucesso!")
                        # Clear confirmation state
                        del st.session_state[user_key]
                        st.rerun()
                    else:
                        st.error("Erro ao excluir usuário.")
            
            with col_cancel:
                if st.button("❌ Cancelar", key=f"cancel_delete_user_{delete_user_id}"):
                    del st.session_state[user_key]
                    st.rerun()
