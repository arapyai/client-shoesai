# manage_db.py
import argparse
import os
from database_abstraction import db
from sqlalchemy import text

def get_user_by_email(email: str):
    for user in db.get_all_users():
        if user['email'] == email:
            return user
    return None


def ensure_tables():
    db.create_tables()


def drop_all_tables(args):
    """Drop all tables and recreate the database structure."""
    if not args.confirm:
        print("⚠️  ATENÇÃO: Esta operação irá apagar TODOS os dados do banco!")
        print("Para confirmar, use: --confirm")
        return
    
    if not db or not db.engine:
        print("❌ Erro: Banco de dados não inicializado.")
        return
    
    try:
        # Get database type to handle differently
        db_url = str(db.engine.url)
        
        with db.get_connection() as conn:
            if db_url.startswith('postgresql'):
                # For PostgreSQL, use CASCADE to drop dependencies
                print("🐘 PostgreSQL detectado - usando CASCADE para remover dependências...")
                
                # Get all table names
                result = conn.execute(
                    text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
                )
                tables = [row[0] for row in result.fetchall()]
                
                # Drop each table with CASCADE
                for table in tables:
                    try:
                        conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
                        print(f"   ✅ Tabela {table} removida")
                    except Exception as e:
                        print(f"   ⚠️  Erro ao remover {table}: {e}")
                
                conn.commit()
                print("✅ Todas as tabelas foram removidas com CASCADE.")
            else:
                # For SQLite and others, use normal drop
                db.metadata.drop_all(db.engine)
                print("✅ Todas as tabelas foram removidas.")
            
            # Recreate tables
            db.metadata.create_all(db.engine)
            print("✅ Estrutura do banco recriada.")
            
        print("🗑️  Banco de dados limpo com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro ao limpar banco: {e}")


def reset_database(args):
    """Reset the entire database - drop tables and delete SQLite file if applicable."""
    if not args.confirm:
        print("⚠️  ATENÇÃO: Esta operação irá apagar COMPLETAMENTE o banco de dados!")
        print("Para confirmar, use: --confirm")
        return
    
    if not db or not db.engine:
        print("❌ Erro: Banco de dados não inicializado.")
        return
    
    try:
        # Get database type and path
        db_url = str(db.engine.url)
        
        if db_url.startswith('sqlite'):
            # For SQLite, delete the file
            db_path = db_url.replace('sqlite:///', '')
            
            # Close the connection first
            db.engine.dispose()
            
            # Remove SQLite files
            files_to_remove = [db_path, f"{db_path}-shm", f"{db_path}-wal", f"{db_path}-journal"]
            removed_files = []
            
            for file_path in files_to_remove:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    removed_files.append(file_path)
            
            if removed_files:
                print(f"🗑️  Arquivos removidos: {', '.join(removed_files)}")
            
            # Reinitialize the database
            db._initialize_engine()
            db.create_tables()
            
        else:
            # For PostgreSQL or other databases, drop all tables with CASCADE
            with db.get_connection() as conn:
                if db_url.startswith('postgresql'):
                    # For PostgreSQL, use CASCADE to drop dependencies
                    print("🐘 PostgreSQL detectado - usando CASCADE para remover dependências...")
                    
                    # Get all table names
                    result = conn.execute(
                        text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
                    )
                    tables = [row[0] for row in result.fetchall()]
                    
                    # Drop each table with CASCADE
                    for table in tables:
                        try:
                            conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
                            print(f"   ✅ Tabela {table} removida")
                        except Exception as e:
                            print(f"   ⚠️  Erro ao remover {table}: {e}")
                    
                    conn.commit()
                    print("✅ Todas as tabelas PostgreSQL removidas com CASCADE.")
                else:
                    # For other databases, use normal drop
                    db.metadata.drop_all(db.engine)
                
                # Recreate tables
                db.metadata.create_all(db.engine)
        
        print("✅ Banco de dados completamente resetado!")
        
    except Exception as e:
        print(f"❌ Erro ao resetar banco: {e}")


# ---- User Operations ----

def user_list(args):
    users = db.get_all_users()
    for u in users:
        role = 'Admin' if u['is_admin'] else 'User'
        print(f"{u['user_id']}: {u['email']} ({role})")


def user_add(args):
    if db.add_user(args.email, args.password, args.admin):
        role = 'Admin' if args.admin else 'User'
        print(f"{role} '{args.email}' added successfully.")
    else:
        print(f"Error: Could not add user '{args.email}'. Email might already exist.")


def user_delete(args):
    user_id = args.id
    if not user_id and args.email:
        user = get_user_by_email(args.email)
        if user:
            user_id = user['user_id']
    if not user_id:
        print('User not found.')
        return
    if db.delete_user(user_id):
        print(f"User ID {user_id} deleted successfully.")
    else:
        print('Error: Could not delete user.')


def user_update(args):
    user_id = args.id
    if not user_id and args.email:
        user = get_user_by_email(args.email)
        if user:
            user_id = user['user_id']
    if not user_id:
        print('User not found.')
        return
    if args.new_email:
        if not db.update_user_email(user_id, args.new_email):
            print('Failed to update email.')
            return
    if args.new_password:
        if not db.update_user_password(user_id, args.new_password):
            print('Failed to update password.')
            return
    if args.admin is not None:
        if not db.update_user_role(user_id, args.admin):
            print('Failed to update role.')
            return
    print('User updated successfully.')

def build_parser():
    parser = argparse.ArgumentParser(description='Manage CourtShoes AI database.')
    subparsers = parser.add_subparsers(dest='entity', required=True)

    # User subcommands
    user_parser = subparsers.add_parser('user', help='Manage users')
    user_sub = user_parser.add_subparsers(dest='action', required=True)

    ua = user_sub.add_parser('add', help='Add user')
    ua.add_argument('--email', required=True)
    ua.add_argument('--password', required=True)
    ua.add_argument('--admin', action='store_true')
    ua.set_defaults(func=user_add)

    ud = user_sub.add_parser('delete', help='Delete user')
    ud.add_argument('--id', type=int)
    ud.add_argument('--email')
    ud.set_defaults(func=user_delete)

    ul = user_sub.add_parser('list', help='List users')
    ul.set_defaults(func=user_list)

    uu = user_sub.add_parser('update', help='Update user')
    uu.add_argument('--id', type=int)
    uu.add_argument('--email')
    uu.add_argument('--new-email')
    uu.add_argument('--new-password')
    uu.add_argument('--admin', type=lambda x: x.lower() == 'true')
    uu.set_defaults(func=user_update)

    # Database management commands
    db_parser = subparsers.add_parser('database', help='Manage database')
    db_sub = db_parser.add_subparsers(dest='action', required=True)

    # Clean command - drop and recreate tables
    clean = db_sub.add_parser('clean', help='Drop all tables and recreate structure')
    clean.add_argument('--confirm', action='store_true', help='Confirm the operation')
    clean.set_defaults(func=drop_all_tables)

    # Reset command - completely reset database (delete SQLite file or drop all tables)
    reset = db_sub.add_parser('reset', help='Completely reset database')
    reset.add_argument('--confirm', action='store_true', help='Confirm the operation')
    reset.set_defaults(func=reset_database)

    return parser


def main():
    ensure_tables()
    parser = build_parser()
    if parser:
        args = parser.parse_args()
        if hasattr(args, 'func'):
            args.func(args)
        else:
            parser.print_help()
    else:
        print("❌ Erro ao inicializar parser de comandos.")


if __name__ == '__main__':
    main()
