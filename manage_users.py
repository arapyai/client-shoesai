# manage_users.py
import argparse
from database_abstraction import add_user, create_tables

def main():
    # Ensure tables exist
    create_tables()
    print("Database tables ensured/created.")

    parser = argparse.ArgumentParser(description="Manage CourtShoes AI users.")
    parser.add_argument("action", choices=["add"], help="Action to perform (add)")
    parser.add_argument("--email", required=True, help="User's email address")
    parser.add_argument("--password", required=True, help="User's password")
    parser.add_argument("--admin", action="store_true", help="Make user an admin")

    args = parser.parse_args()

    if args.action == "add":
        if add_user(args.email, args.password, args.admin):
            role = "Admin" if args.admin else "User"
            print(f"{role} '{args.email}' added successfully.")
        else:
            print(f"Error: Could not add user '{args.email}'. Email might already exist.")

if __name__ == "__main__":
    main()