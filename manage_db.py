# manage_db.py
import argparse
import json
import pandas as pd
from database_abstraction import db


def load_json_records(path: str):
    """Load a JSON file that may be a list or dict of columns."""
    with open(path, 'r', encoding='utf-8-sig') as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    return pd.DataFrame(data).to_dict(orient='records')


def get_user_by_email(email: str):
    for user in db.get_all_users():
        if user['email'] == email:
            return user
    return None


def ensure_tables():
    db.create_tables()


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


# ---- Marathon Operations ----

def marathon_list(args):
    marathons = db.get_marathon_list_from_db()
    for m in marathons:
        print(f"{m['id']}: {m['name']} ({m.get('event_date','')})")


def marathon_add(args):
    user_id = args.user_id or 1
    marathon_id = db.add_marathon_metadata(
        args.name,
        args.event_date,
        args.location,
        args.distance,
        args.description,
        args.json,
        user_id,
    )
    if not marathon_id:
        print('Error adding marathon metadata.')
        return
    records = load_json_records(args.json)
    if db.insert_parsed_json_data(marathon_id, records):
        print(f"Marathon '{args.name}' added with ID {marathon_id}.")
    else:
        print('Failed to import JSON data.')


def marathon_delete(args):
    if db.delete_marathon_by_id(args.id):
        print(f"Marathon {args.id} deleted successfully.")
    else:
        print('Error deleting marathon.')


def marathon_update(args):
    # update metadata or import JSON
    fields = {}
    if args.name:
        fields['name'] = args.name
    if args.event_date:
        fields['event_date'] = args.event_date
    if args.location:
        fields['location'] = args.location
    if args.distance is not None:
        fields['distance_km'] = args.distance
    if args.description:
        fields['description'] = args.description
    if fields:
        from sqlalchemy import update
        with db.get_connection() as conn:
            stmt = update(db.marathons).where(db.marathons.c.marathon_id == args.id).values(**fields)
            conn.execute(stmt)
            conn.commit()
    if args.json:
        records = load_json_records(args.json)
        db.insert_parsed_json_data(args.id, records)
    print('Marathon updated successfully.')


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

    # Marathon subcommands
    m_parser = subparsers.add_parser('marathon', help='Manage marathons')
    m_sub = m_parser.add_subparsers(dest='action', required=True)

    ma = m_sub.add_parser('add', help='Add marathon from JSON')
    ma.add_argument('--name', required=True)
    ma.add_argument('--json', required=True)
    ma.add_argument('--event-date')
    ma.add_argument('--location')
    ma.add_argument('--distance', type=float)
    ma.add_argument('--description')
    ma.add_argument('--user-id', type=int)
    ma.set_defaults(func=marathon_add)

    md = m_sub.add_parser('delete', help='Delete marathon')
    md.add_argument('--id', type=int, required=True)
    md.set_defaults(func=marathon_delete)

    ml = m_sub.add_parser('list', help='List marathons')
    ml.set_defaults(func=marathon_list)

    mu = m_sub.add_parser('update', help='Update marathon metadata or add JSON')
    mu.add_argument('--id', type=int, required=True)
    mu.add_argument('--name')
    mu.add_argument('--event-date')
    mu.add_argument('--location')
    mu.add_argument('--distance', type=float)
    mu.add_argument('--description')
    mu.add_argument('--json')
    mu.set_defaults(func=marathon_update)

    return parser


def main():
    ensure_tables()
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
