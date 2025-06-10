# Database Abstraction Layer

This document explains how to use the new database abstraction layer that supports multiple database providers (SQLite, PostgreSQL, MySQL).

## Overview

The database abstraction layer provides:
- **Multi-database support**: SQLite, PostgreSQL, MySQL
- **Connection pooling**: Better performance for production
- **Type safety**: Better error handling and validation  
- **Environment-based configuration**: Easy deployment across environments
- **Backward compatibility**: Minimal changes to existing code

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Database

Copy the example environment file:
```bash
cp .env.example .env
```

Edit `.env` to configure your database:

**For SQLite (default):**
```bash
DB_TYPE=sqlite
DB_PATH=courtshoes_data.db
```

**For PostgreSQL:**
```bash
DB_TYPE=postgresql
DB_HOST=localhost
DB_PORT=5432
DB_NAME=courtshoes
DB_USER=your_username
DB_PASSWORD=your_password
```

**For MySQL:**
```bash
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_NAME=courtshoes
DB_USER=your_username
DB_PASSWORD=your_password
```

### 3. Migration from Current System

Run the migration script to transition from your current SQLite database:

```bash
python migrate_to_abstracted_db.py
```

This will:
- Backup your existing data
- Create new tables with the abstracted schema
- Migrate all data
- Verify data integrity

## Usage Examples

### Basic Database Operations

```python
from database_abstraction import db

# Create tables
db.create_tables()

# Add a user
success = db.add_user("user@example.com", "password123", is_admin=False)

# Verify user credentials
user = db.verify_user("user@example.com", "password123")
if user:
    print(f"User ID: {user['user_id']}")

# Execute custom queries
results = db.execute_query("SELECT * FROM users WHERE is_admin = :admin", {"admin": True})

# Execute queries as DataFrame
df = db.execute_query_df("SELECT * FROM marathons ORDER BY event_date DESC")
```

### Using Connection Context Manager

```python
from database_abstraction import db
from sqlalchemy import select

# Safe database operations with automatic cleanup
with db.get_connection() as conn:
    stmt = select(db.users).where(db.users.c.email == "test@example.com")
    result = conn.execute(stmt).fetchone()
    if result:
        print(f"Found user: {result.email}")
```

## Database Provider Setup

### PostgreSQL Setup

1. Install PostgreSQL server
2. Create database:
   ```sql
   CREATE DATABASE courtshoes;
   CREATE USER courtshoes_user WITH PASSWORD 'your_password';
   GRANT ALL PRIVILEGES ON DATABASE courtshoes TO courtshoes_user;
   ```
3. Configure `.env`:
   ```bash
   DB_TYPE=postgresql
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=courtshoes
   DB_USER=courtshoes_user
   DB_PASSWORD=your_password
   ```

### MySQL Setup

1. Install MySQL server
2. Create database:
   ```sql
   CREATE DATABASE courtshoes;
   CREATE USER 'courtshoes_user'@'localhost' IDENTIFIED BY 'your_password';
   GRANT ALL PRIVILEGES ON courtshoes.* TO 'courtshoes_user'@'localhost';
   FLUSH PRIVILEGES;
   ```
3. Configure `.env`:
   ```bash
   DB_TYPE=mysql
   DB_HOST=localhost
   DB_PORT=3306
   DB_NAME=courtshoes
   DB_USER=courtshoes_user
   DB_PASSWORD=your_password
   ```

## Schema

The abstracted database uses the following tables:

- **users**: User accounts and authentication
- **marathons**: Marathon/event metadata
- **marathon_metrics**: Pre-computed analytics (for performance)
- **images**: Image files and metadata
- **shoe_detections**: AI-detected shoe brands and bounding boxes
- **person_demographics**: AI-detected person demographics

All tables use consistent naming (lowercase with underscores) and proper foreign key relationships.

## Performance Considerations

1. **Connection Pooling**: Automatically managed by SQLAlchemy
2. **Query Optimization**: Use the DataFrame methods for large result sets
3. **Indexing**: Key columns are automatically indexed
4. **Prepared Statements**: All queries use parameterized statements

## Migration Notes

### Backward Compatibility

The migration script preserves:
- All existing data
- Table relationships
- Auto-increment sequences
- Unique constraints

### Breaking Changes

- Table names changed from `PascalCase` to `snake_case`
- Connection handling now uses context managers
- Error handling is more consistent

## Troubleshooting

### Common Issues

1. **Connection Errors**: Check your `.env` configuration
2. **Missing Dependencies**: Run `pip install -r requirements.txt`
3. **Migration Errors**: Check backup files are created before migration
4. **Permission Errors**: Ensure database user has proper privileges

### Debug Mode

Enable SQL query logging:
```bash
DB_ECHO=true
```

This will print all SQL queries to help debug issues.

## Environment Variables Reference

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DB_TYPE` | Database type (sqlite/postgresql/mysql) | sqlite | No |
| `DB_PATH` | SQLite database file path | courtshoes_data.db | SQLite only |
| `DB_HOST` | Database server hostname | localhost | PostgreSQL/MySQL |
| `DB_PORT` | Database server port | 5432/3306 | PostgreSQL/MySQL |
| `DB_NAME` | Database name | courtshoes | PostgreSQL/MySQL |
| `DB_USER` | Database username | - | PostgreSQL/MySQL |
| `DB_PASSWORD` | Database password | - | PostgreSQL/MySQL |
| `DB_ECHO` | Enable SQL query logging | false | No |

## Security Best Practices

1. **Never commit `.env` files** - They're already in `.gitignore`
2. **Use strong passwords** for database users
3. **Limit database user permissions** to only what's needed
4. **Use SSL connections** in production environments
5. **Regularly backup your data**

## Production Deployment

For production environments:

1. Use PostgreSQL or MySQL instead of SQLite
2. Set up proper database backups
3. Configure SSL/TLS connections
4. Use environment-specific `.env` files
5. Monitor connection pool metrics
6. Set up database monitoring and alerting

## Next Steps

After migration:
1. Test all application functionality
2. Update any direct database imports
3. Consider setting up database monitoring
4. Plan for regular backups
5. Document your specific database configuration
