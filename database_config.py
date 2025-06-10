# database_config.py
import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
DATABASE_CONFIG = {
    # SQLite (default - current setup)
    'sqlite': {
        'url': f"sqlite:///{os.getenv('DB_PATH', 'courtshoes_data.db')}",
        'echo': os.getenv('DB_ECHO', 'false').lower() == 'true'
    },
    
    # PostgreSQL
    'postgresql': {
        'url': f"postgresql://{os.getenv('DB_USER', 'user')}:{os.getenv('DB_PASSWORD', 'password')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'courtshoes')}",
        'echo': os.getenv('DB_ECHO', 'false').lower() == 'true'
    },
    
    # MySQL
    'mysql': {
        'url': f"mysql+pymysql://{os.getenv('DB_USER', 'user')}:{os.getenv('DB_PASSWORD', 'password')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '3306')}/{os.getenv('DB_NAME', 'courtshoes')}",
        'echo': os.getenv('DB_ECHO', 'false').lower() == 'true'
    }
}

def get_database_config() -> Dict[str, Any]:
    """Get database configuration based on environment variable DB_TYPE."""
    db_type = os.getenv('DB_TYPE', 'sqlite').lower()
    
    if db_type not in DATABASE_CONFIG:
        raise ValueError(f"Unsupported database type: {db_type}. Supported types: {list(DATABASE_CONFIG.keys())}")
    
    return DATABASE_CONFIG[db_type]
