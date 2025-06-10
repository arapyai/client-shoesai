# database_abstraction.py
"""
Database abstraction layer using SQLAlchemy Core.
Supports SQLite, PostgreSQL, and MySQL.
"""

import logging
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Union, Tuple
import pandas as pd
from sqlalchemy import (
    create_engine, text, MetaData, Table, Column, Integer, String, 
    Float, Boolean, DateTime, Text, ForeignKey, UniqueConstraint,
    select, insert, update, delete, and_, or_
)
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from datetime import datetime
from passlib.hash import pbkdf2_sha256 as hasher

from database_config import get_database_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Database manager that abstracts different database providers.
    """
    
    def __init__(self):
        self.engine: Optional[Engine] = None
        self.metadata = MetaData()
        self._define_tables()
        self._initialize_engine()
    
    def _initialize_engine(self):
        """Initialize the database engine based on configuration."""
        config = get_database_config()
        try:
            self.engine = create_engine(
                config['url'], 
                echo=config.get('echo', False),
                # Connection pool settings for production
                pool_pre_ping=True,
                pool_recycle=3600
            )
            logger.info(f"Database engine initialized: {self.engine.url.drivername}")
        except Exception as e:
            logger.error(f"Failed to initialize database engine: {e}")
            raise
    
    def _define_tables(self):
        """Define database schema using SQLAlchemy Core."""
        
        # Users table
        self.users = Table(
            'users', self.metadata,
            Column('user_id', Integer, primary_key=True, autoincrement=True),
            Column('email', String(255), unique=True, nullable=False),
            Column('hashed_password', Text, nullable=False),
            Column('is_admin', Boolean, default=False)
        )
        
        # Marathons table
        self.marathons = Table(
            'marathons', self.metadata,
            Column('marathon_id', Integer, primary_key=True, autoincrement=True),
            Column('name', String(255), unique=True, nullable=False),
            Column('event_date', String(50)),
            Column('location', String(255)),
            Column('distance_km', Float),
            Column('description', Text),
            Column('original_json_filename', String(255)),
            Column('uploaded_by_user_id', Integer, ForeignKey('users.user_id')),
            Column('upload_timestamp', DateTime, default=datetime.now)
        )
        
        # Marathon Metrics table
        self.marathon_metrics = Table(
            'marathon_metrics', self.metadata,
            Column('metric_id', Integer, primary_key=True, autoincrement=True),
            Column('marathon_id', Integer, ForeignKey('marathons.marathon_id', ondelete='CASCADE'), unique=True, nullable=False),
            Column('total_images', Integer, default=0),
            Column('total_shoes_detected', Integer, default=0),
            Column('total_persons_with_demographics', Integer, default=0),
            Column('unique_brands_count', Integer, default=0),
            Column('leader_brand_name', Text),
            Column('leader_brand_count', Integer, default=0),
            Column('leader_brand_percentage', Float, default=0.0),
            Column('brand_counts_json', Text),
            Column('gender_distribution_json', Text),
            Column('race_distribution_json', Text),
            Column('category_distribution_json', Text),
            Column('top_brands_json', Text),
            Column('last_calculated', DateTime, default=datetime.now)
        )
        
        # Images table
        self.images = Table(
            'images', self.metadata,
            Column('image_id', Integer, primary_key=True, autoincrement=True),
            Column('marathon_id', Integer, ForeignKey('marathons.marathon_id'), nullable=False),
            Column('filename', String(255), nullable=False),
            Column('category', String(100)),
            Column('original_width', Integer),
            Column('original_height', Integer),
            UniqueConstraint('marathon_id', 'filename', name='uq_marathon_filename')
        )
        
        # Shoe Detections table
        self.shoe_detections = Table(
            'shoe_detections', self.metadata,
            Column('detection_id', Integer, primary_key=True, autoincrement=True),
            Column('image_id', Integer, ForeignKey('images.image_id'), nullable=False),
            Column('brand', String(100)),
            Column('probability', Float),
            Column('confidence', Float),
            Column('bbox_x1', Float),
            Column('bbox_y1', Float),
            Column('bbox_x2', Float),
            Column('bbox_y2', Float)
        )
        
        # Person Demographics table
        self.person_demographics = Table(
            'person_demographics', self.metadata,
            Column('demographic_id', Integer, primary_key=True, autoincrement=True),
            Column('image_id', Integer, ForeignKey('images.image_id'), unique=True, nullable=False),
            Column('gender_label', String(50)),
            Column('gender_prob', Float),
            Column('age_label', String(50)),
            Column('age_prob', Float),
            Column('race_label', String(50)),
            Column('race_prob', Float),
            Column('person_bbox_x1', Float),
            Column('person_bbox_y1', Float),
            Column('person_bbox_x2', Float),
            Column('person_bbox_y2', Float)
        )
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        if not self.engine:
            raise RuntimeError("Database engine not initialized")
        
        conn = self.engine.connect()
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            conn.close()
    
    def create_tables(self):
        """Create all tables if they don't exist."""
        try:
            if not self.engine:
                raise RuntimeError("Database engine not initialized")
            
            self.metadata.create_all(self.engine)
            logger.info("Database tables created/ensured.")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute a raw SQL query and return results as list of dictionaries."""
        with self.get_connection() as conn:
            result = conn.execute(text(query), params or {})
            return [dict(row._mapping) for row in result.fetchall()]
    
    def execute_query_df(self, query: str, params: Optional[List] = None) -> pd.DataFrame:
        """Execute a query and return results as pandas DataFrame."""
        try:
            return pd.read_sql_query(query, self.engine, params=params)
        except Exception as e:
            logger.error(f"Failed to execute query as DataFrame: {e}")
            raise
    
    # User Management Methods
    def add_user(self, email: str, password: str, is_admin: bool = False) -> bool:
        """Add a new user to the database."""
        try:
            hashed_password = hasher.hash(password)
            with self.get_connection() as conn:
                stmt = insert(self.users).values(
                    email=email,
                    hashed_password=hashed_password,
                    is_admin=is_admin
                )
                conn.execute(stmt)
                conn.commit()
                return True
        except IntegrityError:
            logger.warning(f"User with email {email} already exists")
            return False
        except Exception as e:
            logger.error(f"Failed to add user: {e}")
            return False
    
    def verify_user(self, email: str, password: str) -> Optional[Dict]:
        """Verify user credentials and return user info."""
        try:
            with self.get_connection() as conn:
                stmt = select(self.users).where(self.users.c.email == email)
                result = conn.execute(stmt).fetchone()
                
                if result and hasher.verify(password, result.hashed_password):
                    return {
                        "user_id": result.user_id,
                        "email": result.email,
                        "is_admin": bool(result.is_admin)
                    }
                return None
        except Exception as e:
            logger.error(f"Failed to verify user: {e}")
            return None
    
    def update_user_email(self, user_id: int, new_email: str) -> bool:
        """Update user's email address."""
        try:
            with self.get_connection() as conn:
                stmt = update(self.users).where(
                    self.users.c.user_id == user_id
                ).values(email=new_email)
                conn.execute(stmt)
                conn.commit()
                return True
        except IntegrityError:
            logger.warning(f"Email {new_email} already exists")
            return False
        except Exception as e:
            logger.error(f"Failed to update user email: {e}")
            return False
    
    def update_user_password(self, user_id: int, new_password: str) -> bool:
        """Update user's password."""
        try:
            hashed_password = hasher.hash(new_password)
            with self.get_connection() as conn:
                stmt = update(self.users).where(
                    self.users.c.user_id == user_id
                ).values(hashed_password=hashed_password)
                conn.execute(stmt)
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Failed to update user password: {e}")
            return False
    
    def get_all_users(self) -> List[Dict]:
        """Get all users from the database."""
        try:
            with self.get_connection() as conn:
                stmt = select(
                    self.users.c.user_id,
                    self.users.c.email,
                    self.users.c.is_admin
                ).order_by(self.users.c.email)
                result = conn.execute(stmt).fetchall()
                return [
                    {
                        "user_id": row.user_id,
                        "email": row.email,
                        "is_admin": bool(row.is_admin)
                    }
                    for row in result
                ]
        except Exception as e:
            logger.error(f"Failed to get all users: {e}")
            return []
    
    def delete_user(self, user_id: int) -> bool:
        """Delete a user from the database."""
        try:
            with self.get_connection() as conn:
                # Check if user exists
                user_check = select(self.users.c.email).where(self.users.c.user_id == user_id)
                user = conn.execute(user_check).fetchone()
                
                if not user:
                    return False
                
                # Update marathons to set uploaded_by_user_id to NULL
                update_marathons = update(self.marathons).where(
                    self.marathons.c.uploaded_by_user_id == user_id
                ).values(uploaded_by_user_id=None)
                conn.execute(update_marathons)
                
                # Delete the user
                delete_stmt = delete(self.users).where(self.users.c.user_id == user_id)
                conn.execute(delete_stmt)
                
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Failed to delete user: {e}")
            return False
    
    def update_user_role(self, user_id: int, is_admin: bool) -> bool:
        """Update a user's admin status."""
        try:
            with self.get_connection() as conn:
                stmt = update(self.users).where(
                    self.users.c.user_id == user_id
                ).values(is_admin=is_admin)
                conn.execute(stmt)
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Failed to update user role: {e}")
            return False


# Global database manager instance
try:
    db = DatabaseManager()
except Exception as e:
    logger.error(f"Failed to initialize database manager: {e}")
    db = None

# Convenience functions for backward compatibility
def get_db_connection():
    """Legacy function for backward compatibility."""
    if db is None:
        raise RuntimeError("Database manager not initialized")
    return db.get_connection()

def create_tables():
    """Legacy function for backward compatibility."""
    if db is None:
        raise RuntimeError("Database manager not initialized") 
    return db.create_tables()
