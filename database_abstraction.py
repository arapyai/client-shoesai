# database_abstraction.py
"""
Database abstraction layer using SQLAlchemy Core.
Supports SQLite, PostgreSQL, and MySQL.
"""

import json
import logging
from contextlib import contextmanager
from typing import List, Dict, Any, Optional, Union, Tuple
import pandas as pd
from sqlalchemy import (
    create_engine, text, MetaData, Table, Column, Integer, String, 
    Float, Boolean, DateTime, Text, ForeignKey, UniqueConstraint,
    select, insert, update, delete, and_, or_, func
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
            logger.info(f"Database URL: {self.engine.url}")
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
            Column('uploaded_by_user_id', Integer, ForeignKey('users.user_id')),
            Column('upload_timestamp', DateTime, default=datetime.now)
        )
        
        # Marathon runners table
        self.marathon_runners = Table(
            'marathon_runners', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('marathon_id', Integer, ForeignKey('marathons.marathon_id')),
            Column('bib', Integer),
            Column('position', Integer),
            Column('shoe_brand', String(100)),
            Column('shoe_model', String(100)),
            Column('gender', String(10)),
            Column('run_category', String(50)),
            Column('confidence', Float))
    
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

    # Marathon Management Methods
    def add_marathon_metadata(self, name: str, event_date: Optional[str], location: Optional[str], 
                            distance_km: Optional[float], description: Optional[str], 
                            user_id: int) -> Optional[int]:
        """Add marathon metadata to the database."""
        try:
            with self.get_connection() as conn:
                stmt = insert(self.marathons).values(
                    name=name,
                    event_date=event_date,
                    location=location,
                    distance_km=distance_km,
                    description=description,
                    uploaded_by_user_id=user_id,
                    upload_timestamp=datetime.now()
                )
                result = conn.execute(stmt)
                if result.inserted_primary_key:
                    marathon_id = result.inserted_primary_key[0]
                    conn.commit()
                    logger.info(f"Successfully added marathon '{name}' with ID {marathon_id}")
                    return marathon_id
                else:
                    logger.error(f"Failed to get marathon ID after insertion")
                    return None
        except IntegrityError:
            logger.error(f"Marathon with name '{name}' already exists")
            return None
        except Exception as e:
            logger.error(f"Failed to add marathon metadata: {e}")
            return None

    def add_marathon_runner(self, marathon_id: int, bib: Optional[int], position: Optional[int],
                           shoe_brand: Optional[str], shoe_model: Optional[str], 
                           gender: Optional[str], run_category: Optional[str], 
                           confidence: Optional[float]) -> bool:
        """Add a single marathon runner's data to the database."""
        try:
            with self.get_connection() as conn:
                stmt = insert(self.marathon_runners).values(
                    marathon_id=marathon_id,
                    bib=bib,
                    position=position,
                    shoe_brand=shoe_brand,
                    shoe_model=shoe_model,
                    gender=gender,
                    run_category=run_category,
                    confidence=confidence
                )
                conn.execute(stmt)
                conn.commit()
                logger.info(f"Successfully added runner data for marathon {marathon_id}")
                return True
        except Exception as e:
            logger.error(f"Failed to add marathon runner: {e}")
            return False
    
    def add_marathon_runners_bulk(self, marathon_id: int, runners_data: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        Add multiple marathon runners' data to the database in bulk.
        
        Args:
            marathon_id: ID of the marathon
            runners_data: List of dictionaries containing runner data
            
        Returns:
            Tuple of (successful_inserts, failed_inserts)
        """
        successful = 0
        failed = 0
        
        # Process data in smaller batches to avoid transaction issues
        batch_size = 100
        
        for i in range(0, len(runners_data), batch_size):
            batch = runners_data[i:i + batch_size]
            
            try:
                with self.get_connection() as conn:
                    trans = conn.begin()
                    try:
                        for runner_data in batch:
                            # Clean and validate data
                            cleaned_data = self._clean_runner_data(runner_data, marathon_id)
                            if cleaned_data:
                                stmt = insert(self.marathon_runners).values(**cleaned_data)
                                conn.execute(stmt)
                                successful += 1
                            else:
                                failed += 1
                        
                        trans.commit()
                        logger.debug(f"Batch {i//batch_size + 1}: {len(batch)} runners processed")
                        
                    except Exception as e:
                        trans.rollback()
                        logger.warning(f"Batch {i//batch_size + 1} failed: {e}")
                        failed += len(batch)
                        
            except Exception as e:
                logger.error(f"Failed to process batch {i//batch_size + 1}: {e}")
                failed += len(batch)
        
        logger.info(f"Bulk insert completed: {successful} successful, {failed} failed")
        return successful, failed
    
    def _clean_runner_data(self, runner_data: Dict[str, Any], marathon_id: int) -> Optional[Dict[str, Any]]:
        """Clean and validate runner data before insertion."""
        try:
            # Ensure marathon_id is included
            cleaned: Dict[str, Any] = {'marathon_id': marathon_id}
            
            # Convert pandas types to Python native types and clean data
            if 'bib' in runner_data and runner_data['bib'] is not None:
                cleaned['bib'] = int(runner_data['bib']) if str(runner_data['bib']).strip() not in ['', '?', 'nan'] else None
            
            if 'position' in runner_data and runner_data['position'] is not None:
                pos_str = str(runner_data['position']).strip()
                if pos_str not in ['', '?', 'nan', '-']:
                    try:
                        cleaned['position'] = int(pos_str)
                    except (ValueError, TypeError):
                        cleaned['position'] = None
                else:
                    cleaned['position'] = None
            
            if 'shoe_brand' in runner_data and runner_data['shoe_brand'] is not None:
                brand = str(runner_data['shoe_brand']).strip()
                cleaned['shoe_brand'] = brand if brand not in ['', '?', 'nan'] else None
            
            if 'shoe_model' in runner_data and runner_data['shoe_model'] is not None:
                model = str(runner_data['shoe_model']).strip()
                cleaned['shoe_model'] = model if model not in ['', '?', 'nan'] else None
            
            if 'gender' in runner_data and runner_data['gender'] is not None:
                gender = str(runner_data['gender']).strip().upper()
                cleaned['gender'] = gender if gender not in ['', '?', 'NAN'] else None
            
            if 'run_category' in runner_data and runner_data['run_category'] is not None:
                category = str(runner_data['run_category']).strip()
                cleaned['run_category'] = category if category not in ['', '?', 'nan'] else None
            
            if 'confidence' in runner_data and runner_data['confidence'] is not None:
                try:
                    confidence = float(runner_data['confidence'])
                    # Limit confidence to reasonable values (0-100)
                    if 0 <= confidence <= 100:
                        cleaned['confidence'] = confidence
                    else:
                        cleaned['confidence'] = None
                except (ValueError, TypeError):
                    cleaned['confidence'] = None
            
            return cleaned
            
        except Exception as e:
            logger.warning(f"Failed to clean runner data: {e}")
            return None
    
    def get_marathon_runners(self, marathon_id: int) -> List[Dict[str, Any]]:
        """Get all runners data for a specific marathon."""
        try:
            with self.get_connection() as conn:
                stmt = select(self.marathon_runners).where(
                    self.marathon_runners.c.marathon_id == marathon_id
                ).order_by(self.marathon_runners.c.position)
                
                result = conn.execute(stmt).fetchall()
                return [
                    {
                        "id": row.id,
                        "marathon_id": row.marathon_id,
                        "bib": row.bib,
                        "position": row.position,
                        "shoe_brand": row.shoe_brand,
                        "shoe_model": row.shoe_model,
                        "gender": row.gender,
                        "run_category": row.run_category,
                        "confidence": row.confidence
                    }
                    for row in result
                ]
        except Exception as e:
            logger.error(f"Failed to get marathon runners: {e}")
            return []
    
    def delete_marathon_runners(self, marathon_id: int) -> bool:
        """Delete all runners data for a specific marathon."""
        try:
            with self.get_connection() as conn:
                stmt = delete(self.marathon_runners).where(
                    self.marathon_runners.c.marathon_id == marathon_id
                )
                conn.execute(stmt)
                conn.commit()
                logger.info(f"Deleted all runners data for marathon {marathon_id}")
                return True
        except Exception as e:
            logger.error(f"Failed to delete marathon runners: {e}")
            return False
    
    def save_race_statistics(self, marathon_id: int, race_statistics: Dict[str, Any]) -> bool:
        """
        Save race statistics as JSON in the marathon description field or a separate statistics table.
        For now, we'll append it to the description field.
        """
        try:
            with self.get_connection() as conn:
                # Get current description
                stmt = select(self.marathons.c.description).where(
                    self.marathons.c.marathon_id == marathon_id
                )
                result = conn.execute(stmt).fetchone()
                
                current_description = result.description if result and result.description else ""
                
                # Convert numpy/pandas types to Python native types for JSON serialization
                cleaned_stats = self._clean_statistics_for_json(race_statistics)
                
                # Create statistics section
                stats_json = json.dumps(cleaned_stats, indent=2, ensure_ascii=False, default=str)
                statistics_section = f"\n\n--- ESTATÍSTICAS AUTOMÁTICAS ---\n{stats_json}"
                
                # Update description with statistics
                updated_description = current_description + statistics_section
                
                update_stmt = update(self.marathons).where(
                    self.marathons.c.marathon_id == marathon_id
                ).values(description=updated_description)
                
                conn.execute(update_stmt)
                conn.commit()
                logger.info(f"Successfully saved race statistics for marathon {marathon_id}")
                return True
        except Exception as e:
            logger.error(f"Failed to save race statistics: {e}")
            return False
    
    def _clean_statistics_for_json(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Clean statistics data to make it JSON serializable."""
        try:
            import numpy as np
        except ImportError:
            np = None
        
        def convert_value(value):
            # Handle numpy/pandas numeric types
            if hasattr(value, 'item'):  # numpy scalars
                return value.item()
            elif np and hasattr(np, 'integer') and isinstance(value, np.integer):
                return int(value)
            elif np and hasattr(np, 'floating') and isinstance(value, np.floating):
                return float(value)
            elif isinstance(value, dict):
                return {k: convert_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [convert_value(v) for v in value]
            else:
                return value
        
        result = convert_value(stats)
        # Ensure we return a dict
        if isinstance(result, dict):
            return result
        else:
            return {"data": result}
    
    def get_data_for_selected_marathons_db(self, marathon_ids: List[int]) -> Tuple[Any, Any]:
        """
        Get marathon and runner data for selected marathon IDs.
        
        Args:
            marathon_ids: List of marathon IDs to fetch data for
            
        Returns:
            Tuple of (marathons_df, runners_df)
        """
        try:
            import pandas as pd
            
            with self.get_connection() as conn:
                # Get marathon metadata
                marathons_query = select(self.marathons).where(
                    self.marathons.c.marathon_id.in_(marathon_ids)
                )
                marathons_df = pd.read_sql(marathons_query, conn)
                
                # Get runners data for these marathons
                runners_query = select(self.marathon_runners).where(
                    self.marathon_runners.c.marathon_id.in_(marathon_ids)
                )
                runners_df = pd.read_sql(runners_query, conn)
                
                return marathons_df, runners_df
                
        except Exception as e:
            logger.error(f"Failed to get data for marathons {marathon_ids}: {e}")
            try:
                import pandas as pd
                return pd.DataFrame(), pd.DataFrame()
            except ImportError:
                return [], []

    def get_marathon_list_from_db(self) -> List[Dict]:
        """Get list of all marathons with basic info."""
        try:
            with self.get_connection() as conn:
                stmt = select(
                    self.marathons.c.marathon_id.label('id'),
                    self.marathons.c.name,
                    self.marathons.c.event_date,
                    self.marathons.c.location,
                    self.marathons.c.distance_km,
                    self.marathons.c.upload_timestamp
                ).order_by(self.marathons.c.upload_timestamp.desc())
                
                result = conn.execute(stmt).fetchall()
                return [
                    {
                        "id": row.id,
                        "name": row.name,
                        "event_date": row.event_date,
                        "location": row.location,
                        "distance_km": row.distance_km,
                        "upload_timestamp": row.upload_timestamp
                    }
                    for row in result
                ]
        except Exception as e:
            logger.error(f"Failed to get marathon list: {e}")
            return []
  
    
    def get_individual_marathon_metrics(self, marathon_id: int) -> Dict[str, Any]:
        """
        Get detailed metrics for a single marathon.
        
        Args:
            marathon_id: ID of the marathon to get metrics for
            
        Returns:
            Dictionary containing detailed marathon metrics
        """
        try:
            with self.get_connection() as conn:
                # Get marathon basic info
                marathon_stmt = select(self.marathons).where(
                    self.marathons.c.marathon_id == marathon_id
                )
                marathon_result = conn.execute(marathon_stmt).fetchone()
                
                if not marathon_result:
                    return {}
                
                # Get total participants
                total_stmt = select(func.count(self.marathon_runners.c.id)).where(
                    self.marathon_runners.c.marathon_id == marathon_id
                )
                total_participants = conn.execute(total_stmt).scalar() or 0
                
                # Get brand distribution
                brand_stmt = select(
                    self.marathon_runners.c.shoe_brand,
                    func.count(self.marathon_runners.c.shoe_brand).label('count')
                ).where(
                    and_(
                        self.marathon_runners.c.marathon_id == marathon_id,
                        self.marathon_runners.c.shoe_brand.isnot(None)
                    )
                ).group_by(self.marathon_runners.c.shoe_brand).order_by(
                    func.count(self.marathon_runners.c.shoe_brand).desc()
                )
                brand_results = conn.execute(brand_stmt).fetchall()
                
                # Build brand distribution
                brand_distribution = {}
                for row in brand_results:
                    brand_distribution[row.shoe_brand] = row.count
                                
                # Calculate leader brand info
                leader_brand = None
                leader_count = 0
                leader_percentage = 0
                
                if brand_distribution:
                    leader_brand = list(brand_distribution.keys())[0]
                    leader_count = brand_distribution[leader_brand]
                    leader_percentage = round((leader_count / total_participants) * 100, 2) if total_participants > 0 else 0
                
                # Build comprehensive metrics
                metrics = {
                    'marathon_id': marathon_id,
                    'marathon_name': marathon_result.name,
                    'event_date': marathon_result.event_date,
                    'location': marathon_result.location,
                    'distance_km': marathon_result.distance_km,
                    'upload_timestamp': marathon_result.upload_timestamp,
                    
                    # Participation metrics
                    'total_participants': total_participants,  
                    # Brand metrics
                    'total_brands': len(brand_distribution),
                    'brand_distribution': brand_distribution,
                    'leader_brand': {
                        'name': leader_brand or 'N/A',
                        'count': leader_count,
                        'percentage': leader_percentage
                    },
            }
            
            return metrics
                
        except Exception as e:
            logger.error(f"Failed to get individual marathon metrics for {marathon_id}: {e}")
            return {}
    
    def get_gender_brand_distribution(self, marathon_id: int):
        """
        Get gender distribution faceted by brand for a specific marathon.

        Args:
            marathon_id: ID of the marathon
            
        Returns:
            Dictionary containing gender-brand cross-tabulation data
        """
        with self.get_connection() as conn:
            # Get gender-brand cross-tabulation
            stmt = select(
                self.marathon_runners.c.shoe_brand,
                self.marathon_runners.c.gender,
                func.count(self.marathon_runners.c.id).label('count')
            ).where(
                and_(
                    self.marathon_runners.c.marathon_id == marathon_id,
                    self.marathon_runners.c.shoe_brand.isnot(None),
                    self.marathon_runners.c.gender.isnot(None)
                )
            ).group_by(
                self.marathon_runners.c.shoe_brand,
                self.marathon_runners.c.gender
            ).order_by(
                self.marathon_runners.c.shoe_brand,
                self.marathon_runners.c.gender
            )
            
            results = conn.execute(stmt).fetchall()

            results_df = pd.DataFrame(results)
            return(results_df)    
    
    def get_category_brand_distribution(self, marathon_id: int):
        """
        Get run category distribution faceted by brand for a specific marathon.

        Args:
            marathon_id: ID of the marathon
            
        Returns:
            Dictionary containing category-brand cross-tabulation data
        """
        with self.get_connection() as conn:
            # Get category-brand cross-tabulation
            stmt = select(
                self.marathon_runners.c.shoe_brand,
                self.marathon_runners.c.run_category,
                func.count(self.marathon_runners.c.id).label('count')
            ).where(
                and_(
                    self.marathon_runners.c.marathon_id == marathon_id,
                    self.marathon_runners.c.shoe_brand.isnot(None),
                    self.marathon_runners.c.run_category.isnot(None)
                )
            ).group_by(
                self.marathon_runners.c.shoe_brand,
                self.marathon_runners.c.run_category
            ).order_by(
                self.marathon_runners.c.shoe_brand,
                self.marathon_runners.c.run_category
            )
            
            results = conn.execute(stmt).fetchall()

            results_df = pd.DataFrame(results)
            return(results_df)

    def delete_marathon_by_id(self, marathon_id: int) -> bool:
        """
        Delete a marathon and all associated data by marathon ID.
        
        Args:
            marathon_id: ID of the marathon to delete
            
        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                trans = conn.begin()
                try:
                    # First, delete all runners associated with this marathon
                    delete_runners_stmt = delete(self.marathon_runners).where(
                        self.marathon_runners.c.marathon_id == marathon_id
                    )
                    runners_result = conn.execute(delete_runners_stmt)
                    
                    # Then, delete the marathon itself
                    delete_marathon_stmt = delete(self.marathons).where(
                        self.marathons.c.marathon_id == marathon_id
                    )
                    marathon_result = conn.execute(delete_marathon_stmt)
                    
                    if marathon_result.rowcount > 0:
                        trans.commit()
                        logger.info(f"Successfully deleted marathon {marathon_id} and {runners_result.rowcount} associated runners")
                        return True
                    else:
                        trans.rollback()
                        logger.warning(f"Marathon {marathon_id} not found for deletion")
                        return False
                        
                except Exception as e:
                    trans.rollback()
                    raise e
                    
        except Exception as e:
            logger.error(f"Failed to delete marathon {marathon_id}: {e}")
            return False




# Global database manager instance
try:
    db = DatabaseManager()
except Exception as e:
    logger.error(f"Failed to initialize database manager: {e}")
    db = None