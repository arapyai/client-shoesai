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
            Column('image_id', Integer, ForeignKey('images.image_id'), nullable=False),
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
            if not self.engine:
                raise RuntimeError("Database engine not initialized")
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

    # Marathon Management Methods
    def add_marathon_metadata(self, name: str, event_date: Optional[str], location: Optional[str], 
                            distance_km: Optional[float], description: Optional[str], 
                            original_json_filename: Optional[str], user_id: int) -> Optional[int]:
        """Add marathon metadata to the database."""
        try:
            with self.get_connection() as conn:
                stmt = insert(self.marathons).values(
                    name=name,
                    event_date=event_date,
                    location=location,
                    distance_km=distance_km,
                    description=description,
                    original_json_filename=original_json_filename,
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

    def insert_parsed_json_data(
        self,
        marathon_id: int,
        parsed_json_data_list: List[Dict],
        batch_size: int = 500,
    ) -> bool:
        """Insert parsed JSON data into the database in batches.

        Breaking large imports into smaller batches avoids long-running
        transactions and reduces memory usage. Duplicate filenames are
        skipped automatically by using a cache populated with existing
        images as well as those inserted in previous batches.
        """

        if not parsed_json_data_list:
            logger.warning("No data provided for insertion")
            return True

        try:
            # Build cache of existing images for this marathon
            with self.get_connection() as conn:
                existing_stmt = select(self.images.c.image_id, self.images.c.filename).where(
                    self.images.c.marathon_id == marathon_id
                )
                image_id_cache = {
                    row.filename: row.image_id for row in conn.execute(existing_stmt)
                }

            logger.info(
                f"Found {len(image_id_cache)} existing images for marathon {marathon_id} in cache"
            )

            total_processed = 0
            # Process data in batches to keep transactions small
            for batch_start in range(0, len(parsed_json_data_list), batch_size):
                batch = parsed_json_data_list[batch_start : batch_start + batch_size]

                with self.engine.begin() as conn:
                    demographics_records: List[Dict] = []
                    shoes_records: List[Dict] = []

                    for record in batch:
                        filename = record.get("filename")
                        if not filename:
                            logger.warning("Skipping record with no filename")
                            continue

                        if filename in image_id_cache:
                            image_id = image_id_cache[filename]
                        else:
                            result = conn.execute(
                                insert(self.images).values(
                                    marathon_id=marathon_id,
                                    filename=filename,
                                    original_width=record.get("original_width"),
                                    original_height=record.get("original_height"),
                                    category=record.get("folder"),
                                )
                            )
                            image_id = result.inserted_primary_key[0]
                            image_id_cache[filename] = image_id

                        demo = record.get("demographic")
                        if demo:
                            demographics_records.append(
                                self._prepare_demographic_record(image_id, demo)
                            )

                        for shoe in record.get("shoes", []):
                            if isinstance(shoe, dict):
                                shoes_records.append(
                                    self._prepare_shoe_record(image_id, shoe)
                                )

                    if demographics_records:
                        conn.execute(insert(self.person_demographics), demographics_records)
                    if shoes_records:
                        conn.execute(insert(self.shoe_detections), shoes_records)

                total_processed += len(batch)
                logger.info(
                    f"Processed batch {(batch_start // batch_size) + 1}: {len(batch)} records"
                )

            logger.info(
                f"Successfully processed marathon {marathon_id}: {len(image_id_cache)} images total"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to insert parsed JSON data: {e}")
            return False

    def _prepare_demographic_record(self, image_id: int, demographic_data: Dict) -> Dict:
        """Transform demographic JSON data into a record for bulk insert."""
        gender = demographic_data.get('gender', {})
        age = demographic_data.get('age', {})
        race = demographic_data.get('race', {})
        bbox = demographic_data.get('bbox', [None, None, None, None])

        return {
            'image_id': image_id,
            'gender_label': gender.get('label'),
            'gender_prob': gender.get('prob'),
            'age_label': age.get('label'),
            'age_prob': age.get('prob'),
            'race_label': race.get('label'),
            'race_prob': race.get('prob'),
            'person_bbox_x1': bbox[0] if len(bbox) > 0 else None,
            'person_bbox_y1': bbox[1] if len(bbox) > 1 else None,
            'person_bbox_x2': bbox[2] if len(bbox) > 2 else None,
            'person_bbox_y2': bbox[3] if len(bbox) > 3 else None,
        }

    def _prepare_shoe_record(self, image_id: int, shoe_data: Dict) -> Dict:
        """Transform shoe detection JSON data into a record for bulk insert."""
        label = shoe_data.get('label')
        brand = label[0] if isinstance(label, list) and label else label

        prob_data = shoe_data.get('prob')
        prob = prob_data[0] if isinstance(prob_data, list) and prob_data else prob_data

        bbox_data = shoe_data.get('bbox')
        if isinstance(bbox_data, list) and bbox_data:
            bbox_list = bbox_data[0] if isinstance(bbox_data[0], list) else bbox_data
        else:
            bbox_list = [None, None, None, None]

        return {
            'image_id': image_id,
            'brand': brand,
            'probability': prob,
            'confidence': shoe_data.get('confidence'),
            'bbox_x1': bbox_list[0] if len(bbox_list) > 0 else None,
            'bbox_y1': bbox_list[1] if len(bbox_list) > 1 else None,
            'bbox_x2': bbox_list[2] if len(bbox_list) > 2 else None,
            'bbox_y2': bbox_list[3] if len(bbox_list) > 3 else None,
        }

    def delete_marathon_by_id(self, marathon_id: int) -> bool:
        """
        Delete a marathon and all associated data (images, shoe detections, demographics, metrics).
        """
        try:
            with self.get_connection() as conn:
                trans = conn.begin()
                try:
                    # Check if marathon exists
                    marathon_check = select(self.marathons.c.name).where(self.marathons.c.marathon_id == marathon_id)
                    marathon = conn.execute(marathon_check).fetchone()
                    
                    if not marathon:
                        logger.error(f"Marathon with ID {marathon_id} not found")
                        return False
                    
                    marathon_name = marathon.name
                    logger.info(f"Deleting marathon '{marathon_name}' (ID: {marathon_id})...")
                    
                    # Get all image IDs for this marathon
                    image_ids_stmt = select(self.images.c.image_id).where(self.images.c.marathon_id == marathon_id)
                    image_ids = [row.image_id for row in conn.execute(image_ids_stmt).fetchall()]
                    
                    if image_ids:
                        # Delete shoe detections
                        delete_shoes = delete(self.shoe_detections).where(self.shoe_detections.c.image_id.in_(image_ids))
                        conn.execute(delete_shoes)
                        
                        # Delete person demographics
                        delete_demographics = delete(self.person_demographics).where(self.person_demographics.c.image_id.in_(image_ids))
                        conn.execute(delete_demographics)
                    
                    # Delete images
                    delete_images = delete(self.images).where(self.images.c.marathon_id == marathon_id)
                    conn.execute(delete_images)
                    
                    # Delete marathon metrics
                    delete_metrics = delete(self.marathon_metrics).where(self.marathon_metrics.c.marathon_id == marathon_id)
                    conn.execute(delete_metrics)
                    
                    # Finally delete the marathon itself
                    delete_marathon = delete(self.marathons).where(self.marathons.c.marathon_id == marathon_id)
                    conn.execute(delete_marathon)
                    
                    trans.commit()
                    logger.info(f"Successfully deleted marathon '{marathon_name}' and all associated data")
                    return True
                    
                except Exception as e:
                    trans.rollback()
                    logger.error(f"Error deleting marathon {marathon_id}: {e}")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to delete marathon: {e}")
            return False

    def calculate_and_store_marathon_metrics(self, marathon_id: int) -> None:
        """
        Calculate and store pre-computed metrics for a marathon using separate queries to avoid double-counting.
        """
        try:
            logger.info(f"Starting metrics calculation for marathon {marathon_id}")
            
            with self.get_connection() as conn:
                # 1. Count images directly
                images_query = """
                    SELECT COUNT(DISTINCT i.image_id) as total_images
                    FROM images i 
                    WHERE i.marathon_id = :marathon_id
                """
                images_result = conn.execute(text(images_query), {"marathon_id": marathon_id}).fetchone()
                total_images = images_result.total_images if images_result else 0
                
                # 2. Count shoes directly  
                shoes_query = """
                    SELECT COUNT(s.detection_id) as total_shoes
                    FROM shoe_detections s
                    JOIN images i ON s.image_id = i.image_id
                    WHERE i.marathon_id = :marathon_id
                """
                shoes_result = conn.execute(text(shoes_query), {"marathon_id": marathon_id}).fetchone()
                total_shoes = shoes_result.total_shoes if shoes_result else 0
                
                # 3. Count persons with demographics directly
                demographics_query = """
                    SELECT COUNT(p.demographic_id) as total_persons
                    FROM person_demographics p
                    JOIN images i ON p.image_id = i.image_id  
                    WHERE i.marathon_id = :marathon_id
                """
                demographics_result = conn.execute(text(demographics_query), {"marathon_id": marathon_id}).fetchone()
                total_persons = demographics_result.total_persons if demographics_result else 0
                
                # 4. Get brand counts directly
                brand_counts_query = """
                    SELECT s.brand, COUNT(*) as count
                    FROM shoe_detections s
                    JOIN images i ON s.image_id = i.image_id
                    WHERE i.marathon_id = :marathon_id 
                    AND s.brand IS NOT NULL
                    GROUP BY s.brand
                    ORDER BY count DESC
                """
                brand_results = conn.execute(text(brand_counts_query), {"marathon_id": marathon_id}).fetchall()
                
                # Process brand counts
                brand_counts_dict = {row.brand: row.count for row in brand_results}
                unique_brands = len(brand_counts_dict)
                
                # Calculate leader brand
                leader_name = "N/A"
                leader_count = 0
                leader_percentage = 0.0
                
                if brand_counts_dict:
                    leader_name = max(brand_counts_dict, key=brand_counts_dict.get)
                    leader_count = brand_counts_dict[leader_name]
                    leader_percentage = (leader_count / total_shoes * 100) if total_shoes > 0 else 0.0
                
                # 5. Get gender distribution
                gender_query = """
                    SELECT p.gender_label, s.brand, COUNT(*) as count
                    FROM person_demographics p
                    JOIN images i ON p.image_id = i.image_id
                    JOIN shoe_detections s ON i.image_id = s.image_id
                    WHERE i.marathon_id = :marathon_id 
                    AND p.gender_label IS NOT NULL 
                    AND s.brand IS NOT NULL
                    GROUP BY p.gender_label, s.brand
                """
                gender_results = conn.execute(text(gender_query), {"marathon_id": marathon_id}).fetchall()
                
                # Process gender distribution
                gender_data = {}
                for row in gender_results:
                    if row.gender_label not in gender_data:
                        gender_data[row.gender_label] = {}
                    gender_data[row.gender_label][row.brand] = row.count
                
                # 6. Get race distribution  
                race_query = """
                    SELECT p.race_label, s.brand, COUNT(*) as count
                    FROM person_demographics p
                    JOIN images i ON p.image_id = i.image_id
                    JOIN shoe_detections s ON i.image_id = s.image_id
                    WHERE i.marathon_id = :marathon_id 
                    AND p.race_label IS NOT NULL 
                    AND s.brand IS NOT NULL
                    GROUP BY p.race_label, s.brand
                """
                race_results = conn.execute(text(race_query), {"marathon_id": marathon_id}).fetchall()
                
                # Process race distribution
                race_data = {}
                for row in race_results:
                    if row.race_label not in race_data:
                        race_data[row.race_label] = {}
                    race_data[row.race_label][row.brand] = row.count
                
                # 7. Get category distribution
                category_query = """
                    SELECT i.category, s.brand, COUNT(*) as count
                    FROM images i
                    JOIN shoe_detections s ON i.image_id = s.image_id
                    WHERE i.marathon_id = :marathon_id 
                    AND i.category IS NOT NULL 
                    AND s.brand IS NOT NULL
                    GROUP BY i.category, s.brand
                """
                category_results = conn.execute(text(category_query), {"marathon_id": marathon_id}).fetchall()
                
                # Process category distribution
                category_data = {}
                for row in category_results:
                    if row.category not in category_data:
                        category_data[row.category] = {}
                    category_data[row.category][row.brand] = row.count
                
                # Create top brands list
                top_brands_list = []
                for i, (brand, count) in enumerate(sorted(brand_counts_dict.items(), key=lambda x: x[1], reverse=True)[:10], 1):
                    percentage = (count / total_shoes * 100) if total_shoes > 0 else 0.0
                    top_brands_list.append({
                        '#': i,
                        'Marca': brand,
                        'Count': count,
                        'Participação (%)': round(percentage, 1)
                    })
                
                logger.info(f"Calculated metrics: images={total_images}, shoes={total_shoes}, persons={total_persons}, brands={unique_brands}")
                
                # Store the metrics
                stmt = insert(self.marathon_metrics).values(
                    marathon_id=marathon_id,
                    total_images=total_images,
                    total_shoes_detected=total_shoes,
                    total_persons_with_demographics=total_persons,
                    unique_brands_count=unique_brands,
                    leader_brand_name=leader_name,
                    leader_brand_count=leader_count,
                    leader_brand_percentage=leader_percentage,
                    brand_counts_json=json.dumps(brand_counts_dict),
                    gender_distribution_json=json.dumps(gender_data),
                    race_distribution_json=json.dumps(race_data),
                    category_distribution_json=json.dumps(category_data),
                    top_brands_json=json.dumps(top_brands_list),
                    last_calculated=datetime.now()
                )
                
                # Delete existing and insert new
                delete_existing = delete(self.marathon_metrics).where(self.marathon_metrics.c.marathon_id == marathon_id)
                conn.execute(delete_existing)
                conn.execute(stmt)
                conn.commit()
                
                logger.info(f"Successfully stored metrics for marathon {marathon_id}")
                
        except Exception as e:
            logger.error(f"Error calculating metrics for marathon {marathon_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())


    def get_precomputed_marathon_metrics(self, marathon_ids: List[int]) -> Dict[str, Any]:
        """Retrieve pre-computed metrics for selected marathons."""
        if not marathon_ids:
            return {"total_images_selected": 0, "total_shoes_detected": 0}

        try:
            with self.get_connection() as conn:
                # Use execute_query method for simpler parameter handling
                placeholders = ','.join(['?'] * len(marathon_ids))
                query = f"""
                    SELECT m.name as marathon_name, met.*
                    FROM marathon_metrics met
                    JOIN marathons m ON met.marathon_id = m.marathon_id
                    WHERE met.marathon_id IN ({placeholders})
                """
                # Convert to dict for named parameters
                params = {f'param_{i}': marathon_id for i, marathon_id in enumerate(marathon_ids)}
                named_placeholders = ','.join([f':param_{i}' for i in range(len(marathon_ids))])
                query = f"""
                    SELECT m.name as marathon_name, met.*
                    FROM marathon_metrics met
                    JOIN marathons m ON met.marathon_id = m.marathon_id
                    WHERE met.marathon_id IN ({named_placeholders})
                """
                result = conn.execute(text(query), params).fetchall()

                # Aggregate metrics across marathons
                total_images = sum(row.total_images for row in result)
                total_shoes = sum(row.total_shoes_detected for row in result)
                total_persons = sum(row.total_persons_with_demographics for row in result)

                # Combine brand counts from all marathons
                combined_brand_counts = pd.Series(dtype='int64')
                combined_gender_dist = pd.DataFrame()
                combined_race_dist = pd.DataFrame()
                combined_category_dist = pd.DataFrame()
                marathon_specific_data = {}

                for row in result:
                    marathon_name = row.marathon_name

                    # Store individual marathon data for cards
                    marathon_specific_data[marathon_name] = {
                        "images_count": row.total_images,
                        "shoes_count": row.total_shoes_detected,
                        "persons_count": row.total_persons_with_demographics
                    }

                    # Combine brand counts
                    if row.brand_counts_json and row.brand_counts_json != '{}':
                        brand_counts_dict = json.loads(row.brand_counts_json)
                        brand_counts = pd.Series(brand_counts_dict, dtype='int64')
                        combined_brand_counts = combined_brand_counts.add(brand_counts, fill_value=0)

                    # Combine gender distribution
                    if row.gender_distribution_json and row.gender_distribution_json != '{}':
                        gender_dist = pd.read_json(row.gender_distribution_json)
                        if combined_gender_dist.empty:
                            combined_gender_dist = gender_dist
                        else:
                            combined_gender_dist = combined_gender_dist.add(gender_dist, fill_value=0)

                    # Combine race distribution
                    if row.race_distribution_json and row.race_distribution_json != '{}':
                        race_dist = pd.read_json(row.race_distribution_json)
                        if combined_race_dist.empty:
                            combined_race_dist = race_dist
                        else:
                            combined_race_dist = combined_race_dist.add(race_dist, fill_value=0)

                    # Combine category distribution
                    if row.category_distribution_json and row.category_distribution_json != '{}':
                        category_dist = pd.read_json(row.category_distribution_json)
                        if combined_category_dist.empty:
                            combined_category_dist = category_dist
                        else:
                            combined_category_dist = combined_category_dist.add(category_dist, fill_value=0)

                # Calculate leader brand from combined data
                leader_name = "N/A"
                leader_count = 0
                leader_percentage = 0.0
                unique_brands = len(combined_brand_counts)

                if not combined_brand_counts.empty:
                    leader_name = combined_brand_counts.idxmax()
                    leader_count = int(combined_brand_counts.max())
                    leader_percentage = (leader_count / total_shoes * 100) if total_shoes > 0 else 0.0

                # Create top brands table
                top_brands_df = pd.DataFrame()
                if not combined_brand_counts.empty:
                    top_n = 10
                    top_brands_series = combined_brand_counts.head(top_n)
                    top_brands_df = pd.DataFrame({
                        'Marca': top_brands_series.index,
                        'Count': top_brands_series.values.astype(int)
                    })
                    top_brands_df['#'] = range(1, len(top_brands_df) + 1)
                    top_brands_df['Participação (%)'] = (top_brands_df['Count'] / total_shoes * 100).round(1) if total_shoes > 0 else 0.0
                    max_count = top_brands_df['Count'].max()
                    if pd.isna(max_count) or max_count == 0:
                        max_count = 1
                    top_brands_df['Gráfico'] = top_brands_df['Count'].apply(
                        lambda x: "█" * int(round((x / max_count) * 10)) if max_count > 0 and pd.notna(x) else ""
                    )
                    top_brands_df = top_brands_df[['#', 'Marca', 'Count', 'Participação (%)', 'Gráfico']]

                # Return combined metrics in the same format as process_queried_data_for_report
                return {
                    "total_images_selected": total_images,
                    "total_shoes_detected": total_shoes,
                    "unique_brands_count": unique_brands,
                    "brand_counts_all_selected": combined_brand_counts,
                    "top_brands_all_selected": top_brands_df,
                    "persons_analyzed_count": total_persons,
                    "leader_brand_info": {
                        "name": leader_name,
                        "count": leader_count,
                        "percentage": leader_percentage
                    },
                    "gender_brand_distribution": combined_gender_dist,
                    "race_brand_distribution": combined_race_dist,
                    "brand_counts_by_marathon": pd.DataFrame(),  # Not pre-computed for now
                    "brand_counts_by_category": combined_category_dist,
                    "total_persons_by_marathon": pd.Series(dtype='int'),
                    "marathon_specific_data_for_cards": marathon_specific_data,
                }

        except Exception as e:
            logger.error(f"Failed to get precomputed marathon metrics: {e}")
            # Fall back to real-time calculation
            df_flat, df_raw = self.get_data_for_selected_marathons_db(marathon_ids)
            from data_processing import process_queried_data_for_report
            return process_queried_data_for_report(df_flat, df_raw)

    def get_individual_marathon_metrics(self, marathon_ids: List[int]) -> Dict[str, Dict[str, Any]]:
        """Retrieve pre-computed metrics for individual marathons efficiently."""
        if not marathon_ids:
            return {}

        try:
            with self.get_connection() as conn:
                # Use execute_query method for simpler parameter handling
                params = {f'param_{i}': marathon_id for i, marathon_id in enumerate(marathon_ids)}
                named_placeholders = ','.join([f':param_{i}' for i in range(len(marathon_ids))])
                query = f"""
                    SELECT m.name as marathon_name, met.*
                    FROM marathon_metrics met
                    JOIN marathons m ON met.marathon_id = m.marathon_id
                    WHERE met.marathon_id IN ({named_placeholders})
                """
                result = conn.execute(text(query), params).fetchall()

                if not result:
                    # No pre-computed metrics found, fall back to real-time calculation
                    logger.warning("No pre-computed metrics found, falling back to real-time calculation")
                    individual_results = {}
                    for marathon_id in marathon_ids:
                        df_flat, df_raw = self.get_data_for_selected_marathons_db([marathon_id])
                        from data_processing import process_queried_data_for_report
                        marathon_list = self.get_marathon_list_from_db()
                        marathon_name = next((m['name'] for m in marathon_list if m['id'] == marathon_id), f"Marathon_{marathon_id}")
                        individual_results[marathon_name] = process_queried_data_for_report(df_flat, df_raw)
                    return individual_results

                # Process each marathon individually
                individual_results = {}

                for row in result:
                    marathon_name = row.marathon_name

                    # Parse individual marathon data
                    brand_counts = pd.Series(dtype='int64')
                    if row.brand_counts_json and row.brand_counts_json != '{}':
                        brand_counts_dict = json.loads(row.brand_counts_json)
                        brand_counts = pd.Series(brand_counts_dict, dtype='int64')

                    gender_dist = pd.DataFrame()
                    if row.gender_distribution_json and row.gender_distribution_json != '{}':
                        gender_dist = pd.read_json(row.gender_distribution_json)

                    race_dist = pd.DataFrame()
                    if row.race_distribution_json and row.race_distribution_json != '{}':
                        race_dist = pd.read_json(row.race_distribution_json)

                    category_dist = pd.DataFrame()
                    if row.category_distribution_json and row.category_distribution_json != '{}':
                        category_dist = pd.read_json(row.category_distribution_json)

                    # Create top brands table for this marathon
                    top_brands_df = pd.DataFrame()
                    if not brand_counts.empty:
                        top_n = 10
                        top_brands_series = brand_counts.head(top_n)
                        top_brands_df = pd.DataFrame({
                            'Marca': top_brands_series.index,
                            'Count': top_brands_series.values.astype(int)
                        })
                        top_brands_df['#'] = range(1, len(top_brands_df) + 1)
                        total_shoes = row.total_shoes_detected
                        top_brands_df['Participação (%)'] = (top_brands_df['Count'] / total_shoes * 100).round(1) if total_shoes > 0 else 0.0
                        max_count = top_brands_df['Count'].max()
                        if pd.isna(max_count) or max_count == 0:
                            max_count = 1
                        top_brands_df['Gráfico'] = top_brands_df['Count'].apply(
                            lambda x: "█" * int(round((x / max_count) * 10)) if max_count > 0 and pd.notna(x) else ""
                        )
                        top_brands_df = top_brands_df[['#', 'Marca', 'Count', 'Participação (%)', 'Gráfico']]

                    # Store individual marathon data
                    individual_results[marathon_name] = {
                        "total_images_selected": row.total_images,
                        "total_shoes_detected": row.total_shoes_detected,
                        "unique_brands_count": row.unique_brands_count,
                        "brand_counts_all_selected": brand_counts,
                        "top_brands_all_selected": top_brands_df,
                        "persons_analyzed_count": row.total_persons_with_demographics,
                        "leader_brand_info": {
                            "name": row.leader_brand_name or "N/A",
                            "count": row.leader_brand_count or 0,
                            "percentage": row.leader_brand_percentage or 0.0
                        },
                        "gender_brand_distribution": gender_dist,
                        "race_brand_distribution": race_dist,
                        "brand_counts_by_category": category_dist,
                        "brand_counts_by_marathon": pd.DataFrame(),  # Not needed for individual
                        "total_persons_by_marathon": pd.Series(dtype='int'),
                        "marathon_specific_data_for_cards": {
                            marathon_name: {
                                "images_count": row.total_images,
                                "shoes_count": row.total_shoes_detected,
                                "persons_count": row.total_persons_with_demographics
                            }
                        },
                    }

                return individual_results

        except Exception as e:
            logger.error(f"Failed to get individual marathon metrics: {e}")
            # Fall back to real-time calculation
            individual_results = {}
            for marathon_id in marathon_ids:
                df_flat, df_raw = self.get_data_for_selected_marathons_db([marathon_id])
                from data_processing import process_queried_data_for_report
                marathon_list = self.get_marathon_list_from_db()
                marathon_name = next((m['name'] for m in marathon_list if m['id'] == marathon_id), f"Marathon_{marathon_id}")
                individual_results[marathon_name] = process_queried_data_for_report(df_flat, df_raw)
            return individual_results

    def get_marathon_list_from_db(self) -> List[Dict]:
        """Get list of all marathons from the database."""
        try:
            with self.get_connection() as conn:
                stmt = select(
                    self.marathons.c.marathon_id,
                    self.marathons.c.name,
                    self.marathons.c.event_date,
                    self.marathons.c.location,
                    self.marathons.c.distance_km,
                    self.marathons.c.description
                ).order_by(self.marathons.c.event_date.desc(), self.marathons.c.name.asc())
                result = conn.execute(stmt).fetchall()
                
                return [
                    {
                        "id": row.marathon_id,
                        "name": row.name,
                        "event_date": row.event_date,
                        "location": row.location,
                        "distance_km": row.distance_km,
                        "description": row.description
                    }
                    for row in result
                ]
        except Exception as e:
            logger.error(f"Failed to get marathon list: {e}")
            return []

    def get_data_for_selected_marathons_db(self, marathon_ids_list: List[int]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Get data for selected marathons from the database."""
        if not marathon_ids_list:
            return pd.DataFrame(), pd.DataFrame()

        try:
            with self.get_connection() as conn:
                # Use SQLAlchemy's text() with named parameters for PostgreSQL compatibility
                from sqlalchemy import text
                
                # Create named parameters for the IN clause
                params = {f'marathon_id_{i}': marathon_id for i, marathon_id in enumerate(marathon_ids_list)}
                named_placeholders = ','.join([f':marathon_id_{i}' for i in range(len(marathon_ids_list))])

                # Query for flattened data (shoe & demographic per image)
                query_flat = f"""
                    SELECT
                        m.marathon_id,
                        m.name as marathon_name,
                        i.image_id,
                        i.filename,
                        i.category,
                        s.brand as shoe_brand,
                        s.probability as shoe_prob,
                        s.confidence as shoe_confidence,
                        p.gender_label as person_gender,
                        p.age_label as person_age,
                        p.race_label as person_race
                    FROM marathons m
                    JOIN images i ON m.marathon_id = i.marathon_id
                    LEFT JOIN shoe_detections s ON i.image_id = s.image_id
                    LEFT JOIN person_demographics p ON i.image_id = p.image_id
                    WHERE m.marathon_id IN ({named_placeholders})
                """
                logger.info(f"Executing query for flattened data: {query_flat} with params {params}")
                # Execute with named parameters
                result_flat = conn.execute(text(query_flat), params)
                df_flat_selected = pd.DataFrame(result_flat.fetchall(), columns=result_flat.keys())
                logger.info(f"Retrieved {len(df_flat_selected)} rows of flattened data")
                # Query for raw-like structure for counts
                query_raw_reconstructed = f"""
                    SELECT 
                        m.marathon_id,
                        m.name as marathon_name,
                        i.filename,
                        i.category,
                        i.original_width,
                        i.original_height,
                        (SELECT COUNT(*) FROM person_demographics pd WHERE pd.image_id = i.image_id) > 0 as has_demographics
                    FROM marathons m
                    JOIN images i ON m.marathon_id = i.marathon_id
                    WHERE m.marathon_id IN ({named_placeholders})
                """
                
                result_raw = conn.execute(text(query_raw_reconstructed), params)
                df_raw_reconstructed_for_counts = pd.DataFrame(result_raw.fetchall(), columns=result_raw.keys())

                return df_flat_selected, df_raw_reconstructed_for_counts
        except Exception as e:
            logger.error(f"Failed to get data for selected marathons: {e}")
            return pd.DataFrame(), pd.DataFrame()
# Global database manager instance
try:
    db = DatabaseManager()
except Exception as e:
    logger.error(f"Failed to initialize database manager: {e}")
    db = None


# Convenience functions for backward compatibility
def get_marathon_list_from_db():
    """Backward compatibility function."""
    if db is None:
        return []
    return db.get_marathon_list_from_db()


def get_data_for_selected_marathons_db(marathon_ids_list):
    """Backward compatibility function."""
    if db is None:
        return pd.DataFrame(), pd.DataFrame()
    return db.get_data_for_selected_marathons_db(marathon_ids_list)


def get_precomputed_marathon_metrics(marathon_ids):
    """Backward compatibility function."""
    if db is None:
        return {"total_images_selected": 0, "total_shoes_detected": 0}
    return db.get_precomputed_marathon_metrics(marathon_ids)


def get_individual_marathon_metrics(marathon_ids):
    """Backward compatibility function."""
    if db is None:
        return {}
    return db.get_individual_marathon_metrics(marathon_ids)


def add_marathon_metadata(name, event_date, location, distance_km, description, original_json_filename, user_id):
    """Backward compatibility function."""
    if db is None:
        return None
    return db.add_marathon_metadata(name, event_date, location, distance_km, description, original_json_filename, user_id)


def insert_parsed_json_data(marathon_id, parsed_json_data_list):
    """Backward compatibility function."""
    if db is None:
        return False
    return db.insert_parsed_json_data(marathon_id, parsed_json_data_list)


def delete_marathon_by_id(marathon_id):
    """Backward compatibility function."""
    if db is None:
        return False
    return db.delete_marathon_by_id(marathon_id)
