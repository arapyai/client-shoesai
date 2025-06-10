# migrate_to_abstracted_db.py
"""
Migration script to transition from the old SQLite-based database.py 
to the new database abstraction layer.

This script:
1. Backs up existing data
2. Creates new tables with the abstracted schema
3. Migrates data from old tables to new tables
4. Verifies data integrity
"""

import sqlite3
import pandas as pd
import json
from datetime import datetime
from typing import Dict, Any
import logging
from tqdm import tqdm

# Import both old and new database systems
from database_abstraction import db as new_db
import database as old_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseMigrator:
    """Handles migration from old database format to new abstracted format."""
    
    def __init__(self, batch_size: int = 1000):
        self.backup_data = {}
        self.batch_size = batch_size  # Number of records to process in each batch
    
    def backup_existing_data(self) -> bool:
        """Backup all existing data from the current database with progress indication."""
        try:
            logger.info("🔄 Backing up existing data...")
            
            # Get connection to old database
            conn = sqlite3.connect(old_db.DATABASE_NAME)
            conn.row_factory = sqlite3.Row
            
            # Backup all tables
            tables = ['Users', 'Marathons', 'MarathonMetrics', 'Images', 'ShoeDetections', 'PersonDemographics']
            
            for table in tqdm(tables, desc="Backing up tables"):
                try:
                    # First get count for progress indication
                    count_query = f"SELECT COUNT(*) as count FROM {table}"
                    count_result = conn.execute(count_query).fetchone()
                    table_count = count_result['count'] if count_result else 0
                    
                    if table_count > 0:
                        logger.info(f"📊 Backing up {table_count:,} records from {table}...")
                        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                        self.backup_data[table] = df
                        logger.info(f"✅ Backed up {len(df):,} records from {table}")
                    else:
                        logger.info(f"ℹ️ No records found in {table}")
                        self.backup_data[table] = pd.DataFrame()
                        
                except Exception as e:
                    logger.warning(f"⚠️ Could not backup {table}: {e}")
                    self.backup_data[table] = pd.DataFrame()
            
            conn.close()
            
            # Save backup to JSON files with progress
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            tables_to_backup = [(table, df) for table, df in self.backup_data.items() if not df.empty]
            
            if tables_to_backup:
                logger.info("💾 Saving backup files...")
                for table, df in tqdm(tables_to_backup, desc="Saving backups"):
                    backup_file = f"backup_{table.lower()}_{timestamp}.json"
                    df.to_json(backup_file, orient='records', indent=2)
                    logger.info(f"💾 Saved backup to {backup_file}")
            
            # Summary of backed up data
            total_records = sum(len(df) for df in self.backup_data.values())
            logger.info(f"✅ Backup completed successfully - {total_records:,} total records backed up")
            return True
            
        except Exception as e:
            logger.error(f"❌ Backup failed: {e}")
            return False
    
    def create_new_schema(self) -> bool:
        """Create new database schema using the abstraction layer."""
        try:
            logger.info("🔄 Creating new database schema...")
            new_db.create_tables()
            logger.info("✅ New schema created successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Schema creation failed: {e}")
            return False
    
    def migrate_users(self) -> bool:
        """Migrate users table with batch processing."""
        try:
            if 'Users' not in self.backup_data or self.backup_data['Users'].empty:
                logger.info("ℹ️ No users to migrate")
                return True
            
            logger.info("🔄 Migrating users...")
            users_df = self.backup_data['Users']
            
            # Process in batches
            for i in tqdm(range(0, len(users_df), self.batch_size), desc="Migrating users"):
                batch = users_df.iloc[i:i + self.batch_size]
                
                with new_db.get_connection() as conn:
                    trans = conn.begin()
                    try:
                        for _, user in batch.iterrows():
                            stmt = new_db.users.insert().values(
                                user_id=user['user_id'],
                                email=user['email'],
                                hashed_password=user['hashed_password'],
                                is_admin=bool(user['is_admin'])
                            )
                            conn.execute(stmt)
                        trans.commit()
                    except Exception as e:
                        trans.rollback()
                        logger.error(f"❌ Error in user batch {i//self.batch_size + 1}: {e}")
                        return False
            
            logger.info(f"✅ Migrated {len(users_df)} users")
            return True
            
        except Exception as e:
            logger.error(f"❌ User migration failed: {e}")
            return False
    
    def migrate_marathons(self) -> bool:
        """Migrate marathons table with batch processing."""
        try:
            if 'Marathons' not in self.backup_data or self.backup_data['Marathons'].empty:
                logger.info("ℹ️ No marathons to migrate")
                return True
            
            logger.info("🔄 Migrating marathons...")
            marathons_df = self.backup_data['Marathons']
            
            # Process in batches
            for i in tqdm(range(0, len(marathons_df), self.batch_size), desc="Migrating marathons"):
                batch = marathons_df.iloc[i:i + self.batch_size]
                
                with new_db.get_connection() as conn:
                    trans = conn.begin()
                    try:
                        for _, marathon in batch.iterrows():
                            stmt = new_db.marathons.insert().values(
                                marathon_id=marathon['marathon_id'],
                                name=marathon['name'],
                                event_date=marathon.get('event_date'),
                                location=marathon.get('location'),
                                distance_km=marathon.get('distance_km'),
                                description=marathon.get('description'),
                                original_json_filename=marathon.get('original_json_filename'),
                                uploaded_by_user_id=marathon.get('uploaded_by_user_id'),
                                upload_timestamp=pd.to_datetime(marathon.get('upload_timestamp', datetime.now()))
                            )
                            conn.execute(stmt)
                        trans.commit()
                    except Exception as e:
                        trans.rollback()
                        logger.error(f"❌ Error in marathon batch {i//self.batch_size + 1}: {e}")
                        return False
            
            logger.info(f"✅ Migrated {len(marathons_df)} marathons")
            return True
            
        except Exception as e:
            logger.error(f"❌ Marathon migration failed: {e}")
            return False
    
    def migrate_images(self) -> bool:
        """Migrate images table with batch processing and progress bar."""
        try:
            if 'Images' not in self.backup_data or self.backup_data['Images'].empty:
                logger.info("ℹ️ No images to migrate")
                return True
            
            images_df = self.backup_data['Images']
            total_images = len(images_df)
            logger.info(f"🔄 Migrating {total_images:,} images in batches of {self.batch_size}...")
            
            # Process in batches with progress bar
            with tqdm(total=total_images, desc="Migrating images", unit="images") as pbar:
                for i in range(0, len(images_df), self.batch_size):
                    batch = images_df.iloc[i:i + self.batch_size]
                    batch_size = len(batch)
                    
                    with new_db.get_connection() as conn:
                        trans = conn.begin()
                        try:
                            # Prepare batch insert data
                            batch_data = []
                            for _, image in batch.iterrows():
                                batch_data.append({
                                    'image_id': image['image_id'],
                                    'marathon_id': image['marathon_id'],
                                    'filename': image['filename'],
                                    'category': image.get('category'),
                                    'original_width': image.get('original_width'),
                                    'original_height': image.get('original_height')
                                })
                            
                            # Bulk insert
                            conn.execute(new_db.images.insert(), batch_data)
                            trans.commit()
                            
                            # Update progress bar
                            pbar.update(batch_size)
                            pbar.set_postfix({"Batch": f"{i//self.batch_size + 1}", "Processed": f"{min(i + self.batch_size, total_images):,}"})
                            
                        except Exception as e:
                            trans.rollback()
                            logger.error(f"❌ Error in image batch {i//self.batch_size + 1}: {e}")
                            pbar.close()
                            return False
            
            logger.info(f"✅ Migrated {total_images:,} images successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Image migration failed: {e}")
            return False
    
    def migrate_shoe_detections(self) -> bool:
        """Migrate shoe detections table with batch processing and progress bar."""
        try:
            if 'ShoeDetections' not in self.backup_data or self.backup_data['ShoeDetections'].empty:
                logger.info("ℹ️ No shoe detections to migrate")
                return True
            
            detections_df = self.backup_data['ShoeDetections']
            total_detections = len(detections_df)
            logger.info(f"🔄 Migrating {total_detections:,} shoe detections in batches of {self.batch_size}...")
            
            # Process in batches with progress bar
            with tqdm(total=total_detections, desc="Migrating shoe detections", unit="detections") as pbar:
                for i in range(0, len(detections_df), self.batch_size):
                    batch = detections_df.iloc[i:i + self.batch_size]
                    batch_size = len(batch)
                    
                    with new_db.get_connection() as conn:
                        trans = conn.begin()
                        try:
                            # Prepare batch insert data
                            batch_data = []
                            for _, detection in batch.iterrows():
                                batch_data.append({
                                    'detection_id': detection['detection_id'],
                                    'image_id': detection['image_id'],
                                    'brand': detection.get('brand'),
                                    'probability': detection.get('probability'),
                                    'confidence': detection.get('confidence'),
                                    'bbox_x1': detection.get('bbox_x1'),
                                    'bbox_y1': detection.get('bbox_y1'),
                                    'bbox_x2': detection.get('bbox_x2'),
                                    'bbox_y2': detection.get('bbox_y2')
                                })
                            
                            # Bulk insert
                            conn.execute(new_db.shoe_detections.insert(), batch_data)
                            trans.commit()
                            
                            # Update progress bar
                            pbar.update(batch_size)
                            pbar.set_postfix({"Batch": f"{i//self.batch_size + 1}", "Processed": f"{min(i + self.batch_size, total_detections):,}"})
                            
                        except Exception as e:
                            trans.rollback()
                            logger.error(f"❌ Error in shoe detection batch {i//self.batch_size + 1}: {e}")
                            pbar.close()
                            return False
            
            logger.info(f"✅ Migrated {total_detections:,} shoe detections successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Shoe detection migration failed: {e}")
            return False
    
    def migrate_person_demographics(self) -> bool:
        """Migrate person demographics table with batch processing and progress bar."""
        try:
            if 'PersonDemographics' not in self.backup_data or self.backup_data['PersonDemographics'].empty:
                logger.info("ℹ️ No person demographics to migrate")
                return True
            
            demographics_df = self.backup_data['PersonDemographics']
            total_demographics = len(demographics_df)
            logger.info(f"🔄 Migrating {total_demographics:,} person demographics in batches of {self.batch_size}...")
            
            # Process in batches with progress bar
            with tqdm(total=total_demographics, desc="Migrating demographics", unit="records") as pbar:
                for i in range(0, len(demographics_df), self.batch_size):
                    batch = demographics_df.iloc[i:i + self.batch_size]
                    batch_size = len(batch)
                    
                    with new_db.get_connection() as conn:
                        trans = conn.begin()
                        try:
                            # Prepare batch insert data
                            batch_data = []
                            for _, demo in batch.iterrows():
                                batch_data.append({
                                    'demographic_id': demo['demographic_id'],
                                    'image_id': demo['image_id'],
                                    'gender_label': demo.get('gender_label'),
                                    'gender_prob': demo.get('gender_prob'),
                                    'age_label': demo.get('age_label'),
                                    'age_prob': demo.get('age_prob'),
                                    'race_label': demo.get('race_label'),
                                    'race_prob': demo.get('race_prob'),
                                    'person_bbox_x1': demo.get('person_bbox_x1'),
                                    'person_bbox_y1': demo.get('person_bbox_y1'),
                                    'person_bbox_x2': demo.get('person_bbox_x2'),
                                    'person_bbox_y2': demo.get('person_bbox_y2')
                                })
                            
                            # Bulk insert
                            conn.execute(new_db.person_demographics.insert(), batch_data)
                            trans.commit()
                            
                            # Update progress bar
                            pbar.update(batch_size)
                            pbar.set_postfix({"Batch": f"{i//self.batch_size + 1}", "Processed": f"{min(i + self.batch_size, total_demographics):,}"})
                            
                        except Exception as e:
                            trans.rollback()
                            logger.error(f"❌ Error in demographics batch {i//self.batch_size + 1}: {e}")
                            pbar.close()
                            return False
            
            logger.info(f"✅ Migrated {total_demographics:,} person demographics successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Person demographics migration failed: {e}")
            return False
    
    def migrate_marathon_metrics(self) -> bool:
        """Migrate marathon metrics table with batch processing."""
        try:
            if 'MarathonMetrics' not in self.backup_data or self.backup_data['MarathonMetrics'].empty:
                logger.info("ℹ️ No marathon metrics to migrate")
                return True
            
            logger.info("🔄 Migrating marathon metrics...")
            metrics_df = self.backup_data['MarathonMetrics']
            
            # Process in batches
            for i in tqdm(range(0, len(metrics_df), self.batch_size), desc="Migrating metrics"):
                batch = metrics_df.iloc[i:i + self.batch_size]
                
                with new_db.get_connection() as conn:
                    trans = conn.begin()
                    try:
                        batch_data = []
                        for _, metric in batch.iterrows():
                            batch_data.append({
                                'metric_id': metric['metric_id'],
                                'marathon_id': metric['marathon_id'],
                                'total_images': metric.get('total_images', 0),
                                'total_shoes_detected': metric.get('total_shoes_detected', 0),
                                'total_persons_with_demographics': metric.get('total_persons_with_demographics', 0),
                                'unique_brands_count': metric.get('unique_brands_count', 0),
                                'leader_brand_name': metric.get('leader_brand_name'),
                                'leader_brand_count': metric.get('leader_brand_count', 0),
                                'leader_brand_percentage': metric.get('leader_brand_percentage', 0.0),
                                'brand_counts_json': metric.get('brand_counts_json'),
                                'gender_distribution_json': metric.get('gender_distribution_json'),
                                'race_distribution_json': metric.get('race_distribution_json'),
                                'category_distribution_json': metric.get('category_distribution_json'),
                                'top_brands_json': metric.get('top_brands_json'),
                                'last_calculated': pd.to_datetime(metric.get('last_calculated', datetime.now()))
                            })
                        
                        # Bulk insert
                        conn.execute(new_db.marathon_metrics.insert(), batch_data)
                        trans.commit()
                        
                    except Exception as e:
                        trans.rollback()
                        logger.error(f"❌ Error in metrics batch {i//self.batch_size + 1}: {e}")
                        return False
            
            logger.info(f"✅ Migrated {len(metrics_df)} marathon metrics")
            return True
            
        except Exception as e:
            logger.error(f"❌ Marathon metrics migration failed: {e}")
            return False
    
    def verify_migration(self) -> bool:
        """Verify that migration was successful by comparing record counts."""
        try:
            logger.info("🔄 Verifying migration...")
            
            with new_db.get_connection() as conn:
                # Check record counts
                table_mappings = {
                    'Users': new_db.users,
                    'Marathons': new_db.marathons,
                    'Images': new_db.images,
                    'ShoeDetections': new_db.shoe_detections,
                    'PersonDemographics': new_db.person_demographics,
                    'MarathonMetrics': new_db.marathon_metrics
                }
                
                for old_name, new_table in table_mappings.items():
                    old_count = len(self.backup_data.get(old_name, pd.DataFrame()))
                    
                    from sqlalchemy import func, select
                    new_count_result = conn.execute(select(func.count()).select_from(new_table))
                    new_count = new_count_result.scalar()
                    
                    if old_count == new_count:
                        logger.info(f"✅ {old_name}: {old_count} → {new_count} records")
                    else:
                        logger.warning(f"⚠️ {old_name}: {old_count} → {new_count} records (mismatch)")
            
            logger.info("✅ Migration verification completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Migration verification failed: {e}")
            return False
    
    def run_migration(self) -> bool:
        """Run the complete migration process."""
        logger.info("🚀 Starting database migration...")
        
        steps = [
            ("Backup existing data", self.backup_existing_data),
            ("Create new schema", self.create_new_schema),
            ("Migrate users", self.migrate_users),
            ("Migrate marathons", self.migrate_marathons),
            ("Migrate images", self.migrate_images),
            ("Migrate shoe detections", self.migrate_shoe_detections),
            ("Migrate person demographics", self.migrate_person_demographics),
            ("Migrate marathon metrics", self.migrate_marathon_metrics),
            ("Verify migration", self.verify_migration)
        ]
        
        for step_name, step_func in steps:
            logger.info(f"📋 {step_name}...")
            if not step_func():
                logger.error(f"❌ Migration failed at step: {step_name}")
                return False
        
        logger.info("🎉 Migration completed successfully!")
        logger.info("💡 You can now update your imports to use the new database abstraction layer")
        return True


def main():
    """Main migration function."""
    print("=" * 60)
    print("🔄 DATABASE MIGRATION TOOL")
    print("=" * 60)
    print("This tool will migrate your existing SQLite database")
    print("to the new abstracted database layer.")
    print()
    
    # Allow customization of batch size
    print("Configuration options:")
    batch_size_input = input("Batch size for large tables (default: 1000): ").strip()
    try:
        batch_size = int(batch_size_input) if batch_size_input else 1000
        if batch_size < 1:
            batch_size = 1000
    except ValueError:
        batch_size = 1000
    
    print(f"Using batch size: {batch_size}")
    print()
    
    response = input("Do you want to proceed with migration? (y/N): ").strip().lower()
    if response != 'y':
        print("Migration cancelled.")
        return
    
    # Initialize migrator with custom batch size
    migrator = DatabaseMigrator(batch_size=batch_size)
    success = migrator.run_migration()
    
    if success:
        print("\n" + "=" * 60)
        print("✅ MIGRATION COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("Next steps:")
        print("1. Test your application with the new database layer")
        print("2. Update your imports to use database_abstraction instead of database")
        print("3. Consider setting up environment variables for different database providers")
        print("4. Backup files have been created for safety")
    else:
        print("\n" + "=" * 60)
        print("❌ MIGRATION FAILED!")
        print("=" * 60)
        print("Check the logs above for error details.")
        print("Your original database remains unchanged.")


if __name__ == "__main__":
    main()
