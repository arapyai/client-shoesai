#!/usr/bin/env python3
"""
Script to fix PostgreSQL sequences when they get out of sync.
This can happen when data is imported or manipulated outside of the normal flow.
"""

import logging
from database_abstraction import DatabaseManager
from sqlalchemy import text

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_postgre_sequences(db):
    """Fix auto-increment sequences to match the current max IDs."""
    with db.get_connection() as conn:
        # Check if we're using PostgreSQL
        try:
            max_image_id_result = conn.execute(text("SELECT COALESCE(MAX(image_id), 0) FROM images")).fetchone()
            if max_image_id_result:
                max_image_id = max_image_id_result[0]
                if max_image_id > 0:
                    conn.execute(text(f"SELECT setval('images_image_id_seq', {max_image_id}, true)"))
                    logger.info(f"Fixed images sequence to start from {max_image_id + 1}")
        except Exception as e:
            logger.warning(f"Could not fix images sequence: {e}")
        
        # Fix other sequences as needed
        tables_sequences = [
            ('marathons', 'marathons_marathon_id_seq', 'marathon_id'),
            ('users', 'users_user_id_seq', 'user_id'),
            ('marathon_metrics', 'marathon_metrics_metric_id_seq', 'metric_id'),
            ('shoe_detections', 'shoe_detections_detection_id_seq', 'detection_id'),
            ('person_demographics', 'person_demographics_demographic_id_seq', 'demographic_id')
        ]
        
        for table_name, sequence_name, id_column in tables_sequences:
            try:
                max_id_result = conn.execute(text(f"SELECT COALESCE(MAX({id_column}), 0) FROM {table_name}")).fetchone()
                if max_id_result:
                    max_id = max_id_result[0]
                    if max_id > 0:
                        conn.execute(text(f"SELECT setval('{sequence_name}', {max_id}, true)"))
                        logger.debug(f"Fixed {sequence_name} to start from {max_id + 1}")
            except Exception as seq_error:
                logger.warning(f"Could not fix sequence {sequence_name}: {seq_error}")
        
        conn.commit()

def main():
    """Fix PostgreSQL sequences."""
    db = DatabaseManager()
    fix_postgre_sequences(db)

    return 0

if __name__ == "__main__":
    exit(main())
