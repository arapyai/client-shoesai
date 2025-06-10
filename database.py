# database.py
import sqlite3
import pandas as pd
import json
from passlib.hash import pbkdf2_sha256 as hasher
from datetime import datetime
from typing import List, Dict, Any

DATABASE_NAME = "courtshoes_data.db"

def get_db_connection():
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        hashed_password TEXT NOT NULL,
        is_admin BOOLEAN DEFAULT 0
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Marathons (
        marathon_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        event_date TEXT,
        location TEXT,
        distance_km REAL,
        description TEXT,
        original_json_filename TEXT,
        uploaded_by_user_id INTEGER,
        upload_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (uploaded_by_user_id) REFERENCES Users(user_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS MarathonMetrics (
        metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
        marathon_id INTEGER NOT NULL UNIQUE,
        total_images INTEGER DEFAULT 0,
        total_shoes_detected INTEGER DEFAULT 0,
        total_persons_with_demographics INTEGER DEFAULT 0,
        unique_brands_count INTEGER DEFAULT 0,
        leader_brand_name TEXT,
        leader_brand_count INTEGER DEFAULT 0,
        leader_brand_percentage REAL DEFAULT 0.0,
        brand_counts_json TEXT, -- JSON string of brand counts
        gender_distribution_json TEXT, -- JSON string of gender breakdown by brand
        race_distribution_json TEXT, -- JSON string of race breakdown by brand
        category_distribution_json TEXT, -- JSON string of category breakdown by brand
        top_brands_json TEXT, -- JSON string of top brands table data
        last_calculated DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (marathon_id) REFERENCES Marathons(marathon_id) ON DELETE CASCADE
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Images (
        image_id INTEGER PRIMARY KEY AUTOINCREMENT,
        marathon_id INTEGER NOT NULL,
        filename TEXT NOT NULL,
        category TEXT, -- This can be used to store the folder or label
        original_width INTEGER,
        original_height INTEGER,
        FOREIGN KEY (marathon_id) REFERENCES Marathons(marathon_id),
        UNIQUE (marathon_id, filename)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ShoeDetections (
        detection_id INTEGER PRIMARY KEY AUTOINCREMENT,
        image_id INTEGER NOT NULL,
        brand TEXT,
        probability REAL,
        confidence REAL,
        bbox_x1 REAL, bbox_y1 REAL, bbox_x2 REAL, bbox_y2 REAL,
        FOREIGN KEY (image_id) REFERENCES Images(image_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS PersonDemographics (
        demographic_id INTEGER PRIMARY KEY AUTOINCREMENT,
        image_id INTEGER NOT NULL UNIQUE, -- Ensures only one demographic entry per image
        gender_label TEXT, gender_prob REAL,
        age_label TEXT, age_prob REAL,
        race_label TEXT, race_prob REAL,
        person_bbox_x1 REAL, person_bbox_y1 REAL, 
        person_bbox_x2 REAL, person_bbox_y2 REAL,
        FOREIGN KEY (image_id) REFERENCES Images(image_id)
    )
    """)
    conn.commit()
    conn.close()

def add_user(email, password, is_admin=False):
    conn = get_db_connection()
    cursor = conn.cursor()
    hashed_password = hasher.hash(password)
    try:
        cursor.execute("INSERT INTO Users (email, hashed_password, is_admin) VALUES (?, ?, ?)",
                       (email, hashed_password, is_admin))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def verify_user(email, password):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, email, hashed_password, is_admin FROM Users WHERE email = ?", (email,))
    user_record = cursor.fetchone()
    conn.close()
    if user_record and hasher.verify(password, user_record['hashed_password']):
        return {"user_id": user_record["user_id"], "email": user_record["email"], "is_admin": bool(user_record["is_admin"])}
    return None

def update_user_email(user_id, new_email):
    """
    Updates a user's email address.
    
    Args:
        user_id (int): The ID of the user to update
        new_email (str): The new email address
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE Users SET email = ? WHERE user_id = ?", (new_email, user_id))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def update_user_password(user_id, new_password):
    """
    Updates a user's password.
    
    Args:
        user_id (int): The ID of the user to update
        new_password (str): The new password (will be hashed)
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    hashed_password = hasher.hash(new_password)
    try:
        cursor.execute("UPDATE Users SET hashed_password = ? WHERE user_id = ?", (hashed_password, user_id))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()

def add_marathon_metadata(name, event_date, location, distance_km, description, original_json_filename, user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO Marathons (name, event_date, location, distance_km, description, original_json_filename, uploaded_by_user_id, upload_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, event_date, location, distance_km, description, original_json_filename, user_id, datetime.now()))
        marathon_id = cursor.lastrowid
        conn.commit()
        return marathon_id
    except sqlite3.IntegrityError:
        print(f"Error: Marathon with name '{name}' likely already exists.")
        return None
    finally:
        conn.close()

def insert_parsed_json_data(marathon_id, parsed_json_data_list):
    """
    Inserts data parsed from a JSON file (list of image records) into the database.
    Handles duplicate filenames by inserting the image once and linking detections.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    image_id_cache = {} # To store image_id for (marathon_id, filename)

    for image_record_dict in parsed_json_data_list:
        img_category = image_record_dict.get('folder') #trocar por label depois
        img_filename = image_record_dict.get('filename')
        if not img_filename:
            print("Skipping record with no filename.")
            continue

        image_key = (marathon_id, img_filename)
        current_image_id = None

        if image_key in image_id_cache:
            current_image_id = image_id_cache[image_key]
        else:
            try:
                cursor.execute("""
                    INSERT INTO Images (marathon_id, filename, original_width, original_height, category)
                    VALUES (?, ?, ?, ?, ?)
                """, (marathon_id, img_filename, image_record_dict.get('original_width'), image_record_dict.get('original_height'), img_category))
                current_image_id = cursor.lastrowid
                image_id_cache[image_key] = current_image_id
            except sqlite3.IntegrityError: # Should not happen if cache logic is correct, but as safeguard
                # Fetch existing image_id if insert failed due to unique constraint (race condition or pre-existing)
                cursor.execute("SELECT image_id FROM Images WHERE marathon_id = ? AND filename = ?", image_key)
                existing_image = cursor.fetchone()
                if existing_image:
                    current_image_id = existing_image['image_id']
                    image_id_cache[image_key] = current_image_id
                else:
                    print(f"Error: Could not insert or find image for {img_filename} in marathon {marathon_id}.")
                    conn.rollback() # Rollback this specific image record's attempt
                    continue # Skip to next record
            except Exception as e_img:
                print(f"Error inserting image {img_filename}: {e_img}")
                conn.rollback()
                continue
        
        if current_image_id is None:
            continue # Should not happen if logic above is correct

        # Insert Shoe Detections (can be multiple per image_id)
        shoes = image_record_dict.get('shoes', [])
        if isinstance(shoes, list):
            for shoe in shoes:
                if isinstance(shoe, dict):
                    # Safe extraction of shoe data with proper null checks
                    label_data = shoe.get('label')
                    brand = label_data[0] if isinstance(label_data, list) and len(label_data) > 0 else None
                    
                    prob_data = shoe.get('prob') 
                    prob = prob_data[0] if isinstance(prob_data, list) and len(prob_data) > 0 else None
                    
                    bbox_data = shoe.get('bbox')
                    bbox_list = bbox_data[0] if isinstance(bbox_data, list) and len(bbox_data) > 0 else [None]*4
                    
                    confidence = shoe.get('confidence')
                    try:
                        cursor.execute("""
                            INSERT INTO ShoeDetections (image_id, brand, probability, confidence, 
                                                        bbox_x1, bbox_y1, bbox_x2, bbox_y2)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (current_image_id, brand, prob, confidence, 
                              bbox_list[0], bbox_list[1], bbox_list[2], bbox_list[3]))
                    except Exception as e_shoe:
                        print(f"Error inserting shoe for image_id {current_image_id}: {e_shoe}")
                        # Decide if you want to rollback the whole image or just skip the shoe
                        conn.rollback() # For safety, rolling back this sub-transaction for the shoe
                        break # Break from shoe loop for this image_record if a shoe fails


        # Insert Person Demographics (only once per image_id due to UNIQUE constraint)
        demographic = image_record_dict.get('demographic')
        if isinstance(demographic, dict):
            gender_label = demographic.get('gender', {}).get('label')
            gender_prob = demographic.get('gender', {}).get('prob')
            age_label = demographic.get('age', {}).get('label')
            age_prob = demographic.get('age', {}).get('prob')
            race_label = demographic.get('race', {}).get('label')
            race_prob = demographic.get('race', {}).get('prob')
            person_bbox_list = demographic.get('bbox', [None]*4)

            try:
                cursor.execute("""
                    INSERT INTO PersonDemographics (image_id, gender_label, gender_prob, age_label, age_prob, 
                                                    race_label, race_prob, person_bbox_x1, person_bbox_y1, 
                                                    person_bbox_x2, person_bbox_y2)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (current_image_id, gender_label, gender_prob, age_label, age_prob,
                      race_label, race_prob, person_bbox_list[0], person_bbox_list[1],
                      person_bbox_list[2], person_bbox_list[3]))
            except sqlite3.IntegrityError as e_demo_unique:
                # This is expected if demographic data for this image_id was already inserted
                # from a previous row in the JSON that pointed to the same original image.
                # print(f"Demographic data for image_id {current_image_id} (file: {img_filename}) already exists. Skipping.")
                pass
            except Exception as e_demo:
                print(f"Error inserting demographic for image_id {current_image_id}: {e_demo}")
                conn.rollback() # Rollback this sub-transaction for the demographic
                # Potentially continue to next image_record or break this image_record processing

    conn.commit()
    conn.close()
    
    # Calculate and store pre-computed metrics after successful import
    print(f"🔄 Calculating metrics for marathon {marathon_id}...")
    calculate_and_store_marathon_metrics(marathon_id)


def get_marathon_list_from_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT marathon_id, name, event_date, location FROM Marathons ORDER BY event_date DESC, name ASC")
    marathons = [{"id": row['marathon_id'], "name": row['name'], 
                  "event_date": row['event_date'], "location": row['location']} 
                 for row in cursor.fetchall()]
    conn.close()
    return marathons

def get_data_for_selected_marathons_db(marathon_ids_list):
    if not marathon_ids_list:
        return pd.DataFrame(), pd.DataFrame()

    conn = get_db_connection()
    placeholders = ','.join(['?'] * len(marathon_ids_list))

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
        FROM Marathons m
        JOIN Images i ON m.marathon_id = i.marathon_id
        LEFT JOIN ShoeDetections s ON i.image_id = s.image_id  -- LEFT JOIN to include images with no shoes
        LEFT JOIN PersonDemographics p ON i.image_id = p.image_id -- LEFT JOIN to include images with no demographics
        WHERE m.marathon_id IN ({placeholders})
    """
    df_flat_selected = pd.read_sql_query(query_flat, conn, params=marathon_ids_list)

    # Query for raw-like structure for counts
    query_raw_reconstructed = f"""
        SELECT 
            m.marathon_id,
            m.name as marathon_name,
            i.filename,
            i.category,
            i.original_width,
            i.original_height,
            (SELECT COUNT(*) FROM PersonDemographics pd WHERE pd.image_id = i.image_id) > 0 as has_demographics
        FROM Marathons m
        JOIN Images i ON m.marathon_id = i.marathon_id
        WHERE m.marathon_id IN ({placeholders})
    """
    df_raw_reconstructed_for_counts = pd.read_sql_query(query_raw_reconstructed, conn, params=marathon_ids_list)
    
    conn.close()
    return df_flat_selected, df_raw_reconstructed_for_counts

def calculate_and_store_marathon_metrics(marathon_id: int) -> None:
    """
    Calculate and store pre-computed metrics for a marathon.
    This function should be called after importing marathon data.
    
    Args:
        marathon_id: The ID of the marathon to calculate metrics for
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get all data for this marathon
        df_flat, df_raw = get_data_for_selected_marathons_db([marathon_id])
        
        if df_flat.empty and df_raw.empty:
            # Store empty metrics
            cursor.execute("""
                INSERT OR REPLACE INTO MarathonMetrics 
                (marathon_id, total_images, total_shoes_detected, total_persons_with_demographics,
                 unique_brands_count, leader_brand_name, leader_brand_count, leader_brand_percentage,
                 brand_counts_json, gender_distribution_json, race_distribution_json, category_distribution_json, top_brands_json)
                VALUES (?, 0, 0, 0, 0, 'N/A', 0, 0.0, '{}', '{}', '{}', '{}', '[]')
            """, (marathon_id,))
            conn.commit()
            return
        
        # Import the processing function
        from data_processing import process_queried_data_for_report
        
        # Calculate metrics using existing function
        metrics = process_queried_data_for_report(df_flat, df_raw)
        
        # Extract key metrics
        total_images = metrics.get("total_images_selected", 0)
        total_shoes = metrics.get("total_shoes_detected", 0)
        total_persons = metrics.get("persons_analyzed_count", 0)
        unique_brands = metrics.get("unique_brands_count", 0)
        
        leader_info = metrics.get("leader_brand_info", {})
        leader_name = leader_info.get("name", "N/A")
        leader_count = leader_info.get("count", 0)
        leader_percentage = leader_info.get("percentage", 0.0)
        
        # Convert complex data to JSON strings
        brand_counts_json = metrics["brand_counts_all_selected"].to_json() if not metrics["brand_counts_all_selected"].empty else "{}"
        
        gender_dist_json = "{}"
        if not metrics["gender_brand_distribution"].empty:
            gender_dist_json = metrics["gender_brand_distribution"].to_json()
        
        race_dist_json = "{}"
        if not metrics["race_brand_distribution"].empty:
            race_dist_json = metrics["race_brand_distribution"].to_json()
        
        category_dist_json = "{}"
        if not metrics["brand_counts_by_category"].empty:
            category_dist_json = metrics["brand_counts_by_category"].to_json()
        
        top_brands_json = "[]"
        if not metrics["top_brands_all_selected"].empty:
            top_brands_json = metrics["top_brands_all_selected"].to_json(orient='records')
        
        # Store calculated metrics
        cursor.execute("""
            INSERT OR REPLACE INTO MarathonMetrics 
            (marathon_id, total_images, total_shoes_detected, total_persons_with_demographics,
             unique_brands_count, leader_brand_name, leader_brand_count, leader_brand_percentage,
             brand_counts_json, gender_distribution_json, race_distribution_json, category_distribution_json, top_brands_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (marathon_id, total_images, total_shoes, total_persons, unique_brands,
              leader_name, leader_count, leader_percentage,
              brand_counts_json, gender_dist_json, race_dist_json, category_dist_json, top_brands_json))
        
        conn.commit()
        print(f"✅ Calculated and stored metrics for marathon {marathon_id}")
        
    except Exception as e:
        print(f"❌ Error calculating metrics for marathon {marathon_id}: {e}")
        conn.rollback()
    finally:
        conn.close()


def get_precomputed_marathon_metrics(marathon_ids: List[int]) -> Dict[str, Any]:
    """
    Retrieve pre-computed metrics for selected marathons.
    
    Args:
        marathon_ids: List of marathon IDs
        
    Returns:
        Dictionary with combined metrics for all selected marathons
    """
    if not marathon_ids:
        return {"total_images_selected": 0, "total_shoes_detected": 0}
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get metrics for all selected marathons
        placeholders = ','.join(['?'] * len(marathon_ids))
        cursor.execute(f"""
            SELECT m.name as marathon_name, met.*
            FROM MarathonMetrics met
            JOIN Marathons m ON met.marathon_id = m.marathon_id
            WHERE met.marathon_id IN ({placeholders})
        """, marathon_ids)
        
        metrics_rows = cursor.fetchall()
        
        if not metrics_rows:
            # No pre-computed metrics found, fall back to real-time calculation
            print("⚠️ No pre-computed metrics found, falling back to real-time calculation")
            df_flat, df_raw = get_data_for_selected_marathons_db(marathon_ids)
            from data_processing import process_queried_data_for_report
            return process_queried_data_for_report(df_flat, df_raw)
        
        # Aggregate metrics across marathons
        total_images = sum(row['total_images'] for row in metrics_rows)
        total_shoes = sum(row['total_shoes_detected'] for row in metrics_rows)
        total_persons = sum(row['total_persons_with_demographics'] for row in metrics_rows)
        
        # Combine brand counts from all marathons
        combined_brand_counts = pd.Series(dtype='int64')
        combined_gender_dist = pd.DataFrame()
        combined_race_dist = pd.DataFrame()
        combined_category_dist = pd.DataFrame()
        marathon_specific_data = {}
        
        for row in metrics_rows:
            marathon_name = row['marathon_name']
            
            # Store individual marathon data for cards
            marathon_specific_data[marathon_name] = {
                "images_count": row['total_images'],
                "shoes_count": row['total_shoes_detected'],
                "persons_count": row['total_persons_with_demographics']
            }
            
            # Combine brand counts
            if row['brand_counts_json'] and row['brand_counts_json'] != '{}':
                brand_counts = pd.read_json(row['brand_counts_json'], typ='series')
                combined_brand_counts = combined_brand_counts.add(brand_counts, fill_value=0)
            
            # Combine gender distribution
            if row['gender_distribution_json'] and row['gender_distribution_json'] != '{}':
                gender_dist = pd.read_json(row['gender_distribution_json'])
                if combined_gender_dist.empty:
                    combined_gender_dist = gender_dist
                else:
                    combined_gender_dist = combined_gender_dist.add(gender_dist, fill_value=0)
            
            # Combine race distribution
            if row['race_distribution_json'] and row['race_distribution_json'] != '{}':
                race_dist = pd.read_json(row['race_distribution_json'])
                if combined_race_dist.empty:
                    combined_race_dist = race_dist
                else:
                    combined_race_dist = combined_race_dist.add(race_dist, fill_value=0)
            
            # Combine category distribution
            if row['category_distribution_json'] and row['category_distribution_json'] != '{}':
                category_dist = pd.read_json(row['category_distribution_json'])
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
        print(f"❌ Error retrieving pre-computed metrics: {e}")
        # Fall back to real-time calculation
        df_flat, df_raw = get_data_for_selected_marathons_db(marathon_ids)
        from data_processing import process_queried_data_for_report
        return process_queried_data_for_report(df_flat, df_raw)
    finally:
        conn.close()

def get_individual_marathon_metrics(marathon_ids: List[int]) -> Dict[str, Dict[str, Any]]:
    """
    Retrieve pre-computed metrics for individual marathons efficiently.
    
    Args:
        marathon_ids: List of marathon IDs
        
    Returns:
        Dictionary mapping marathon_name -> individual_metrics
    """
    if not marathon_ids:
        return {}
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get metrics for all selected marathons
        placeholders = ','.join(['?'] * len(marathon_ids))
        cursor.execute(f"""
            SELECT m.name as marathon_name, met.*
            FROM MarathonMetrics met
            JOIN Marathons m ON met.marathon_id = m.marathon_id
            WHERE met.marathon_id IN ({placeholders})
        """, marathon_ids)
        
        metrics_rows = cursor.fetchall()
        
        if not metrics_rows:
            # No pre-computed metrics found, fall back to real-time calculation
            print("⚠️ No pre-computed metrics found, falling back to real-time calculation")
            individual_results = {}
            for marathon_id in marathon_ids:
                df_flat, df_raw = get_data_for_selected_marathons_db([marathon_id])
                from data_processing import process_queried_data_for_report
                marathon_name = next((m['name'] for m in get_marathon_list_from_db() if m['id'] == marathon_id), f"Marathon_{marathon_id}")
                individual_results[marathon_name] = process_queried_data_for_report(df_flat, df_raw)
            return individual_results
        
        # Process each marathon individually
        individual_results = {}
        
        for row in metrics_rows:
            marathon_name = row['marathon_name']
            
            # Parse individual marathon data
            brand_counts = pd.Series(dtype='int64')
            if row['brand_counts_json'] and row['brand_counts_json'] != '{}':
                brand_counts = pd.read_json(row['brand_counts_json'], typ='series')
            
            gender_dist = pd.DataFrame()
            if row['gender_distribution_json'] and row['gender_distribution_json'] != '{}':
                gender_dist = pd.read_json(row['gender_distribution_json'])
            
            race_dist = pd.DataFrame()
            if row['race_distribution_json'] and row['race_distribution_json'] != '{}':
                race_dist = pd.read_json(row['race_distribution_json'])
            
            category_dist = pd.DataFrame()
            if row['category_distribution_json'] and row['category_distribution_json'] != '{}':
                category_dist = pd.read_json(row['category_distribution_json'])
            
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
                total_shoes = row['total_shoes_detected']
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
                "total_images_selected": row['total_images'],
                "total_shoes_detected": row['total_shoes_detected'],
                "unique_brands_count": row['unique_brands_count'],
                "brand_counts_all_selected": brand_counts,
                "top_brands_all_selected": top_brands_df,
                "persons_analyzed_count": row['total_persons_with_demographics'],
                "leader_brand_info": {
                    "name": row['leader_brand_name'] or "N/A",
                    "count": row['leader_brand_count'] or 0,
                    "percentage": row['leader_brand_percentage'] or 0.0
                },
                "gender_brand_distribution": gender_dist,
                "race_brand_distribution": race_dist,
                "brand_counts_by_category": category_dist,
                "brand_counts_by_marathon": pd.DataFrame(),  # Not needed for individual
                "total_persons_by_marathon": pd.Series(dtype='int'),
                "marathon_specific_data_for_cards": {
                    marathon_name: {
                        "images_count": row['total_images'],
                        "shoes_count": row['total_shoes_detected'],
                        "persons_count": row['total_persons_with_demographics']
                    }
                },
            }
        
        return individual_results
        
    except Exception as e:
        print(f"❌ Error retrieving individual pre-computed metrics: {e}")
        # Fall back to real-time calculation
        individual_results = {}
        for marathon_id in marathon_ids:
            df_flat, df_raw = get_data_for_selected_marathons_db([marathon_id])
            from data_processing import process_queried_data_for_report
            marathon_name = next((m['name'] for m in get_marathon_list_from_db() if m['id'] == marathon_id), f"Marathon_{marathon_id}")
            individual_results[marathon_name] = process_queried_data_for_report(df_flat, df_raw)
        return individual_results
    finally:
        conn.close()


def delete_marathon_by_id(marathon_id: int) -> bool:
    """
    Delete a marathon and all associated data (images, shoe detections, demographics, metrics).
    
    Args:
        marathon_id: The ID of the marathon to delete
        
    Returns:
        bool: True if deletion was successful, False otherwise
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check if marathon exists
        cursor.execute("SELECT name FROM Marathons WHERE marathon_id = ?", (marathon_id,))
        marathon = cursor.fetchone()
        
        if not marathon:
            print(f"❌ Marathon with ID {marathon_id} not found")
            return False
        
        marathon_name = marathon['name']
        print(f"🗑️ Deleting marathon '{marathon_name}' (ID: {marathon_id})...")
        
        # Delete in order due to foreign key constraints
        # 1. Delete shoe detections
        cursor.execute("""
            DELETE FROM ShoeDetections 
            WHERE image_id IN (
                SELECT image_id FROM Images WHERE marathon_id = ?
            )
        """, (marathon_id,))
        
        # 2. Delete person demographics
        cursor.execute("""
            DELETE FROM PersonDemographics 
            WHERE image_id IN (
                SELECT image_id FROM Images WHERE marathon_id = ?
            )
        """, (marathon_id,))
        
        # 3. Delete images
        cursor.execute("DELETE FROM Images WHERE marathon_id = ?", (marathon_id,))
        
        # 4. Delete marathon metrics
        cursor.execute("DELETE FROM MarathonMetrics WHERE marathon_id = ?", (marathon_id,))
        
        # 5. Finally delete the marathon itself
        cursor.execute("DELETE FROM Marathons WHERE marathon_id = ?", (marathon_id,))
        
        conn.commit()
        print(f"✅ Successfully deleted marathon '{marathon_name}' and all associated data")
        return True
        
    except Exception as e:
        print(f"❌ Error deleting marathon {marathon_id}: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

# Initialize database and tables
if __name__ == "__main__":
    create_tables()
    print("Database tables created/ensured.")