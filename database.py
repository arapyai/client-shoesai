# database.py
import sqlite3
import pandas as pd
import json
from passlib.hash import pbkdf2_sha256 as hasher
from datetime import datetime

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
    CREATE TABLE IF NOT EXISTS Images (
        image_id INTEGER PRIMARY KEY AUTOINCREMENT,
        marathon_id INTEGER NOT NULL,
        filename TEXT NOT NULL,
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
                    INSERT INTO Images (marathon_id, filename, original_width, original_height)
                    VALUES (?, ?, ?, ?)
                """, (marathon_id, img_filename, image_record_dict.get('original_width'), image_record_dict.get('original_height')))
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
                    brand = shoe.get('label')[0] if shoe.get('label') and shoe.get('label') else None
                    prob = shoe.get('prob')[0] if shoe.get('prob') and shoe.get('prob') else None
                    bbox_list = shoe.get('bbox')[0] if shoe.get('bbox') and shoe.get('bbox') else [None]*4
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

# Initialize database and tables
if __name__ == "__main__":
    create_tables()
    print("Database tables created/ensured.")