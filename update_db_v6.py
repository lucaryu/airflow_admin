import sqlite3
import os

def update_db():
    db_path = os.path.join(os.path.dirname(__file__), 'instance', 'toy_airflow.db')
    
    if not os.path.exists(db_path):
        print(f"Database file not found at {db_path}")
        return

    print(f"Connecting to database at {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Check if source_schema already exists
        cursor.execute("PRAGMA table_info(mapping)")
        columns = [col[1] for col in cursor.fetchall()]
        
        added = False
        if 'source_schema' not in columns:
            print("Adding source_schema column to mapping table...")
            cursor.execute("ALTER TABLE mapping ADD COLUMN source_schema VARCHAR(100)")
            added = True
            
        if 'target_schema' not in columns:
            print("Adding target_schema column to mapping table...")
            cursor.execute("ALTER TABLE mapping ADD COLUMN target_schema VARCHAR(100)")
            added = True

        if added:
            conn.commit()
            print("Successfully updated database schema.")
        else:
            print("Columns already exist. No update needed.")

    except Exception as e:
        print(f"Error updating database: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    update_db()
