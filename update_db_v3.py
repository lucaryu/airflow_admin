"""
DB Migration Script v3: Add source_table_desc and source_column_desc columns
"""
import sqlite3
import os

def migrate():
    db_path = os.path.join(os.path.dirname(__file__), 'instance', 'toy_airflow.db')
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    changes = []
    
    # Check and add source_table_desc to mapping table
    cursor.execute("PRAGMA table_info(mapping)")
    mapping_cols = [row[1] for row in cursor.fetchall()]
    
    if 'source_table_desc' not in mapping_cols:
        cursor.execute("ALTER TABLE mapping ADD COLUMN source_table_desc VARCHAR(500)")
        changes.append("Added 'source_table_desc' to mapping table")
    
    # Check and add source_column_desc to mapping_column table
    cursor.execute("PRAGMA table_info(mapping_column)")
    mc_cols = [row[1] for row in cursor.fetchall()]
    
    if 'source_column_desc' not in mc_cols:
        cursor.execute("ALTER TABLE mapping_column ADD COLUMN source_column_desc VARCHAR(500)")
        changes.append("Added 'source_column_desc' to mapping_column table")
    
    conn.commit()
    conn.close()
    
    if changes:
        print("Migration v3 completed:")
        for c in changes:
            print(f"  - {c}")
    else:
        print("Migration v3: No changes needed (columns already exist)")

if __name__ == '__main__':
    migrate()
