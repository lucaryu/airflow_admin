import sqlite3

def update_db():
    conn = sqlite3.connect('instance/toy_airflow.db')
    cursor = conn.cursor()

    try:
        # Create custom_operator table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS custom_operator (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(100) NOT NULL,
            description VARCHAR(500),
            code TEXT NOT NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        print("Successfully created custom_operator table.")
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")
    finally:
        conn.commit()
        conn.close()

if __name__ == "__main__":
    update_db()
