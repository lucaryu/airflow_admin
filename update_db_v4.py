"""
update_db_v4.py
dag_naming_rule 테이블을 기존 SQLite DB에 추가하는 마이그레이션 스크립트
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'instance', 'toy_airflow.db')

def run_migration():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # dag_naming_rule 테이블 존재 여부 확인
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dag_naming_rule'")
    exists = cursor.fetchone()

    if not exists:
        cursor.execute("""
            CREATE TABLE dag_naming_rule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_tokens TEXT NOT NULL DEFAULT '[]',
                separator VARCHAR(5) DEFAULT '_',
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("[OK] dag_naming_rule 테이블 생성 완료")
    else:
        print("[SKIP] dag_naming_rule 테이블이 이미 존재합니다")

    conn.commit()
    conn.close()
    print("마이그레이션 완료!")

if __name__ == '__main__':
    run_migration()
