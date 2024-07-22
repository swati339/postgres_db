from dbs.database import get_connection
from psycopg2.extras import execute_values
import json

# Establish a connection
conn = get_connection()

def drop_table():
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS students;")
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

def create_table():
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS students (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) UNIQUE NOT NULL,
                    age INTEGER,
                    department VARCHAR(100),
                    details JSONB
                );
            """)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

def insert_data(data):
    try:
        with conn.cursor() as cur:
            # Prepare data for insertion
            formatted_data = [(name, age, department, json.dumps(details)) for name, age, department, details in data]
            
            # Execute the insert statement with conflict handling
            execute_values(cur, """
                INSERT INTO students (name, age, department, details)
                VALUES %s
                ON CONFLICT (name) DO UPDATE
                SET age = EXCLUDED.age,
                    department = EXCLUDED.department,
                    details = EXCLUDED.details;
            """, formatted_data)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

def fetch_data():
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM students ORDER BY age")
            data = cur.fetchall()
            return data
    except Exception as e:
        raise e
