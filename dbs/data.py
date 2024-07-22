from dbs.database import get_connection
from psycopg2.extras import execute_values


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
                    department VARCHAR(100)
                );
            """)
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    
def insert_data(data):
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO students (name, age, department)
                VALUES %s
                ON CONFLICT (name) DO UPDATE
                SET age = EXCLUDED.age,
                    department = EXCLUDED.department;
            """, data)
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