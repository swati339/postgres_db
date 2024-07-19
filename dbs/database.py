import psycopg2
from contextlib import contextmanager

@contextmanager
def get_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            dbname='testdb',
            user='testuser',
            password='password',
            host='localhost',
            port='5432'
        )
        yield conn
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
