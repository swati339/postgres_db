
from dbs.database import get_connection

def create_table():
    query = '''
    CREATE TABLE IF NOT EXISTS students (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        age INTEGER,
        department VARCHAR(100)
    );
    '''
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()

def insert_data():
    query = '''
    INSERT INTO students (name, age, department)
    VALUES 
    ('Ram', 30, 'Engineering'),
    ('Shyam', 25, 'Marketing'),
    ('Bob', 28, 'Sales');
    '''
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()

def fetch_data():
    query = 'SELECT * FROM students;'
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
    return data
