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
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()  # Ensure table creation is committed

def insert_data():
    query = '''
    INSERT INTO students (name, age, department)
    VALUES 
    ('Ram', 30, 'Engineering'),
    ('Shyam', 25, 'Marketing'),
    ('Bob', 28, 'Sales');
    '''
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()  # Ensure data insertion is committed
            cur.execute('SELECT COUNT(*) FROM students;')
            count = cur.fetchone()[0]
            print(f"Inserted rows count: {count}")

def fetch_data():
    query = 'SELECT * FROM students;'
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            data = cur.fetchall()
            return data