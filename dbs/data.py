from dbs.database import get_connection

conn = get_connection()  # Initialize the connection globally

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
    
# def insert_data():
#     try:
#         with conn.cursor() as cur:
#             cur.execute("INSERT INTO students (name, age, department) VALUES (%s, %s, %s)", ('Ram', 20, 'Engineering'))
#             cur.execute("INSERT INTO students (name, age, department) VALUES (%s, %s, %s)", ('Shyam', 22, 'Business'))
#             cur.execute("INSERT INTO students (name, age, department) VALUES (%s, %s, %s)", ('Bob', 21, 'Arts'))
#             conn.commit()
#     except Exception as e:
#         conn.rollback()
#         raise e

    
def insert_data(data):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO students (name, age, department)
                VALUES (%s, %s, %s)
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
            cur.execute("SELECT * FROM students")
            data = cur.fetchall()
            return data
    except Exception as e:
        raise e