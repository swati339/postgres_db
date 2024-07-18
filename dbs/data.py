# from .database import get_connection


# def create_table():
#     conn = None
#     cur = None
#     try:
#         conn = get_connection()
#         cur = conn.cursor()


#         cur.execute('''
#             CREATE TABLE IF NOT EXISTS students (
#                id SERIAL PRIMARY KEY,
#                name VARCHAR(100),
#                age INTEGER,
#                department VARCHAR(100)
#         );
#     ''')
      


#         conn.commit()
#     except Exception as e:
#         print(f"Error: {e}")

#     finally:
#        if cur:
#           cur.close()
#        if conn:
#           conn.close()

      
# def insert_data():
#     conn = None
#     cur = None
#     try:
#         conn = get_connection()
#         cur = conn.cursor()

#         cur.execute('''INSERT INTO students (name, age, department) VALUES
#             ('Ram', 20, 'Computer Science'),
#             ('Shyam', 22, 'Business'),
#             ('Bob', 21, 'Engineering');
                    
#                 ''')
#         conn.commit()

#     except Exception as e:
#         print(f"Error: {e}")
#     finally:
#         if cur:
#             cur.close()
#         if conn:
#             conn.close()

# def fetch_data():
#     connection = None
#     cursor = None
#     try:
#         connection = get_connection()
#         cursor = connection.cursor()

#         cursor.execute('SELECT * FROM students;')
#         rows = cursor.fetchall()

#         return rows
#     except Exception as error:
#         print(f"Error: {error}")
#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()

from dbs.database import get_connection


def create_table():
    connection = None
    cursor = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute('''
            DROP TABLE IF EXISTS employees;
            CREATE TABLE employees (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                age INTEGER,
                department VARCHAR(255)
            );
        ''')
        connection.commit()
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# db/data.py - insert_data() function

def insert_data():
    connection = None
    cursor = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        
        # Truncate table to ensure it's empty
        cursor.execute('TRUNCATE TABLE employees RESTART IDENTITY;')
        
        # Insert new data
        cursor.execute('''
            INSERT INTO employees (name, age, department) VALUES
            ('Ram', 30, 'Engineering'),
            ('Shyam', 25, 'Marketing'),
            ('Bob', 28, 'Sales');
        ''')
        connection.commit()
    except Exception as error:
        print(f"Error: {error}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()



def fetch_data():
    connection = None
    cursor = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM employees;')
        data = cursor.fetchall()
        return data
    except Exception as error:
        print(f"Error: {error}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()





