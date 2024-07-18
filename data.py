import psycopg2

db_params = {
    'dbname': 'testdb',
    'user': 'testuser',
    'password': 'password',
    'host': 'localhost',
    'port': '5432'
}

try:
  conn = psycopg2.connect(**db_params)
  cur = conn.cursor()


  cur.execute('''
        CREATE TABLE IF NOT EXISTS students (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            age INTEGER,
            department VARCHAR(100)
        );
    ''')
      


  cur.execute('''INSERT INTO students (name, age, department) VALUES
            ('Ram', 20, 'Computer Science'),
            ('Shyam', 22, 'Business'),
            ('Bob', 21, 'Engineering');''')

  conn.commit()

  cur.execute("SELECT * FROM students")
  rows = cur.fetchall()
  for row in rows:
    print(row)

except Exception as e:
    print(f"Error: {e}")

finally:
   if cur:
      cur.close()
   if conn:
      conn.close()





