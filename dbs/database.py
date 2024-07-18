import psycopg2


def get_connection():
    db_params = {
        'dbname': 'testdb',
        'user': 'testuser',
        'password': 'password',
        'host': 'localhost',
        'port': 5432
    }
    
    return psycopg2.connect(**db_params)