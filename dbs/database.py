import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

# Global connection variable
global_connection = None

def init_db():
    global global_connection
    if global_connection is None:
        try:
            global_connection = psycopg2.connect(
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT')
            )
            print("Database connection initialized successfully.")
        except Exception as e:
            print("Error initializing database connection:", e)
            raise

def get_connection():
    global global_connection
    if global_connection is None:
        init_db()
    return global_connection
