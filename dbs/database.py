import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

def get_connection():
    try:
        connection = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        print("Database connection initialized successfully.")
        return connection
    except Exception as e:
        print("Error initializing database connection:", e)
        raise

def close_connection(connection):
    if connection is not None:
        connection.close()
        print("Database connection closed successfully.")