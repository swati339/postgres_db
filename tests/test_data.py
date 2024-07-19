import pytest
from dbs.data import create_table, insert_data, fetch_data
from dbs.database import init_db, close_connection, get_connection

@pytest.fixture(scope='session', autouse=True)
def setup_db():
    init_db()
    yield
    close_connection()

@pytest.fixture(scope='function', autouse=True)
def clear_table():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM students;')
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

def test_create_table():
    create_table()
    data = fetch_data()
    assert len(data) == 0, f"Table should be empty initially, but has {len(data)} rows"

def test_insert_data():
    create_table()
    insert_data()
    data = fetch_data()
    assert len(data) == 3, f"Should have inserted 3 rows, but has {len(data)} rows"
    assert data[0][1] == 'Ram', "First row should have name 'Ram'"
    assert data[1][1] == 'Shyam', "Second row should have name 'Shyam'"
    assert data[2][1] == 'Bob', "Third row should have name 'Bob'"

def test_fetch_data():
    create_table()
    insert_data()
    data = fetch_data()
    assert len(data) == 3, f"Should fetch 3 rows, but has {len(data)} rows"
    assert data[0][1] == 'Ram', "First row should have name 'Ram'"
    assert data[1][1] == 'Shyam', "Second row should have name 'Shyam'"
    assert data[2][1] == 'Bob', "Third row should have name 'Bob'"


