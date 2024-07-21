import pytest
from dbs.data import create_table, insert_data, fetch_data
from dbs.database import close_connection, get_connection

@pytest.fixture(scope='session', autouse=True)
def setup_db():
    conn = get_connection()
    try:
        create_table()
        yield
    finally:
        close_connection(conn)


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
    finally:
        close_connection(conn)

def test_create_table():
    create_table()
    data = fetch_data()
    assert len(data) == 0, f"Table should be empty initially, but has {len(data)} rows"

def test_upsert_data():
    initial_data = [('Ram', 20, 'Engineering'), ('Shyam', 22, 'Engineering'), ('Bob', 24, 'Engineering')]
    for item in initial_data:
        insert_data(item)

    updated_data = [('Ram', 21, 'Science'), ('Shyam', 23, 'Business')]
    for item in updated_data:
        insert_data(item)
    
    fetched_data = fetch_data()
    print("Fetched Data:", fetched_data)  
    
    assert len(fetched_data) == 3, f"Should have 3 rows, but has {len(fetched_data)} rows"
    
    assert fetched_data[0][1] == 'Ram', "First row should have name 'Ram'"
    assert fetched_data[0][2] == 21, "First row should have updated age 21"
    assert fetched_data[0][3] == 'Science', "First row should have updated department 'Science'"
    
    assert fetched_data[1][1] == 'Shyam', "Second row should have name 'Shyam'"
    assert fetched_data[1][2] == 23, "Second row should have updated age 23"
    assert fetched_data[1][3] == 'Business', "Second row should have updated department 'Business'"
    
    assert fetched_data[2][1] == 'Bob', "Third row should have name 'Bob'"
    assert fetched_data[2][2] == 24, "Third row should have age 24"
    assert fetched_data[2][3] == 'Engineering', "Third row should have department 'Engineering'"

def test_fetch_data():
    initial_data = [('Ram', 20, 'Engineering'), ('Shyam', 22, 'Engineering'), ('Bob', 24, 'Engineering')]
    for item in initial_data:
        insert_data(item)
    
    fetched_data = fetch_data()
    assert len(fetched_data) == 3, f"Should fetch 3 rows, but has {len(fetched_data)} rows"
    assert fetched_data[0][1] == 'Ram', "First row should have name 'Ram'"
    assert fetched_data[1][1] == 'Shyam', "Second row should have name 'Shyam'"
    assert fetched_data[2][1] == 'Bob', "Third row should have name 'Bob'"