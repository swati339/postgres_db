import pytest
from dbs.data import drop_table, create_table, insert_data, fetch_data
from dbs.database import close_connection, get_connection

@pytest.fixture(scope='session', autouse=True)
def setup_db():
    conn = get_connection()
    try:
        drop_table()  # Drop the existing table to ensure a clean setup
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
    # Insert initial data
    initial_data = [('Ram', 20, 'Engineering'), ('Shyam', 22, 'Engineering'), ('Bob', 24, 'Engineering')]
    for item in initial_data:
        insert_data(item)

    # Insert data with conflicts (updating existing records)
    updated_data = [('Ram', 21, 'Science'), ('Shyam', 23, 'Business')]
    for item in updated_data:
        insert_data(item)
    
    # Fetch and verify data
    fetched_data = fetch_data()
    print("Fetched Data:", fetched_data)  # Debugging line
    
    # Check if the data length is as expected
    assert len(fetched_data) == 3, f"Should have 3 rows, but has {len(fetched_data)} rows"
    
    # Check if the data is correctly updated
    assert fetched_data[0][1] == 'Ram', f"First row should have name 'Ram', but has {fetched_data[0][1]}"
    assert fetched_data[0][2] == 21, f"First row should have updated age 21, but has {fetched_data[0][2]}"
    assert fetched_data[0][3] == 'Science', f"First row should have updated department 'Science', but has {fetched_data[0][3]}"
    
    assert fetched_data[1][1] == 'Shyam', f"Second row should have name 'Shyam', but has {fetched_data[1][1]}"
    assert fetched_data[1][2] == 23, f"Second row should have updated age 23, but has {fetched_data[1][2]}"
    assert fetched_data[1][3] == 'Business', f"Second row should have updated department 'Business', but has {fetched_data[1][3]}"
    
    assert fetched_data[2][1] == 'Bob', f"Third row should have name 'Bob', but has {fetched_data[2][1]}"
    assert fetched_data[2][2] == 24, f"Third row should have age 24, but has {fetched_data[2][2]}"
    assert fetched_data[2][3] == 'Engineering', f"Third row should have department 'Engineering', but has {fetched_data[2][3]}"

def test_fetch_data():
    # Insert some data
    initial_data = [('Ram', 20, 'Engineering'), ('Shyam', 22, 'Engineering'), ('Bob', 24, 'Engineering')]
    for item in initial_data:
        insert_data(item)
    
    # Fetch and verify data
    fetched_data = fetch_data()
    assert len(fetched_data) == 3, f"Should fetch 3 rows, but has {len(fetched_data)} rows"
    assert fetched_data[0][1] == 'Ram', f"First row should have name 'Ram', but has {fetched_data[0][1]}"
    assert fetched_data[1][1] == 'Shyam', f"Second row should have name 'Shyam', but has {fetched_data[1][1]}"
    assert fetched_data[2][1] == 'Bob', f"Third row should have name 'Bob', but has {fetched_data[2][1]}"
