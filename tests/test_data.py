import pytest
import json
from dbs.data import drop_table, create_table, insert_data, fetch_data
from dbs.database import close_connection, get_connection

@pytest.fixture(scope='session', autouse=True)
def setup_db():
    conn = get_connection()
    try:
        drop_table()
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

def test_multiple_upsert_data():
    initial_data = [
        ('Ram', 20, 'Engineering', {'hobbies': ['reading', 'sports']}),
        ('Shyam', 22, 'Engineering', {'hobbies': ['music', 'travel']}),
        ('Bob', 24, 'Engineering', {'hobbies': ['coding', 'gaming']})
    ]
    insert_data(initial_data)

    updated_data = [
        ('Ram', 21, 'Science', {'hobbies': ['reading', 'science']}),
        ('Shyam', 23, 'Business', {'hobbies': ['music', 'business']}),
        ('Aesha', 30, 'Science', {'hobbies': ['dancing', 'travel']})
    ]
    insert_data(updated_data)
    
    fetched_data = fetch_data()
    print("Fetched Data:", fetched_data)  # Debugging line
    
    # Check type of fetched_data
    assert isinstance(fetched_data, list), "Fetched data should be a list"
    
    for row in fetched_data:
        assert isinstance(row, tuple), f"Each row should be a tuple, but got {type(row)}"
    
    assert len(fetched_data) == 4, f"Should have 4 rows, but has {len(fetched_data)} rows"
    
    # Validate the data with the adjusted index for the columns
    assert fetched_data[0][1] == 'Ram', f"First row should have name 'Ram', but has {fetched_data[0][1]}"
    assert fetched_data[0][2] == 21, f"First row should have updated age 21, but has {fetched_data[0][2]}"
    assert fetched_data[0][3] == 'Science', f"First row should have updated department 'Science', but has {fetched_data[0][3]}"
    assert fetched_data[0][4] == {'hobbies': ['reading', 'science']}, f"First row should have updated details {'hobbies': ['reading', 'science']}, but has {fetched_data[0][4]}"
    
    assert fetched_data[1][1] == 'Shyam', f"Second row should have name 'Shyam', but has {fetched_data[1][1]}"
    assert fetched_data[1][2] == 23, f"Second row should have updated age 23, but has {fetched_data[1][2]}"
    assert fetched_data[1][3] == 'Business', f"Second row should have updated department 'Business', but has {fetched_data[1][3]}"
    assert fetched_data[1][4] == {'hobbies': ['music', 'business']}, f"Second row should have updated details {'hobbies': ['music', 'business']}, but has {fetched_data[1][4]}"
    
    assert fetched_data[2][1] == 'Bob', f"Third row should have name 'Bob', but has {fetched_data[2][1]}"
    assert fetched_data[2][2] == 24, f"Third row should have age 24, but has {fetched_data[2][2]}"
    assert fetched_data[2][3] == 'Engineering', f"Third row should have department 'Engineering', but has {fetched_data[2][3]}"
    assert fetched_data[2][4] == {'hobbies': ['coding', 'gaming']}, f"Third row should have details {'hobbies': ['coding', 'gaming']}, but has {fetched_data[2][4]}"
    
    assert fetched_data[3][1] == 'Aesha', f"Fourth row should have name 'Aesha', but has {fetched_data[3][1]}"
    assert fetched_data[3][2] == 30, f"Fourth row should have age 30, but has {fetched_data[3][2]}"
    assert fetched_data[3][3] == 'Science', f"Fourth row should have department 'Science', but has {fetched_data[3][3]}"
    assert fetched_data[3][4] == {'hobbies': ['dancing', 'travel']}, f"Fourth row should have details {'hobbies': ['dancing', 'travel']}, but has {fetched_data[3][4]}"



def test_fetch_data():
    initial_data = [
        ('Ram', 20, 'Engineering', {'hobbies': ['reading', 'sports']}),
        ('Shyam', 22, 'Business', {'hobbies': ['music', 'travel']}),
        ('Bob', 24, 'Engineering', {'hobbies': ['coding', 'gaming']}),
        ('Aesha', 30, 'Science', {'hobbies': ['dancing', 'travel']})
    ]
    insert_data(initial_data)
    
    fetched_data = fetch_data()
    print("Fetched Data:", fetched_data)  # Debugging line
    print("Type of Fetched Data:", type(fetched_data))  # Should be list

    assert isinstance(fetched_data, list), "Fetched data should be a list"
    
    for i, row in enumerate(fetched_data):
        print(f"Row {i}:", row)  # Debugging line
        assert isinstance(row, tuple), f"Row {i} should be a tuple, but got {type(row)}"
    
    assert len(fetched_data) == 4, f"Should fetch 4 rows, but has {len(fetched_data)} rows"
    
    # Validate the data
    expected_names = ['Ram', 'Shyam', 'Bob', 'Aesha']
    for i, name in enumerate(expected_names):
        assert fetched_data[i][1] == name, f"Row {i} should have name '{name}', but has {fetched_data[i][1]}"
