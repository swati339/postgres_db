import pytest
from dbs.data import create_table, insert_data, fetch_data

@pytest.fixture(scope='module', autouse = True)
def setup_database():
    create_table()
    yield
    # Cleanup code if needed

def test_create_table():
    data = fetch_data()
    assert len(data) == 0, "Table should be empty initially"

def test_insert_data():
    insert_data()
    data = fetch_data()
    assert len(data) == 3, "Should have inserted 3 rows"

def test_fetch_data():
    insert_data()  # Ensure data is inserted
    data = fetch_data()
    assert data is not None, "Fetch data should not return None"
    assert len(data) == 3, "Should fetch 3 rows"
    assert data[0][1] == 'Ram', "First row should have name 'Ram'"
    assert data[1][1] == 'Shyam', "Second row should have name 'Shyam'"
    assert data[2][1] == 'Bob', "Third row should have name 'Bob'"