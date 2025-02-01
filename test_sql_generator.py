from sqlalchemy import create_engine
from sql_generator import *

def test_build_schema():
    expected = """
    album: ['album_id', 'title', 'artist']\ncustomer: ['customer_id', 'first_name', 'last_name', 'address', 'city', 'country', 'state', 'postal_code', 'email', 'phone']
invoice: ['invoice_id', 'customer_id', 'billing_address', 'billing_city', 'billing_country', 'billing_postal_code', 'billing_state']
invoice_item: ['invoice_line_id', 'invoice_id', 'track_id', 'quantity']
track: ['track_id', 'name', 'album_id', 'composer', 'milliseconds', 'bytes', 'unit_price']
    """.strip()
    engine = create_engine("sqlite:///media_store.db")
    schema = build_schema(engine)
    assert schema == expected
    assert isinstance(schema, str)

def test_execute_sql_query():
    engine = create_engine("sqlite:///media_store.db")
    sql = "SELECT * FROM album"
    df = execute_sql_query(engine, sql)
    assert df.shape == (347, 3)
    assert set(df.columns) == set(['album_id', 'title', 'artist'])

def test_build_system_prompt():
    schema_description = "album: ['album_id', 'title', 'artist']"
    expected = """
    Given the following SQL tables, your job is to write SQLite queries to answer the user’s questions.
    Respond only with SQL and no additional content.\nalbum: ['album_id', 'title', 'artist']
    """.strip()
    assert build_system_prompt(schema_description) == expected

def test_user_prompt():
    user_question = "What are the names of all the albums?"
    expected = "What are the names of all the albums?\nSELECT"
    assert build_user_prompt(user_question) == expected