import pandas as pd
import pytest
from app import get_drivers
@pytest.fixture()
def database(postgresql):
    """Set up the mock DB with the SQL flat file."""
    with open("test_trade_news_events.sql") as f:
        setup_sql = f.read()
    with postgresql.cursor() as cursor:
        cursor.execute(setup_sql)
        postgresql.commit()
    yield postgresql
def test_example_postgres(database):
    drivers = get_drivers(db_url=database)
    assert len(drivers) == 2
    assert set(drivers["name"]) == {"Dan", "Jeff"}