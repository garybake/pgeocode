import pytest
import sqlite3

from loc_db import LocationDatabase

db_file = ":memory:"


@pytest.fixture
def setup_db():
    """ Fixture to set up the in-memory database with test data """
    db = LocationDatabase(db_file)
    yield db
    db.close()


class TestLocDB:
    @staticmethod
    def create_locations_table(db):
        conn = db.conn
        cursor = conn.cursor()
        create_table_sql = """
            create table location (
                "index" INTEGER,
                country_code TEXT,
                postal_code TEXT,
                place_name TEXT,
                state_name TEXT,
                state_code INTEGER,
                county_name REAL,
                county_code REAL,
                community_name REAL,
                community_code REAL,
                latitude REAL,
                longitude REAL,
                accuracy INTEGER
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()

    def create_locations_data(self, db):
        # sample_data = [
        #     ('2020-01-01', 'BUY', 'IBM', 1000, 45.0),
        #     ('2020-01-01', 'SELL', 'GOOG', 40, 123.0),
        # ]
        # cursor.executemany(
        #   'INSERT INTO stocks VALUES(?, ?, ?, ?, ?)', sample_data)
        pass

    def test_connection(self, setup_db):
        assert isinstance(setup_db.conn, sqlite3.Connection)

    def test_close(self):
        assert not hasattr(setup_db, "conn")

    def test_loc_table_exists(self, setup_db):
        assert not setup_db.loc_table_exists()
        self.create_locations_table(setup_db)
        assert setup_db.loc_table_exists()

    def test_check_first_time(self, setup_db):
        assert setup_db.check_first_time()
        self.create_locations_table(setup_db)
        assert not setup_db.check_first_time()

    def test_add_country_data(self):
        assert False

    def test_show_country_counts(self):
        assert False

    def test_find_lat_lon(self):
        assert False
