import pytest
import sqlite3
import json

import pandas as pd

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

    @staticmethod
    def create_locations_data(db):
        conn = db.conn
        cursor = conn.cursor()
        sample_data = [
            (
                1,
                "AA",
                "AA100",
                "AAPlace",
                "AAState",
                2,
                None,
                None,
                None,
                None,
                42.5833,
                1.6667,
                6,
            ),
            (
                2,
                "AB",
                "AB100",
                "ABPlace",
                "ABState",
                2,
                None,
                None,
                None,
                None,
                43.4444,
                2.3333,
                5,
            ),
            (
                3,
                "AB",
                "AC100",
                "ACPlace",
                "ACState",
                2,
                None,
                None,
                None,
                None,
                44.4444,
                3.3333,
                4,
            ),
        ]
        cursor.executemany(
            """
            INSERT INTO location
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            sample_data,
        )
        conn.commit()

    @staticmethod
    def create_locations_dataframe():
        data = [
            [
                "BA",
                "BA101",
                "BA1Place",
                "BA1State",
                2,
                None,
                None,
                None,
                None,
                41.4444,
                1.3331,
                1,
            ],
            [
                "BA",
                "BA102",
                "BA2Place",
                "BA2State",
                2,
                None,
                None,
                None,
                None,
                42.4444,
                1.3332,
                1,
            ],
            [
                "BA",
                "BA103",
                "BA3Place",
                "BA3State",
                2,
                None,
                None,
                None,
                None,
                43.4444,
                1.3333,
                1,
            ],
            [
                "BA",
                "BA104",
                "BA4Place",
                "BA4State",
                2,
                None,
                None,
                None,
                None,
                44.4444,
                1.3334,
                1,
            ],
        ]

        df = pd.DataFrame(
            data,
            columns=[
                "country_code",
                "postal_code",
                "place_name",
                "state_name",
                "state_code",
                "county_name",
                "county_code",
                "community_name",
                "community_code",
                "latitude",
                "longitude",
                "accuracy",
            ],
        )
        return df

    def test_connection(self, setup_db):
        assert isinstance(setup_db.conn, sqlite3.Connection)

    def test_close(self, setup_db):
        self.create_locations_table(setup_db)
        setup_db.close()

        with pytest.raises(sqlite3.ProgrammingError):
            self.create_locations_data(setup_db)

    def test_loc_table_exists(self, setup_db):
        assert not setup_db.loc_table_exists()
        self.create_locations_table(setup_db)
        assert setup_db.loc_table_exists()

    def test_check_first_time(self, setup_db):
        assert setup_db.check_first_time()
        self.create_locations_table(setup_db)
        assert not setup_db.check_first_time()

    def test_add_country_data(self, setup_db):
        self.create_locations_table(setup_db)
        self.create_locations_data(setup_db)

        df = self.create_locations_dataframe()
        setup_db.add_country_data(df, "BA")

        res = setup_db.find_lat_lon("BA", "BA102")

        assert res["country_code"] == "BA"
        assert res["postal_code"] == "BA102"
        assert res["latitude"] == 42.4444
        assert res["longitude"] == 1.3332

    def test_add_country_data_overwrites(self, setup_db):
        self.create_locations_table(setup_db)
        self.create_locations_data(setup_db)

        df = self.create_locations_dataframe()
        setup_db.add_country_data(df, "BA")

        data = [
            [
                "BA",
                "BA104",
                "BA4Place",
                "BA4State",
                2,
                None,
                None,
                None,
                None,
                44.4444,
                1.3334,
                1,
            ],
            [
                "BA",
                "BA105",
                "BA5Place",
                "BA5State",
                2,
                None,
                None,
                None,
                None,
                45.4444,
                1.3335,
                1,
            ],
            [
                "BA",
                "BA106",
                "BA6Place",
                "BA6State",
                2,
                None,
                None,
                None,
                None,
                46.4444,
                1.3336,
                1,
            ],
            [
                "BA",
                "BA107",
                "BA7Place",
                "BA7State",
                2,
                None,
                None,
                None,
                None,
                47.4444,
                1.3337,
                1,
            ],
        ]

        df = pd.DataFrame(
            data,
            columns=[
                "country_code",
                "postal_code",
                "place_name",
                "state_name",
                "state_code",
                "county_name",
                "county_code",
                "community_name",
                "community_code",
                "latitude",
                "longitude",
                "accuracy",
            ],
        )
        setup_db.add_country_data(df, "BA")

        res = setup_db.find_lat_lon("BA", "BA102")
        assert res is None

        res = setup_db.find_lat_lon("BA", "BA106")
        assert res["country_code"] == "BA"
        assert res["postal_code"] == "BA106"
        assert res["latitude"] == 46.4444
        assert res["longitude"] == 1.3336

    def test_show_country_counts(self, setup_db):
        self.create_locations_table(setup_db)
        self.create_locations_data(setup_db)

        counts_dict = setup_db.show_country_counts()

        assert counts_dict["AA"] == 1
        assert counts_dict["AB"] == 2

    def test_find_lat_lon(self, setup_db):
        self.create_locations_table(setup_db)
        self.create_locations_data(setup_db)

        actual = setup_db.find_lat_lon("AB", "AB100")
        expected = {
            "country_code": "AB",
            "postal_code": "AB100",
            "latitude": 43.4444,
            "longitude": 2.3333,
            "accuracy": 5,
        }

        expected_str = json.dumps(expected, sort_keys=True)
        actual_str = json.dumps(actual, sort_keys=True)

        assert expected_str == actual_str

    def test_find_lat_lon_not_exists(self, setup_db):
        self.create_locations_table(setup_db)
        self.create_locations_data(setup_db)

        res = setup_db.find_lat_lon("AB", "NOTEXISTS")

        assert res is None
