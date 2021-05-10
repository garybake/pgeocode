# License 3-clause BSD
#
# Authors: Roman Yurchak <roman.yurchak@symerio.com>,
# Gary Bake <garybake@gmail.com>

import sqlite3
import logging
from typing import Optional, Dict

import pandas as pd

logging.basicConfig(level=logging.DEBUG)


class LocationDatabase:
    """Location Database Helper class."""

    def __init__(self):
        self.conn = None

    def connect(self, filename: str) -> None:
        """Connect to the database.
        This will create the database if it doesn't already exist

        Parameters
        ----------
        filename : str
          path to the sqlite database

        """
        logging.debug(f"Connecting to {filename}")
        self.conn = sqlite3.connect(filename)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        """Closes the connection to the database."""
        self.conn.close()

    def loc_table_exists(self) -> bool:
        """Check if the location table exists.

        Returns
        -------
        result : Boolean
          whether the table exists
        """
        sql = """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        AND name='location';
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        return row is not None

    def check_first_time(self) -> bool:
        """Is this the first time the db has been used.

        The database will be created when the first connection is made.
        A check is made for the location table to see if everything is setup.

        Returns
        -------
        result : Boolean
          whether this is the first time the database has been used.
        """
        return not self.loc_table_exists()

    def add_country_data(
        self, df: pd.DataFrame, country_code: str, erase_first: bool = True
    ) -> None:
        """Add a country's data to the database

        Parameters
        ----------
        df : pandas.DataFrame
          a pandas.DataFrame with the relevant information
        country_code : String
          a pandas.DataFrame with the relevant information
        erase_first : Boolean
          a pandas.DataFrame with the relevant information

        Returns
        -------
        result : Boolean
          whether the table exists
        """
        logging.debug(f"Importing to {country_code}")
        first_time = self.check_first_time()
        if first_time:
            logging.debug(f"First time: {first_time}")

        if erase_first and not first_time:
            sql_drop_data = "DELETE FROM location WHERE country_code = ?"
            self.conn.execute(sql_drop_data, (country_code,))

        df.to_sql("location", con=self.conn, if_exists="append")

        if first_time:
            cursor = self.conn.cursor()
            sql_create_index = """
            CREATE INDEX IF NOT EXISTS idx_postalcode_countrycode
            ON location(postal_code)
            """
            cursor.execute(sql_create_index)

    def show_country_counts(self) -> None:
        """Print contents of the database
        TODO: this is mostly for debugging so should take it out

        Shows number of postcodes by country
        """

        sql = """
            SELECT country_code, count(*)
            FROM location
            GROUP BY country_code
        """
        cursor = self.conn.cursor()
        rows = cursor.execute(sql).fetchall()

        for row in rows:
            print(row)

    def find_lat_lon(self, country_code: str, postcode: str) -> Optional[Dict]:
        """Find coordinates based on a country_code and postcode
        returns Dict of row
        If postcode is not found then None is returned

        example output
        {
            'country_code': 'GB', 'postal_code': 'SW1W 0NY',
            'latitude': 51.4954, 'longitude': -0.1474, 'accuracy': 6
        }

        Parameters
        ----------
        country_code : String
          Two character ISO code of the country
        postcode : String
          The postcode to search for

        Returns
        -------
        result : Dict
          Dict of output
        """
        cursor = self.conn.cursor()

        sql = f"""
            SELECT country_code, postal_code, latitude, longitude, accuracy
            FROM location
            WHERE country_code = ?
            AND postal_code = ?
        """

        row = cursor.execute(sql, (country_code, postcode)).fetchone()
        if not row:
            return None

        return dict(row)
