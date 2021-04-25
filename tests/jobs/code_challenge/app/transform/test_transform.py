import datetime
import pandas as pd
from pandas.testing import assert_frame_equal

from tests.base import PySparkTest


class TestTransform(PySparkTest):
    def test_should_select_required_columns(self):

        pandas_df = pd.DataFrame(
            {
                "tpep_dropoff_datetime": ["2021-03-23"],
                "trip_distance": [2],
                "remove": ["test"],
            }
        )

        expected_df = pd.DataFrame(
            {"tpep_dropoff_datetime": ["2021-03-23"], "trip_distance": [2]}
        )
        self.assert_dataframe(
            self.transform.select_required_colums, expected_df, pandas_df
        )

    def test_should_extract_date_from_drop_off_datetime_col(self):

        pandas_df = pd.DataFrame(
            {
                "tpep_dropoff_datetime": ["2021-03-23"],
                "trip_distance": [2],
            }
        )

        expected_df = pd.DataFrame(
            {
                "tpep_dropoff_datetime": ["2021-03-23"],
                "trip_distance": [2],
                "tpep_dropoff_date": [datetime.datetime(2021, 3, 23)],
            }
        )
        self.assert_dataframe(
            self.transform.extract_date_from_drop_off_datetime_col,
            expected_df,
            pandas_df,
        )

    def test_should_get_daily_average_of_trips(self):

        pandas_df = pd.DataFrame(
            {
                "tpep_dropoff_datetime": ["2021-03-23"],
                "trip_distance": [2],
                "tpep_dropoff_date": [datetime.datetime(2021, 3, 23)],
            }
        )
        expected_df = pd.DataFrame(
            {
                "tpep_dropoff_date": [datetime.datetime(2021, 3, 23)],
                "daily_avg_trip_distance": [2.0],
            }
        )
        self.assert_dataframe(
            self.transform.daily_average_of_trips, expected_df, pandas_df
        )

    def test_should_extract_month_from_drop_off_datetime_col(self):

        pandas_df = pd.DataFrame(
            {
                "tpep_dropoff_date": [datetime.datetime(2021, 3, 23)],
                "daily_avg_trip_distance": [2.0],
            }
        )

        expected_df = pd.DataFrame(
            {
                "tpep_dropoff_date": [datetime.datetime(2021, 3, 23)],
                "daily_avg_trip_distance": [2.0],
                "tpep_dropoff_month_year": ["2021-03"],
            }
        )

        self.assert_dataframe(
            self.transform.extract_month_from_drop_off_datetime_col,
            expected_df,
            pandas_df,
        )

    def test_should_execute_transformations(self):

        expected_df = pd.DataFrame(
            {
                "tpep_dropoff_date": [
                    datetime.datetime(2021, 3, 24),
                    datetime.datetime(2021, 3, 23)
                ],
                "daily_avg_trip_distance": [2.0, 2.0],
                "tpep_dropoff_month_year": ["2021-03", "2021-03"],
            }
        )

        actual_spark_df = self.transform.execute()
        actual_df = actual_spark_df.toPandas()
        assert_frame_equal(left=actual_df, right=expected_df, check_dtype=False)
