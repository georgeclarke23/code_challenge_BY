import datetime

import pandas as pd

from tests.base import PySparkTest


class TestDisplay(PySparkTest):

    def test_should_get_monthly_average(self):
        pandas_df = pd.DataFrame(
            {
                "tpep_dropoff_date": [
                    datetime.datetime(2021, 3, 24),
                    datetime.datetime(2021, 3, 23)
                ],
                "daily_avg_trip_distance": [2.0, 2.0],
                "tpep_dropoff_month_year": ["2021-03", "2021-03"],
            }
        )

        expected_df = pd.DataFrame(
            {

                "tpep_dropoff_month_year": ["2021-03"],
                "monthly_avg_trip_distance": [2.0]
            }
        )

        self.assert_dataframe(
            self.display.display_monthly_average,
            expected_df,
            pandas_df
        )

    def test_should_display_45day_moving_average(self):
        pandas_df = pd.DataFrame(
            {
                "tpep_dropoff_date": [
                    datetime.datetime(2021, 3, 24),
                    datetime.datetime(2021, 3, 23)
                ],
                "daily_avg_trip_distance": [4.0, 2.0],
                "tpep_dropoff_month_year": ["2021-03", "2021-03"],
            }
        )

        expected_df = pd.DataFrame(
            {

                "tpep_dropoff_date": [
                    datetime.datetime(2021, 3, 24),
                    datetime.datetime(2021, 3, 23)
                ],
                "45_day_moving_average": [3.0, 2.0]
            }
        )

        self.assert_dataframe(
            self.display.display_45day_moving_average,
            expected_df,
            pandas_df
        )

