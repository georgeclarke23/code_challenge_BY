from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, round

from ..context import Context
from ..transform.transform import Transform


class Display:
    def __init__(self, context: Context, transform: Transform):
        self.context = context
        self.transform = transform
        self.__logger = self.context.logger.getLogger(__name__)

    def exec(self):
        self.display_monthly_average(self.transform.dataframe)
        self.display_45day_moving_average(self.transform.dataframe)

    def display_monthly_average(self, dataframe: DataFrame):
        self.__logger.info(f"Monthly Average Trips")
        dataframe.groupBy("tpep_dropoff_month_year").agg(
            round(avg("avg_trip_distance"), 2)
        ).sort(col("tpep_dropoff_month_year").desc()).show(truncate=False)

    def display_45day_moving_average(self, dataframe: DataFrame):
        self.__logger.info(f"45 day moving average")

        dataframe.createOrReplaceTempView("daily_avg_trips")
        self.context.spark.sql(
            "select tpep_dropoff_date, "
            "avg(avg_trip_distance) OVER(ORDER BY tpep_dropoff_date "
            "ROWS BETWEEN 44 PRECEDING AND CURRENT ROW ) as "
            "45_day_moving_average from daily_avg_trips order by tpep_dropoff_date desc"
        ).show(truncate=False)
