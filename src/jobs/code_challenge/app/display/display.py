from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, round
from pyspark.sql.window import Window

from ..context import Context
from ..transform.transform import Transform


class Display:
    def __init__(self, context: Context, transform: Transform):
        self.context = context
        self.transform = transform
        self.__logger = self.context.logger.getLogger(__name__)

    def exec(self):
        self.display_monthly_average(self.transform.dataframe).show(truncate=False)
        self.display_45day_moving_average(self.transform.dataframe).orderBy(col("tpep_dropoff_date").desc()).show(truncate=False)

    def display_monthly_average(self, dataframe: DataFrame) -> DataFrame:
        self.__logger.info(f"Monthly Average Trips")
        return dataframe.groupBy("tpep_dropoff_month_year").agg(
            round(avg("daily_avg_trip_distance"), 2).alias("monthly_avg_trip_distance")
        ).sort(col("tpep_dropoff_month_year").desc())

    def display_45day_moving_average(self, dataframe: DataFrame) -> DataFrame:
        self.__logger.info(f"45 day moving average")
        # days = lambda i: i * 86400
        # w = (Window()
        #      .orderBy(col("tpep_dropoff_date").cast('long'))
        #      .rangeBetween(-days(6), 0))
        #
        # return dataframe.withColumn('45_day_moving_average', avg("daily_avg_trip_distance").over(w))
        dataframe.createOrReplaceTempView("daily_avg_trips")
        return self.context.spark.sql(
            "select tpep_dropoff_date, "
            "avg(daily_avg_trip_distance) OVER(ORDER BY tpep_dropoff_date asc "
            "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ) as "
            "45_day_moving_average from daily_avg_trips order by tpep_dropoff_date desc"
        )
