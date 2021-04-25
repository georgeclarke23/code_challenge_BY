from pipetools import pipe
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, date_format, avg, col

from ..context import Context
from ..ingest.ingest import Ingest
from ..utils.containers import ExecutionContainer


class Transform(ExecutionContainer):
    """
    Transformation class
    - All transformation to the ingested dataset are completed here.
    """
    def __init__(self, context: Context, ingest: Ingest):
        super().__init__(context)
        self.context = context
        self.ingest = ingest
        self.required_columns = ["tpep_dropoff_datetime", "trip_distance"]
        self.__logger = self.context.logger.getLogger(__name__)

    def execute(self) -> DataFrame:
        """
        Piping one transformation into another to achieve the desired dataframe.
        :return: Transformed dataframe
        """
        transform = (
            pipe
            | self.select_required_colums
            | self.extract_date_from_drop_off_datetime_col
            | self.daily_average_of_trips
            | self.extract_month_from_drop_off_datetime_col
        )
        self.dataframe = transform(self.ingest.dataframe)
        return self.dataframe

    def select_required_colums(self, dataframe: DataFrame) -> DataFrame:
        """
        Selecting columns that are required, to achieve the desired dataframe.
        :param dataframe:
        :return: Dataframe with the necessary columns
        """
        self.__logger.info(f"Selecting required Columns: {self.required_columns}")
        return dataframe.select(*self.required_columns)

    def extract_date_from_drop_off_datetime_col(
        self, dataframe: DataFrame
    ) -> DataFrame:
        """
        Extracting the date from the datetime column
        :param dataframe:
        :return: Dataframe with a new date column
        """
        self.__logger.info("Extracting date from `tpep_dropoff_date` Column")
        return dataframe.withColumn(
            "tpep_dropoff_date", to_date("tpep_dropoff_datetime")
        )

    def daily_average_of_trips(self, dateframe: DataFrame) -> DataFrame:
        """
        Transforming the dataframe into daily average trips
        :param dateframe:
        :return: Dataframe that has the day and the daily average.
        """
        self.__logger.info("Transforming dataframe to get daily average of trips")
        return dateframe.groupBy("tpep_dropoff_date").agg(
            avg("trip_distance").alias("daily_avg_trip_distance")
        )

    def extract_month_from_drop_off_datetime_col(
        self, dataframe: DataFrame
    ) -> DataFrame:
        """
        Extracting the month and year from the datetime column
        :param dataframe:
        :return: Dataframe with an additional column that has the year and month
        """
        self.__logger.info("Extracting month and year from `tpep_dropoff_date` Column")
        return dataframe.withColumn(
            "tpep_dropoff_month_year", date_format(col("tpep_dropoff_date"), "yyyy-MM")
        )
