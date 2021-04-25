import os

from pyspark.sql import DataFrame

from ..utils.containers import ExecutionContainer


class Ingest(ExecutionContainer):
    """
    Ingestion Class
    - Ingest the CSV(s) into memory
    """
    def __init__(self, context):
        super().__init__(context)
        self.context = context
        self.spark = self.context.spark
        self.__logger = self.context.logger.getLogger(__name__)
        self.source_path = os.path.join(os.getcwd(), self.context.get("SOURCE_PATH"), "*.csv")

    def execute(self) -> DataFrame:
        """
        Ingesting all dataframes under the specified directory into memory.
        :return:
        """
        self.__logger.info(f"Reading file from {self.source_path}")
        self.dataframe = self.spark.read.option("inferSchema", "true").csv(self.source_path, header=True, schema=None)
        self.__logger.info(f"Successfully read file from {self.source_path}")
        return self.dataframe
