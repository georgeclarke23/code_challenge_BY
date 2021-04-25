import datetime
import logging
from unittest import TestCase

import pandas as pd
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession

from src.jobs.code_challenge.app.context import Context
from src.jobs.code_challenge.app.display.display import Display
from src.jobs.code_challenge.app.ingest.ingest import Ingest
from src.jobs.code_challenge.app.transform.transform import Transform


class PySparkTest(TestCase):
    spark = None

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (
            SparkSession.builder.master("local[2]")
            .appName("my-local-testing-pyspark-context")
            .enableHiveSupport()
            .getOrCreate()
        )

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()
        cls.log = logging
        cls.context = Context("test_app", cls.spark, cls.log)
        cls.context.set({"SOURCE_PATH": "data/"})
        cls.ingest = Ingest(cls.context)
        cls.pandas_df = pd.DataFrame(
            {
                "tpep_dropoff_datetime": ["2021-03-23", "2021-03-24"],
                "trip_distance": [2, 2],
                "remove": ["test", "test"],
            }
        )
        cls.ingest.dataframe = cls.spark.createDataFrame(cls.pandas_df)
        cls.transform = Transform(cls.context, cls.ingest)
        cls.display = Display(cls.context, cls.transform.dataframe)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @classmethod
    def assert_dataframe(cls, func, expected_df, pandas_df):
        df = cls.spark.createDataFrame(pandas_df)
        actual_spark_df = func(df)
        actual_df = actual_spark_df.toPandas()
        assert_frame_equal(left=actual_df, right=expected_df, check_dtype=False)
