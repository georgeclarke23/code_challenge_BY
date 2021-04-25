import logging
import unittest

from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession

from src.jobs.code_challenge.app.context import Context

class PySparkTest(unittest.TestCase):
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
        cls.log = logging.getLogger(__name__)
        cls.context = Context("test_app", cls.spark, cls.log)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @classmethod
    def assert_dataframe(cls, func, expected_df, pandas_df):
        df = cls.spark.createDataFrame(pandas_df)
        actual_spark_df = func(df, cls.log)
        actual_df = actual_spark_df.toPandas()
        assert_frame_equal(left=actual_df, right=expected_df, check_dtype=False)