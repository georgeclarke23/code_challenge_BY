import logging
import time

from pyspark.sql import SparkSession

from .app.context import Context
from .app.display.display import Display
from .app.ingest.ingest import Ingest
from .app.transform.transform import Transform


def run(spark: SparkSession):
    """
    Entry point for this job
    :param spark: Spark session passed from the Main Jobs Interface
    :return:
    """
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    logger = logging.getLogger(__name__)
    try:
        start = time.time()

        context = Context("BY_Engineering_Code_Challenge", spark, logging)

        logger.info("Application Started")

        # Ingest CSV from source path supplied in the ENV
        logger.info("Ingesting data")
        ingest = Ingest(context).exec()

        logger.info("Transforming ingested data")
        transform = Transform(context, ingest).exec()

        logger.info("Displaying the data")
        Display(context, transform).exec()

        logger.info("Application Successfully Completed")
        end = time.time()
        logger.info("Process Finished take {0:.2f} secs".format((end - start)))

    except Exception as e:
        logger.error("Job failed: %s", e)
        raise Exception("Failure Occured whiles running the code challenge job") from e
