import argparse
import importlib
import os
import sys

from pyspark.sql import SparkSession


def main(args):
    """
    Main entry point to run the selected spark job. In here we create a spark session
    that is passed to the spark job
    :param args: command line arguments
    """
    try:
        spark = SparkSession.builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        job_module = importlib.import_module("jobs.%s" % args.job)
        job_module.run(spark)
    except Exception:
        sys.exit(1)

if __name__ == "__main__":
    if os.path.exists("jobs.zip"):
        sys.path.insert(0, "jobs.zip")
    else:
        sys.path.insert(0, "./jobs")

    parser = argparse.ArgumentParser()
    parser.add_argument("--job", type=str, required=True)
    parsed_args = parser.parse_args()

    main(parsed_args)
