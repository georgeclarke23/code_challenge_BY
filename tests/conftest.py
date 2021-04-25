import logging

import pytest
from pyspark.sql import SparkSession

from src.jobs.code_challenge.app.context import Context


@pytest.fixture
def fake_logging():
    return logging


@pytest.fixture
def fake_spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("my-local-testing-pyspark-context")
        .enableHiveSupport()
        .getOrCreate()
    )


@pytest.fixture
def fake_context(fake_spark, fake_logging):
    return Context(app_name="Fake", spark=fake_spark, logger=fake_logging)
