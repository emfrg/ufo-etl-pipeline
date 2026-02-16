import pytest
from pyspark.sql import SparkSession


@pytest.fixture()
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()
