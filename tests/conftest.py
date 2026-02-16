import logging
import shutil
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

@pytest.fixture()
def spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").getOrCreate()