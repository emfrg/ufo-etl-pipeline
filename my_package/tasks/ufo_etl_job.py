from pyspark.sql.functions import (
    to_timestamp, col, year, month, floor, upper, avg, count, min, max,
)
from pyspark.sql.types import IntegerType

from my_package.common import Task


DEFAULT_INPUT_PATH = "/Volumes/workspace/default/raw_data/scrubbed.csv"


class UfoEtlTask(Task):
    def __init__(self, spark=None, input_path=None):
        super().__init__(spark)
        self.input_path = input_path or DEFAULT_INPUT_PATH

    def _clean_and_enrich(self):
        self.logger.info("1. Reading raw UFO sightings CSV")
        df = self.spark.read.csv(self.input_path, header=True, inferSchema=True)

        self.logger.info("2. Parsing datetime and extracting date parts")
        df = df.withColumn("timestamp", to_timestamp(col("datetime"), "M/d/yyyy H:mm"))
        df = df.withColumn("year", year(col("timestamp")))
        df = df.withColumn("month", month(col("timestamp")))
        df = df.withColumn("decade", (floor(col("year") / 10) * 10).cast(IntegerType()))

        self.logger.info("3. Casting duration and uppercasing state/country")
        df = df.withColumn("duration_seconds", col("duration (seconds)").cast(IntegerType()))
        df = df.withColumn("state", upper(col("state")))
        df = df.withColumn("country", upper(col("country")))

        self.logger.info("4. Dropping rows with null state or shape")
        df = df.filter(col("state").isNotNull() & col("shape").isNotNull())

        self.logger.info("5. Dropping original columns with invalid Delta characters")
        df = df.drop("duration (seconds)", "duration (hours/min)", "date posted", "datetime")

        df.write.mode("OVERWRITE").saveAsTable("ufo_sightings_cleaned")

    def _aggregate_by_state_and_shape(self):
        self.logger.info("5. Aggregating sightings by state and shape")
        df = self.spark.table("ufo_sightings_cleaned")

        df_agg = df.groupBy("state", "shape").agg(
            count("*").alias("sighting_count"),
            avg("duration_seconds").alias("avg_duration"),
            min("year").alias("earliest_year"),
            max("year").alias("latest_year"),
        )

        df_agg.write.mode("OVERWRITE").saveAsTable("ufo_sightings_agg")

    def launch(self):
        self.logger.info("Launching UFO ETL task")
        self._clean_and_enrich()
        self._aggregate_by_state_and_shape()
        self.logger.info("UFO ETL task finished!")


# Entry point for python_wheel_task (referenced in setup.py)
def entrypoint():  # pragma: no cover
    task = UfoEtlTask()
    task.launch()


if __name__ == "__main__":
    entrypoint()
