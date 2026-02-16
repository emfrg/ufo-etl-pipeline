from pyspark.sql.functions import to_timestamp,avg
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

from my_package.common import Task



class SampleETLTask(Task):
    def _add_trip_duration(self):
        file_location = "dbfs:/databricks-datasets/nyctaxi-with-zipcodes/subsampled"
        format = "delta"
        df = self.spark.read.load(path=file_location, format="delta")
        
        self.logger.info("1. compute the trip duration")
        df = df.withColumn(
            "duration_seconds",
            (
                df.tpep_dropoff_datetime
                - df.tpep_pickup_datetime
            ).cast(IntegerType()),
        )
        df.write.mode("OVERWRITE").saveAsTable("nyc_taxi_with_trip_duration")
          
    def _calc_avg_fair_per_zipcode(self):
        self.logger.info("2. compute average fare for each zipcode")
        df = self.spark.table("nyc_taxi_with_trip_duration")
        # Compute aggregated data
        agg_funcs = [avg("fare_amount").alias("avgFare"), 
                    avg("trip_distance").alias("avgTripDist"), 
                    avg("duration_seconds").alias("avgTripDur"), 
                ]
                
        df_zip_agg = df.groupBy("pickup_zip").agg(*agg_funcs)
        df_zip_agg.write.mode("OVERWRITE").saveAsTable("nyc_taxi_zip_agg_vals")

    def launch(self):
        self.logger.info("Launching sample ETL task")
        self.logger.info("Load dataset")

        self._add_trip_duration()
        self._calc_avg_fair_per_zipcode()
        
        self.logger.info("Sample ETL task finished!")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = SampleETLTask()
    task.launch()
    
if __name__ == "__main__":
    entrypoint()
