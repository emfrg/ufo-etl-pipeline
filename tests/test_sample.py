#import pytest
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from my_package.tasks.sample_etl_job import SampleETLTask

def test_jobs():
    etl_job = SampleETLTask()
    etl_job.launch()
    
    df_trip_duration = etl_job.read_managed_table("nyc_taxi_with_trip_duration")
    assert "duration_seconds" in df_trip_duration.columns

    null_count = df_trip_duration.where(col('duration_seconds').isNull()).count()
    assert null_count==0

    count_zip_code = df_trip_duration.select(col("pickup_zip")).distinct().count()
    assert count_zip_code>0

    df_zip_agg_vals = etl_job.read_managed_table("nyc_taxi_zip_agg_vals")
    aggregated_cols = df_zip_agg_vals.columns
    assert "avgFare" in aggregated_cols
    assert "avgTripDist" in aggregated_cols
    assert "avgTripDur" in aggregated_cols
    assert df_zip_agg_vals.count()>0

if __name__ == '__main__':
    test_jobs()