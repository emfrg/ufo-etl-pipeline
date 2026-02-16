import os

from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

from my_package.tasks.ufo_etl_job import UfoEtlTask


def test_ufo_etl():
    # --- 1. Create synthetic CSV mimicking scrubbed.csv schema ---
    header = "datetime,city,state,country,shape,duration (seconds),duration (hours/min),comments,date posted,latitude,longitude"
    rows = [
        # Valid rows (should survive cleaning)
        '10/10/1949 20:30,san marcos,tx,us,cylinder,2700,45 minutes,saw something,4/27/2004,29.8830556,-97.9411111',
        '6/15/1995 14:00,phoenix,az,us,light,120,2 minutes,bright light,1/1/2000,33.4483771,-112.0740373',
        '3/20/2005 22:15,seattle,wa,us,triangle,600,10 minutes,three lights,5/1/2005,47.6062095,-122.3320708',
        '12/1/2010 03:00,portland,or,us,fireball,30,30 seconds,fireball seen,1/15/2011,45.5234515,-122.6762071',
        '7/4/2015 21:00,denver,co,us,circle,300,5 minutes,round object,8/1/2015,39.7392358,-104.990251',
        # Rows with null state or shape (should be DROPPED by _clean_and_enrich)
        '1/1/2000 00:00,london,,gb,disk,60,1 minute,hovering disk,2/1/2000,51.5073509,-0.1277583',
        '5/5/2020 10:00,nowhere,,us,oval,100,1 minute,saw oval,6/1/2020,40.0,-90.0',
        '8/8/2018 12:00,place,ca,us,,200,3 minutes,no shape,9/1/2018,35.0,-118.0',
    ]

    # Write to a Unity Catalog Volume path (FUSE-mounted on Databricks).
    # Local /tmp/ paths are interpreted as DBFS on serverless, which is disabled.
    csv_path = "/Volumes/workspace/default/raw_data/test_ufo.csv"
    try:
        with open(csv_path, "w") as f:
            f.write(header + "\n")
            for row in rows:
                f.write(row + "\n")

        # --- 2. Run ETL task with synthetic data ---
        task = UfoEtlTask(input_path=csv_path)
        task.launch()

        # --- 3. Assert on ufo_sightings_cleaned ---
        df_cleaned = task.spark.table("ufo_sightings_cleaned")

        # Required columns exist
        for col_name in ["year", "month", "decade", "duration_seconds", "timestamp"]:
            assert col_name in df_cleaned.columns, f"Missing column: {col_name}"

        # Timestamp is proper type
        assert df_cleaned.schema["timestamp"].dataType == TimestampType()

        # No nulls in state or shape (the 2 bad rows should be dropped)
        assert df_cleaned.filter(col("state").isNull()).count() == 0
        assert df_cleaned.filter(col("shape").isNull()).count() == 0

        # State and country are uppercase
        states = [row.state for row in df_cleaned.select("state").collect()]
        for s in states:
            assert s == s.upper(), f"State not uppercase: {s}"

        countries = [row.country for row in df_cleaned.select("country").collect()]
        for c in countries:
            assert c == c.upper(), f"Country not uppercase: {c}"

        # Should have 5 rows (8 input - 3 dropped for null state/shape)
        assert df_cleaned.count() == 5

        # --- 4. Assert on ufo_sightings_agg ---
        df_agg = task.spark.table("ufo_sightings_agg")

        for col_name in ["sighting_count", "avg_duration", "earliest_year", "latest_year"]:
            assert col_name in df_agg.columns, f"Missing agg column: {col_name}"

        assert df_agg.count() > 0
        assert df_agg.filter(col("sighting_count") <= 0).count() == 0
    finally:
        if os.path.exists(csv_path):
            os.remove(csv_path)


if __name__ == "__main__":
    test_ufo_etl()
