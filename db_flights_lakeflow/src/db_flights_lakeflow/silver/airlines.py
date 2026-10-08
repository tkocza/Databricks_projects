from datetime import datetime

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from db_flights_lakeflow.shared.metadata import TECHNICAL_COLUMNS, SCD_VALID_FROM

BRONZE = "db_flights_lakeflow.bronze.airlines"
SILVER = "db_flights_lakeflow.silver.airlines"


@dp.view(name="airlines_latest")
@dp.expect_or_fail("airline_code_not_null", "airline_code IS NOT NULL")
@dp.expect("airline_code_valid_length", "length(airline_code) = 2")
def airlines_latest():

    bronze = spark.read.table(BRONZE)
    
    # find the new batch of records
    latest_load = bronze.agg(F.max("load_date").alias("load_date"))

    df = bronze.join(latest_load, "load_date", "left_semi") \
                .withColumnsRenamed({"iata_code": "airline_code"}) \
                .drop(*TECHNICAL_COLUMNS)
    return df


dp.create_streaming_table(SILVER)

dp.create_auto_cdc_from_snapshot_flow(
    target=SILVER,
    source="airlines_latest",
    keys=["airline_code"],
    stored_as_scd_type=2,
)
