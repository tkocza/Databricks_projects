from pyspark import pipelines as dp
from pyspark.sql import functions as F

from db_flights_lakeflow.shared.metadata import TECHNICAL_COLUMNS
from db_flights_lakeflow.shared.hash import build_row_hash


@dp.table(name="db_flights_lakeflow.silver.airports")
# errors and termination
@dp.expect_or_fail("airport_code_not_null", "airport_code IS NOT NULL")
# warnings
@dp.expect("airport_code_valid_length", "length(airport_code) = 3")
def silver_airports():

    df = spark.read.table("db_flights_lakeflow.bronze.airports")

    rename_cols_map = {
        "iata_code": "airport_code"
    }

    df = df.withColumnsRenamed(rename_cols_map)

    exclude_key = TECHNICAL_COLUMNS + ["airport_code"]
    cols_to_hash = [c for c in df.columns if c not in exclude_key]

    df = df.withColumn("row_hash", build_row_hash(cols_to_hash))

    return df