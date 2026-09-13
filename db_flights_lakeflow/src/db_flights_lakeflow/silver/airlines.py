from pyspark import pipelines as dp

from db_flights_lakeflow.shared.metadata import TECHNICAL_COLUMNS
from db_flights_lakeflow.shared.hash import build_row_hash

@dp.table(name="db_flights_lakeflow.silver.airlines")
# errors and termination
@dp.expect_or_fail("airline_code_not_null", "airline_code IS NOT NULL")
# warnings
@dp.expect("airline_code_valid_length", "length(airline_code) = 2")
def transform_airlines():

    df = spark.read.table("db_flights_lakeflow.bronze.airlines")

    rename_cols_map_airlines = {
        'iata_code': 'airline_code'
    }
    df = df.withColumnsRenamed(rename_cols_map_airlines)

    exclude_key = TECHNICAL_COLUMNS + ["airline_code"]
    cols_to_hash = [c for c in df.columns if c not in exclude_key]
    df = df.withColumn("row_hash", build_row_hash(cols_to_hash))

    return df