from pyspark import pipelines as dp

from db_flights_lakeflow.shared.metadata import TECHNICAL_COLUMNS
from db_flights_lakeflow.shared.dates_transformations import create_date_column, create_timestamp_column

@dp.table(name="db_flights_lakeflow.silver.flights")
# errors and termination
@dp.expect_or_fail("flight_date_not_null", "flight_date IS NOT NULL")
@dp.expect_or_fail("flight_number_not_null", "flight_number IS NOT NULL")
@dp.expect_or_fail("airline_code_not_null", "airline_code IS NOT NULL")
@dp.expect_or_fail("origin_airport_not_null", "origin_airport IS NOT NULL")
@dp.expect_or_fail("destination_airport_not_null", "destination_airport IS NOT NULL")

def transform_flights():

    df = spark.read.table("db_flights_lakeflow.bronze.flights")

    rename_cols_map_flights = {
    'airline': 'airline_code'
    }
    df = df.withColumnsRenamed(rename_cols_map_flights)

    df = (
        df
        .withColumn("flight_date", create_date_column("year", "month", "day"))
        .withColumn("scheduled_departure_timestamp", create_timestamp_column("flight_date", "scheduled_departure"))
        .withColumn("scheduled_arrival_timestamp", create_timestamp_column("flight_date", "scheduled_arrival"))
        .withColumn("departure_timestamp", create_timestamp_column("flight_date", "departure_time"))
        .withColumn("arrival_timestamp", create_timestamp_column("flight_date", "arrival_time"))
    )

    return df