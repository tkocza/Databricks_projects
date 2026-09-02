from pyspark.sql import functions as F

from db_flights.shared.metadata import TECHNICAL_COLUMNS
from db_flights.shared.reader import read_table

SOURCE_TABLE = "db_flights.silver.flights"
TARGET_TABLE = "db_flights.gold.fct_flights"
DIM_AIRPORTS = "db_flights.gold.dim_airports"
DIM_AIRLINES = "db_flights.gold.dim_airlines"

def load_fct_flights():

    df = read_table(SOURCE_TABLE).drop(*TECHNICAL_COLUMNS,
                                              'year',
                                              'month',
                                              'day',
                                              'scheduled_departure',
                                              'departure_time',
                                              'scheduled_arrival',
                                              'arrival_time')


    '''
    Business rules implementation (feature engineering).
    Mark missing values before lookups. If business keys are missing that is a quality data issue
    '''
    df = (
            df
            .withColumn(
                "flights_length_category",
                F.when(F.col("air_time") < 300, "short")
                .when(F.col("air_time") < 3600, "medium")
                .otherwise("long")
            )
            .withColumn(
                "departure_delay_category",
                F.when(F.col("departure_delay") < 0, "Early")
                .when(F.col("departure_delay") < 15, "On Time")
                .when(F.col("departure_delay") < 30, "Small Delay")
                .when(F.col("departure_delay") < 60, "Medium Delay")
                .when(F.col("departure_delay") < 120, "Long Delay")
                .otherwise("Critical Delay")
            )
            .withColumn(
                "arrival_delay_category",
                F.when(F.col("arrival_delay") < 0, "Early")
                .when(F.col("arrival_delay") < 15, "On Time")
                .when(F.col("arrival_delay") < 30, "Small Delay")
                .when(F.col("arrival_delay") < 60, "Medium Delay")
                .when(F.col("arrival_delay") < 120, "Long Delay")
                .otherwise("Critical Delay")
            )
            .fillna({
                "origin_airport": "QUARANTINED",
                "destination_airport": "QUARANTINED",
                "airline_code": "QUARANTINED"
            })
            .withColumn(
                "date_id",
                F.date_format("flight_date", "yyyyMMdd").cast("int")
            )
        )

    # -------------------
    # Lookup dimensions
    # -------------------

    dim_airports_lkp = read_table(DIM_AIRPORTS).select(
        "airport_code",
        "airport_id",
        "valid_from",
        "valid_to"
    )

    dim_airlines_lkp = read_table(DIM_AIRLINES).select(
        "airline_code",
        "airline_id",
        "valid_from",
        "valid_to"
    )

    # -------------------
    # Origin airport lookup
    # -------------------

    df = (
        df.alias("fct")
        .join(
            dim_airports_lkp.alias("d_ap"),
            (F.col("fct.origin_airport") == F.col("d_ap.airport_code")) &
            (F.col("fct.flight_date") >= F.col("d_ap.valid_from")) &
            (F.col("fct.flight_date") < F.col("d_ap.valid_to")),
            "left"
        )
        .select(
            "fct.*",
            F.coalesce(F.col("d_ap.airport_id"), F.lit(-1)).alias("origin_airport_id")
        )
    )

    # -------------------
    # Destination airport lookup
    # -------------------

    df = (
        df.alias("fct")
        .join(
            dim_airports_lkp.alias("d_ap"),
            (F.col("fct.destination_airport") == F.col("d_ap.airport_code")) &
            (F.col("fct.flight_date") >= F.col("d_ap.valid_from")) &
            (F.col("fct.flight_date") < F.col("d_ap.valid_to")),
            "left"
        )
        .select(
            "fct.*",
            F.coalesce(F.col("d_ap.airport_id"), F.lit(-1)).alias("destination_airport_id")
        )
    )

    # -------------------
    # Airline lookup
    # -------------------

    df = (
        df.alias("fct")
        .join(
            dim_airlines_lkp.alias("d_al"),
            (F.col("fct.airline_code") == F.col("d_al.airline_code")) &
            (F.col("fct.flight_date") >= F.col("d_al.valid_from")) &
            (F.col("fct.flight_date") < F.col("d_al.valid_to")),
            "left"
        )
        .select(
            "fct.*",
            F.coalesce(F.col("d_al.airline_id"), F.lit(-1)).alias("airline_id")
        )
    )

    df = df.withColumn("run_id", F.date_format(F.current_timestamp(), "yyyyMMddHHmmss"))

    # Select columns in the order expected by the target table
    column_order = [
        "run_id",
        "date_id",
        "origin_airport_id",
        "destination_airport_id",
        "airline_id",
        "flight_number",
        "airline_code",
        "tail_number",
        "origin_airport",
        "destination_airport",
        "departure_delay",
        "taxi_out",
        "wheels_off",
        "scheduled_time",
        "elapsed_time",
        "air_time",
        "distance",
        "wheels_on",
        "taxi_in",
        "arrival_delay",
        "diverted",
        "cancelled",
        "cancellation_reason",
        "air_system_delay",
        "security_delay",
        "airline_delay",
        "late_aircraft_delay",
        "weather_delay",
        "flight_date",
        "scheduled_departure_timestamp",
        "scheduled_arrival_timestamp",
        "departure_timestamp",
        "arrival_timestamp",
        "flights_length_category",
        "departure_delay_category",
        "arrival_delay_category",
    ]
    
    df.select(column_order).write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(TARGET_TABLE)

def main():
    load_fct_flights()
    

if __name__ == "__main__":
    main()