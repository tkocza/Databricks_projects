from db_flights.shared.metadata import TECHNICAL_COLUMNS

from db_flights.shared.writer import overwrite_table
from db_flights.shared.reader import read_table
from db_flights.shared.dates_transformations import create_date_column, create_timestamp_column

def transform_flights():

    df = read_table("db_flights.bronze.flights")

    rename_cols_map_airports = {
    'airline': 'airline_code'
    }
    df = df.withColumnsRenamed(rename_cols_map_airports)

    df = (
        df
        .withColumn("flight_date", create_date_column("year", "month", "day"))
        .withColumn("scheduled_departure_timestamp", create_timestamp_column("flight_date", "scheduled_departure"))
        .withColumn("scheduled_arrival_timestamp", create_timestamp_column("flight_date", "scheduled_arrival"))
        .withColumn("departure_timestamp", create_timestamp_column("flight_date", "departure_time"))
        .withColumn("arrival_timestamp", create_timestamp_column("flight_date", "arrival_time"))
    )

    overwrite_table(df, "db_flights.silver.flights")

def main():
    transform_flights()
    

if __name__ == "__main__":
    main()