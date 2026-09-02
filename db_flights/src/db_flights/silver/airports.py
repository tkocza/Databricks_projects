from db_flights.shared.metadata import TECHNICAL_COLUMNS

from db_flights.shared.writer import overwrite_table
from db_flights.shared.reader import read_table
from db_flights.shared.hash import build_row_hash


def transform_airports():

    df = read_table("db_flights.bronze.airports")

    rename_cols_map_airports = {
    'iata_code': 'airport_code'
    }
    df = df.withColumnsRenamed(rename_cols_map_airports)

    exclude_key = TECHNICAL_COLUMNS + ["airport_code"]
    cols_to_hash = [c for c in df.columns if c not in exclude_key]
    df = df.withColumn("row_hash", build_row_hash(cols_to_hash))

    overwrite_table(df, "db_flights.silver.airports")
    

def main():
    transform_airports()
    

if __name__ == "__main__":
    main()