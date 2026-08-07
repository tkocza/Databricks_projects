from db_flights.shared.writer import overwrite_table
from db_flights.shared.reader import read_table


def transform_airports():

    df = read_table("db_flights.bronze.airports")

    rename_cols_map_airports = {
    'iata_code': 'airport_code'
}
    df = df.withColumnsRenamed(rename_cols_map_airports)

    overwrite_table(df, "db_flights.silver.airports")
    

def main():
    transform_airports()
    

if __name__ == "__main__":
    main()