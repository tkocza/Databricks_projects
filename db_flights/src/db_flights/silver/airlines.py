from db_flights.shared.writer import overwrite_table
from db_flights.shared.reader import read_table


def transform_airlines():

    df = read_table("db_flights.bronze.airlines")

    rename_cols_map_airlines = {
        'iata_code': 'airline_code'
    }
    df = df.withColumnsRenamed(rename_cols_map_airlines)

    overwrite_table(df, "db_flights.silver.airlines")
    

def main():
    transform_airlines()
    

if __name__ == "__main__":
    main()