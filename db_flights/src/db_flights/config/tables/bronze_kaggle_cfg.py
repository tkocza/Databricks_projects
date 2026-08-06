from db_flights.config.schemas.kaggle_schemas import *

INGESTION_CONFIG = [
    {   "src_table_name": "flights",
        "source": "kaggle",
        "path": "/Volumes/db_flights/raw_data/kaggle_datasets/flights.csv",
        "table_schema": SCHEMA_FLIGHTS,
        "target_table": "db_flights.bronze.flights"
    },
    {   "src_table_name": "airports",
        "source": "kaggle",
        "path": "/Volumes/db_flights/raw_data/kaggle_datasets/airports.csv",
        "table_schema": SCHEMA_AIRPORTS,
        "target_table": "db_flights.bronze.airports"
    },
    {   "src_table_name": "airlines",
        "source": "kaggle",
        "path": "/Volumes/db_flights/raw_data/kaggle_datasets/airlines.csv",
        "table_schema": SCHEMA_AIRLINES,
        "target_table": "db_flights.bronze.airlines"
    }
]