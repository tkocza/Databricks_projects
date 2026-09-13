from db_flights_lakeflow.config.schemas.kaggle_schemas import *

INGESTION_CONFIG = [
    {   "src_table_name": "flights",
        "source": "kaggle",
        "path": "/Volumes/db_flights/raw_data/kaggle_datasets",
        "file_name": "flights.csv",
        "table_schema": SCHEMA_FLIGHTS,
        "target_table": "db_flights_lakeflow.bronze.flights"
    },
    {   "src_table_name": "airports",
        "source": "kaggle",
        "path": "/Volumes/db_flights/raw_data/kaggle_datasets",
        "file_name": "airports.csv",
        "table_schema": SCHEMA_AIRPORTS,
        "target_table": "db_flights_lakeflow.bronze.airports"
    },
    {   "src_table_name": "airlines",
        "source": "kaggle",
        "path": "/Volumes/db_flights/raw_data/kaggle_datasets",
        "file_name": "airlines.csv",
        "table_schema": SCHEMA_AIRLINES,
        "target_table": "db_flights_lakeflow.bronze.airlines"
    }
]