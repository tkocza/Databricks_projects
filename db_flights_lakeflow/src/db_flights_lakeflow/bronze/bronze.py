from pyspark import pipelines as dp
from pyspark.sql import functions as F

from db_flights_lakeflow.config.tables.bronze_kaggle_cfg import INGESTION_CONFIG
from db_flights_lakeflow.shared.columns import to_lowercase_columns
from db_flights_lakeflow.shared.metadata import add_technical_columns

def create_bronze_table(table_cfg):

    table_name = table_cfg["target_table"]
    table_path = table_cfg["path"]
    table_schema = table_cfg["table_schema"]
    file_name = table_cfg["file_name"]

    @dp.table(name=table_name)
    def bronze_table():

        # Auto Loader reads directories; pathGlobFilter selects the required source file
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("pathGlobFilter", file_name)
            .schema(table_schema)
            .option("header", True)
            .load(table_path)
        )

        df = add_technical_columns(df, table_cfg)
        df = to_lowercase_columns(df)

        return df


for table_cfg in INGESTION_CONFIG:
    create_bronze_table(table_cfg)