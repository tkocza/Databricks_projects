from pyspark.sql import SparkSession
import sys
import os

from db_flights.config.tables.bronze_kaggle_cfg import INGESTION_CONFIG
from db_flights.shared.metadata import *
from db_flights.shared.columns import *

def read_source(spark, table_cfg):
    return (
        spark.read
        .schema(table_cfg["table_schema"])
        .option("header", True)
        .csv(table_cfg["path"])
    )

def write_table(df, table_cfg):

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true") # if structure changes, overwrite the schema will follow this changes
        .saveAsTable(table_cfg["target_table"])
    )
    

def run_bronze_ingestion(spark):

    for table_cfg in INGESTION_CONFIG:
        print(f"Ingesting {table_cfg['source']}.{table_cfg['src_table_name']} into {table_cfg['target_table']}")
        df = read_source(spark, table_cfg)

        df = add_technical_columns(df, table_cfg)
        df = to_lowercase_columns(df)

        write_table(df, table_cfg)

def main():
    spark = SparkSession.builder.getOrCreate()
    run_bronze_ingestion(spark)
    

if __name__ == "__main__":
    run_bronze_ingestion()