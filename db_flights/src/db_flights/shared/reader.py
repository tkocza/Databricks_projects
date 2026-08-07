from pyspark.sql import DataFrame, SparkSession


def read_table(table_name: str) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()

    return spark.read.table(table_name)