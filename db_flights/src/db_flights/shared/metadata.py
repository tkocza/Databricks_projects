from pyspark.sql import functions as F

TECHNICAL_COLUMNS = [
    "load_date",
    "source_file"
]

def add_technical_columns(df, table_cfg):
    return (
        df
        .withColumn('load_date', F.current_timestamp())
        .withColumn('source_file', F.lit(table_cfg["src_table_name"]))
    )

