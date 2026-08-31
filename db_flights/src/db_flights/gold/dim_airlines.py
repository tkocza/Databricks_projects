from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from db_flights.shared.reader import read_table


spark = SparkSession.builder.getOrCreate()

SOURCE_TABLE = "db_flights.silver.airlines"
TARGET_TABLE = "db_flights.gold.dim_airlines"


def create_initial_dimension(source):

    df = (
        source
        .withColumn("valid_from", F.to_timestamp(F.lit("1900-01-01")))
        .withColumn("valid_to", F.to_timestamp(F.lit("3000-12-31")))
        .withColumn("is_current", F.lit(True))
    )

    df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)


def update_dimension(source):

    target = DeltaTable.forName(spark, TARGET_TABLE)

    current_dim = read_table(TARGET_TABLE).filter(F.col("is_current"))

    # full join is necessary to detect 3 cases: new, changed, deleted
    changes = (
        source.alias("new")
        .join(current_dim.alias("old"), "airline_code", "full")
        .withColumn(
            "record_status",
            F.when(F.col("old.airline_code").isNull(), "NEW")
             .when(F.col("new.airline_code").isNull(), "DELETED")
             .when(F.col("new.row_hash") != F.col("old.row_hash"), "CHANGED")
             .otherwise("UNCHANGED")
        )
    )

    # Expire changed and deleted records
    records_to_expire = (
        changes
        .filter(F.col("record_status").isin("CHANGED", "DELETED"))
        .select("airline_code")
        .distinct()
    )

    (
        target.alias("target")
        .merge(
            records_to_expire.alias("source"),
            "target.airline_code = source.airline_code AND target.is_current = true"
        )
        .whenMatchedUpdate(
            set={
                "valid_to": "current_timestamp()",
                "is_current": "false"
            }
        )
        .execute()
    )

    # Insert new versions
    records_to_insert = (
        changes
        .filter(F.col("record_status").isin("NEW", "CHANGED"))
        .select("new.*")
        .withColumn("valid_from", F.current_timestamp())
        .withColumn("valid_to", F.to_timestamp(F.lit("3000-12-31")))
        .withColumn("is_current", F.lit(True))
    )

    records_to_insert.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)


def update_dim_airlines():

    source = read_table(SOURCE_TABLE)

    if not spark.catalog.tableExists(TARGET_TABLE):
        create_initial_dimension(source)
    else:
        update_dimension(source)


def main():
    update_dim_airlines()


if __name__ == "__main__":
    main()