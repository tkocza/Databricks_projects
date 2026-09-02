from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from db_flights.shared.metadata import TECHNICAL_COLUMNS
from db_flights.shared.metadata import SCD_VALID_FROM, SCD_VALID_TO

from db_flights.shared.reader import read_table


spark = SparkSession.builder.getOrCreate()

SOURCE_TABLE = "db_flights.silver.airports"
TARGET_TABLE = "db_flights.gold.dim_airports"


def load_dimension():

    source = read_table(SOURCE_TABLE).drop(*TECHNICAL_COLUMNS)
    target = DeltaTable.forName(spark, TARGET_TABLE)

    current_dim = (
        read_table(TARGET_TABLE)
        .filter(
            F.col("is_current") & (~F.col("airport_id").isin(-1, -999))
        )
    )

    # Initial load
    if current_dim.isEmpty():

        records_to_insert = (
            source
            .withColumn("valid_from", F.to_timestamp(F.lit(SCD_VALID_FROM)))
            .withColumn("valid_to", F.to_timestamp(F.lit(SCD_VALID_TO)))
            .withColumn("is_current", F.lit(True))
        )

        records_to_insert.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
        return

    # Detect new, changed and deleted records
    changes = (
        source.alias("new")
        .join(current_dim.alias("old"), "airport_code", "full")
        .withColumn(
            "record_status",
            F.when(F.col("old.airport_code").isNull(), "NEW")
             .when(F.col("new.airport_code").isNull(), "DELETED")
             .when(F.col("new.row_hash") != F.col("old.row_hash"), "CHANGED")
             .otherwise("UNCHANGED")
        )
    )

    # Expire changed and deleted records
    records_to_expire = (
        changes
        .filter(F.col("record_status").isin("CHANGED", "DELETED"))
        .select("airport_code")
        .distinct()
    )

    (
        target.alias("target")
        .merge(
            records_to_expire.alias("source"),
            "target.airport_code = source.airport_code AND target.is_current = true"
        )
        # "- INTERVAL 1 SECOND" to get 23:59:59 day before
        .whenMatchedUpdate(
            set={
                "valid_to": "CAST(current_date() AS TIMESTAMP) - INTERVAL 1 SECOND",
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
        .withColumn("valid_from", F.current_date().cast("timestamp"))
        .withColumn("valid_to", F.to_timestamp(F.lit(SCD_VALID_TO)))
        .withColumn("is_current", F.lit(True))
    )

    records_to_insert.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)

def main():
    load_dimension()


if __name__ == "__main__":
    main()