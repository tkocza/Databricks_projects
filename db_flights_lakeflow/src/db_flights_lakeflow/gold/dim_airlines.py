from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

from db_flights_lakeflow.shared.metadata import SCD_VALID_FROM, SCD_VALID_TO

SILVER = "db_flights_lakeflow.silver.airlines"
GOLD = "db_flights_lakeflow.gold.dim_airlines"

@dp.materialized_view(
    name=GOLD,
    comment="Airlines dimension (SCD2) built from silver.airlines. "
            "Includes special records -1 (UNKNOWN) and -999 (QUARANTINED).",
    schema="""
        airline_id BIGINT COMMENT 'Surrogate key for the airlines dimension (hash of airline_code and valid_from; -1 and -999 are special records)',
        airline_code STRING NOT NULL COMMENT 'Airline IATA code (2-letter)',
        airline STRING COMMENT 'Full airline name',
        valid_from TIMESTAMP COMMENT 'Timestamp from which this dimension version is valid',
        valid_to TIMESTAMP COMMENT 'Timestamp until which this dimension version is valid',
        is_current BOOLEAN COMMENT 'Indicates whether this is the current version of the record'
    """,
)
def dim_airlines():
    silver = spark.read.table(SILVER)
    initial_rows = silver.agg(F.min("__START_AT").alias("initial_start"))

    dim = (
        silver.crossJoin(initial_rows)
        .select(
            "airline_code",
            "airline",
            F.when(
                F.col("__START_AT") == F.col("initial_start"),
                F.lit(SCD_VALID_FROM).cast("timestamp"),
            ).otherwise(F.col("__START_AT")).alias("valid_from"),
            F.coalesce(
                F.col("__END_AT") - F.expr("INTERVAL 1 SECOND"),
                F.lit(SCD_VALID_TO).cast("timestamp"),
            ).alias("valid_to"),
            F.col("__END_AT").isNull().alias("is_current"),
        )
        # if you allow negative hash
        # .withColumn("airline_id", F.xxhash64("airline_code", "valid_from"))
        # if you prefer only positive numbers in hash
        .withColumn("airline_id", F.xxhash64("airline_code", "valid_from").bitwiseAND(F.lit(9223372036854775807)),)
        )

    special_records = (
        spark.createDataFrame(
            [
                (-1, "UNKNOWN", "Unknown Airline"),
                (-999, "QUARANTINED", "Missing Airline"),
            ],
            "airline_id long, airline_code string, airline string",
        )
        .withColumn("valid_from", F.lit(SCD_VALID_FROM).cast("timestamp"))
        .withColumn("valid_to", F.lit(SCD_VALID_TO).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    df = dim.unionByName(special_records).select("airline_id", "airline_code", "airline", "valid_from", "valid_to", "is_current")

    return df