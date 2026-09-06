from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

TABLE = "db_flights.gold.fact_flights"

# Z-ORDER on frequently filtered/joined columns and flight_date is already used for partitioning, so it is excluded from Z-ORDER
ZORDER_COLUMNS = ["airline_id", "origin_airport_id", "destination_airport_id"]


def optimize_table(table: str, zorder_columns: list[str]) -> None:

    columns = ", ".join(zorder_columns)

    # Run OPTIMIZE after the Gold load to compact files and organize the final dataset.
    spark.sql(f"""
        OPTIMIZE {table}
        ZORDER BY ({columns})
    """)


def main():
    optimize_table(TABLE, ZORDER_COLUMNS)


if __name__ == "__main__":
    main()