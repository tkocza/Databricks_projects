from delta.tables import DeltaTable
from pyspark.sql import SparkSession

'''
DDL is executed via spark.sql() to keep the public portfolio environment-independent
and avoid hardcoding a workspace-specific warehouse ID.
'''

spark = SparkSession.builder.getOrCreate()

TARGET_TABLE = "db_flights.gold.dim_time"

START_DATE = "2000-01-01"
END_DATE = "2050-12-31"


def create_dim_time_table():

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            date_id BIGINT
                COMMENT 'Date key in YYYYMMDD format',

            date DATE
                COMMENT 'Calendar date',

            year INT
                COMMENT 'Calendar year',

            quarter INT
                COMMENT 'Calendar quarter (1-4)',

            month INT
                COMMENT 'Month number (1-12)',

            month_name STRING
                COMMENT 'Full month name',

            month_short STRING
                COMMENT 'Abbreviated month name',

            week INT
                COMMENT 'Week number of the year',

            day INT
                COMMENT 'Day of month',

            day_of_week INT
                COMMENT 'Day of week (1 = Sunday, 7 = Saturday)',

            day_of_year INT
                COMMENT 'Day of year (1-366)',

            day_name STRING
                COMMENT 'Full day name',

            year_month STRING
                COMMENT 'Year and month in YYYY-MM format',

            year_week STRING
                COMMENT 'Year and week in YYYY-Www format',

            first_day_of_month DATE
                COMMENT 'First day of the month',

            last_day_of_month DATE
                COMMENT 'Last day of the month',

            is_month_end BOOLEAN
                COMMENT 'Indicates whether the date is the last day of the month',

            is_weekend BOOLEAN
                COMMENT 'Indicates whether the date falls on Saturday or Sunday',

            holiday_name STRING
                COMMENT 'Name of the holiday, if applicable',

            is_holiday BOOLEAN
                COMMENT 'Indicates whether the date is a holiday',

            CONSTRAINT pk_dim_time PRIMARY KEY (date_id)
        )
        USING DELTA
    """)


def load_dim_time():

    dim_time = spark.sql(f"""
        SELECT
            date,
            CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_id,
            year(date) AS year,
            quarter(date) AS quarter,
            month(date) AS month,
            date_format(date, 'MMMM') AS month_name,
            date_format(date, 'MMM') AS month_short,
            weekofyear(date) AS week,
            dayofmonth(date) AS day,
            dayofweek(date) AS day_of_week,
            dayofyear(date) AS day_of_year,
            date_format(date, 'EEEE') AS day_name,
            date_format(date, 'yyyy-MM') AS year_month,
            concat(year(date), '-W', lpad(weekofyear(date), 2, '0')) AS year_week,
            trunc(date, 'month') AS first_day_of_month,
            last_day(date) AS last_day_of_month,
            date = last_day(date) AS is_month_end,
            dayofweek(date) IN (1, 7) AS is_weekend,
            CAST(NULL AS STRING) AS holiday_name,
            FALSE AS is_holiday
        FROM (
            SELECT explode(
                sequence(
                    to_date('{START_DATE}'),
                    to_date('{END_DATE}'),
                    interval 1 day
                )
            ) AS date
        )
    """)

    target = DeltaTable.forName(spark, TARGET_TABLE)

    (
        target.alias("target")
        .merge(
            dim_time.alias("source"),
            "target.date_id = source.date_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def main():

    create_dim_time_table()
    load_dim_time()


if __name__ == "__main__":
    main()