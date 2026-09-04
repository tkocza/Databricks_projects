from pyspark.sql import SparkSession

'''
DDL is executed via spark.sql() to keep the public portfolio environment-independent
and avoid hardcoding a workspace-specific warehouse ID.
'''

spark = SparkSession.builder.getOrCreate()

def create_table_gold_fct_flights():
    spark.sql("""
    CREATE TABLE IF NOT EXISTS db_flights.gold.fct_flights (
        run_id STRING NOT NULL
            COMMENT 'Identifier of the pipeline run that loaded the record',

        date_id BIGINT NOT NULL
            COMMENT 'Foreign key to the date dimension',

        origin_airport_id BIGINT NOT NULL
            COMMENT 'Foreign key to the origin airport dimension',

        destination_airport_id BIGINT NOT NULL
            COMMENT 'Foreign key to the destination airport dimension',

        airline_id BIGINT NOT NULL
            COMMENT 'Foreign key to the airline dimension',

        flight_number INT NOT NULL
            COMMENT 'Flight identifier',

        airline_code STRING
            COMMENT 'Business key of the airline from the source system',

        tail_number STRING
            COMMENT 'Aircraft registration',

        origin_airport STRING
            COMMENT 'Business key of the origin airport from the source system',

        destination_airport STRING
            COMMENT 'Business key of the destination airport from the source system',

        departure_delay INT
            COMMENT 'Departure delay in minutes',

        taxi_out INT
            COMMENT 'Taxi-out time in minutes',

        wheels_off INT
            COMMENT 'Actual takeoff time (HHMM)',

        scheduled_time INT
            COMMENT 'Planned flight duration in minutes',

        elapsed_time INT
            COMMENT 'Actual flight duration in minutes',

        air_time INT
            COMMENT 'Air time in minutes',

        distance INT
            COMMENT 'Flight distance in miles',

        wheels_on INT
            COMMENT 'Actual landing time (HHMM)',

        taxi_in INT
            COMMENT 'Taxi-in time in minutes',

        arrival_delay INT
            COMMENT 'Arrival delay in minutes',

        diverted INT
            COMMENT '1 = diverted flight',

        cancelled INT
            COMMENT '1 = cancelled flight',

        cancellation_reason STRING
            COMMENT 'Cancellation reason code',

        air_system_delay INT
            COMMENT 'Delay caused by the air system',

        security_delay INT
            COMMENT 'Delay caused by security',

        airline_delay INT
            COMMENT 'Delay caused by the airline',

        late_aircraft_delay INT
            COMMENT 'Delay caused by a late aircraft',

        weather_delay INT
            COMMENT 'Delay caused by weather',

        flight_date DATE
            COMMENT 'Flight date',

        scheduled_departure_timestamp TIMESTAMP
            COMMENT 'Scheduled departure timestamp',

        scheduled_arrival_timestamp TIMESTAMP
            COMMENT 'Scheduled arrival timestamp',

        departure_timestamp TIMESTAMP
            COMMENT 'Actual departure timestamp',

        arrival_timestamp TIMESTAMP
            COMMENT 'Actual arrival timestamp',

        flights_length_category STRING
            COMMENT 'Flight length category based on distance',

        departure_delay_category STRING
            COMMENT 'Departure delay category',

        arrival_delay_category STRING
            COMMENT 'Arrival delay category',
            
        CONSTRAINT fk_fact_flights_date
            FOREIGN KEY (date_id)
            REFERENCES db_flights.gold.dim_time (date_id),

        CONSTRAINT fk_fact_flights_origin_airport
            FOREIGN KEY (origin_airport_id)
            REFERENCES db_flights.gold.dim_airports (airport_id),

        CONSTRAINT fk_fact_flights_destination_airport
            FOREIGN KEY (destination_airport_id)
            REFERENCES db_flights.gold.dim_airports (airport_id),

        CONSTRAINT fk_fact_flights_airline
            FOREIGN KEY (airline_id)
            REFERENCES db_flights.gold.dim_airlines (airline_id)            
    )
    USING DELTA
    PARTITIONED BY (flight_date)
    """)


def main():
    create_table_gold_fct_flights()


if __name__ == "__main__":
    main()