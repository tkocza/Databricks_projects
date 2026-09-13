from pyspark.sql.types import *

SCHEMA_FLIGHTS = StructType([
    StructField("YEAR", IntegerType(), True, {"comment": "Flight year"}),
    StructField("MONTH", IntegerType(), True, {"comment": "Month (1–12)"}),
    StructField("DAY", IntegerType(), True, {"comment": "Day of month"}),
    StructField("DAY_OF_WEEK", IntegerType(), True, {"comment": "Day of week (1–7)"}),

    StructField("AIRLINE", StringType(), True, {"comment": "Airline carrier code"}),
    StructField("FLIGHT_NUMBER", IntegerType(), True, {"comment": "Flight identifier"}),
    StructField("TAIL_NUMBER", StringType(), True, {"comment": "Aircraft registration"}),

    StructField("ORIGIN_AIRPORT", StringType(), True, {"comment": "Departure airport code"}),
    StructField("DESTINATION_AIRPORT", StringType(), True, {"comment": "Arrival airport code"}),

    StructField("SCHEDULED_DEPARTURE", IntegerType(), True, {"comment": "Scheduled departure (HHMM)"}),
    StructField("DEPARTURE_TIME", IntegerType(), True, {"comment": "Actual departure (HHMM)"}),
    StructField("DEPARTURE_DELAY", IntegerType(), True, {"comment": "Delay in minutes"}),

    StructField("TAXI_OUT", IntegerType(), True, {"comment": "Taxi-out time (min)"}),
    StructField("WHEELS_OFF", IntegerType(), True, {"comment": "Takeoff time (HHMM)"}),

    StructField("SCHEDULED_TIME", IntegerType(), True, {"comment": "Planned duration (min)"}),
    StructField("ELAPSED_TIME", IntegerType(), True, {"comment": "Actual duration (min)"}),
    StructField("AIR_TIME", IntegerType(), True, {"comment": "Air time (min)"}),
    StructField("DISTANCE", IntegerType(), True, {"comment": "Distance (miles)"}),

    StructField("WHEELS_ON", IntegerType(), True, {"comment": "Landing time (HHMM)"}),
    StructField("TAXI_IN", IntegerType(), True, {"comment": "Taxi-in time (min)"}),

    StructField("SCHEDULED_ARRIVAL", IntegerType(), True, {"comment": "Scheduled arrival (HHMM)"}),
    StructField("ARRIVAL_TIME", IntegerType(), True, {"comment": "Actual arrival (HHMM)"}),
    StructField("ARRIVAL_DELAY", IntegerType(), True, {"comment": "Arrival delay in minutes"}),

    StructField("DIVERTED", IntegerType(), True, {"comment": "1 = diverted flight"}),
    StructField("CANCELLED", IntegerType(), True, {"comment": "1 = cancelled flight"}),
    StructField("CANCELLATION_REASON", StringType(), True, {"comment": "Cancellation reason code"}),

    StructField("AIR_SYSTEM_DELAY", IntegerType(), True, {"comment": "Air system delay"}),
    StructField("SECURITY_DELAY", IntegerType(), True, {"comment": "Security delay"}),
    StructField("AIRLINE_DELAY", IntegerType(), True, {"comment": "Airline delay"}),
    StructField("LATE_AIRCRAFT_DELAY", IntegerType(), True, {"comment": "Late aircraft delay"}),
    StructField("WEATHER_DELAY", IntegerType(), True, {"comment": "Weather delay"})
])

SCHEMA_AIRPORTS = StructType([
    StructField("IATA_CODE", StringType(), True, {"comment": "Unique airport IATA code (3-letter)"}),
    StructField("AIRPORT", StringType(), True, {"comment": "Full airport name"}),
    StructField("CITY", StringType(), True, {"comment": "City of airport location"}),
    StructField("STATE", StringType(), True, {"comment": "State or region"}),
    StructField("COUNTRY", StringType(), True, {"comment": "Country"}),
    StructField("LATITUDE", DoubleType(), True, {"comment": "Latitude coordinate"}),
    StructField("LONGITUDE", DoubleType(), True, {"comment": "Longitude coordinate"})
])

SCHEMA_AIRLINES = StructType([
    StructField("IATA_CODE", StringType(), True, {"comment": "Airline IATA code (2-letter)"}),
    StructField("AIRLINE", StringType(), True, {"comment": "Full airline name"})
])