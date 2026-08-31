CREATE TABLE IF NOT EXISTS db_flights.gold.dim_airlines (
    airline_id BIGINT GENERATED ALWAYS AS IDENTITY,
    airline_code STRING,
    airline_name STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
)
USING DELTA;

INSERT INTO db_flights.gold.dim_airlines (
    airline_id,
    row_hash,
    valid_from,
    valid_to,
    is_current,
    airline_code,
    airline
)
SELECT *
FROM VALUES
    (-1, 'NA', NULL, NULL, TRUE, 'UNKNOWN', 'Unknown Airline'),
    (-999, 'NA', NULL, NULL, TRUE, 'QUARANTINED', 'Missing Airline') AS v(
        airline_id,
        row_hash,
        valid_from,
        valid_to,
        is_current,
        airline_code,
        airline
    )
WHERE NOT EXISTS (
    SELECT 1
    FROM db_flights.gold.dim_airlines t
    WHERE t.airline_id = v.airline_id
);


--if merge is prefered
/*
MERGE INTO db_flights.gold.dim_airlines AS target
USING (
    SELECT 
    -1 AS airline_id,
    'NA' AS row_hash,
    NULL AS valid_from,
    NULL AS valid_to, 
    TRUE AS is_current,
    'UNKNOWN' AS airline_code, 
    'Unknown Airline' AS airline

    UNION ALL

    SELECT 
    -999 AS airline_id,
    'NA' AS row_hash,
    NULL AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current,
    'QUARANTINED' AS airline_code, 
    'Missing Airline' AS airline
) AS source
ON target.airline_id = source.airline_id

WHEN NOT MATCHED THEN
    INSERT (
        airline_id,
        row_hash,
        valid_from,
        valid_to,
        is_current,
        airline_code,
        airline
    )
    VALUES (
        source.airline_id,
        source.row_hash,
        source.valid_from,
        source.valid_to,
        source.is_current,
        source.airline_code,
        source.airline
    );