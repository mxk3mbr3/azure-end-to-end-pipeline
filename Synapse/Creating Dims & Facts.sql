-- Creating Dimensional Model - Dims & Fact

-- 1. DimAirports
CREATE VIEW DW.DimAirports AS
WITH cte_dim_airports AS (
SELECT
    DISTINCT airport_code AS airport_code,
    airport_city
FROM
    OPENROWSET(
    BULK 'https://flightdatamla.dfs.core.windows.net/transformation/*/*.parquet',
    FORMAT = 'PARQUET'
    ) AS [result]
)
SELECT
    ROW_NUMBER() OVER (ORDER BY airport_city) AS airport_id,
    airport_city,
    airport_code
FROM
    cte_dim_airports
GO

-- 2. DimAircrafts 
CREATE VIEW DW.DimAircrafts AS
WITH cte_dim_aircraft AS (
SELECT
    DISTINCT aircraft_registration AS aircraft_registration,
    aircraft_type,
    airline
FROM
    OPENROWSET(
    BULK 'https://flightdatamla.dfs.core.windows.net/transformation/*/*.parquet',
    FORMAT = 'PARQUET'
    ) AS [result]
)
SELECT
    ROW_NUMBER() OVER (ORDER BY airline) AS aircraft_id,
    airline,
    aircraft_type,
    aircraft_registration
FROM
    cte_dim_aircraft
GO

-- 3. DimDates 
CREATE VIEW DW.DimDates AS
WITH cte_dim_date AS (
SELECT
    DISTINCT CONVERT(DATE, scheduled_time) AS full_date
FROM
    OPENROWSET(
    BULK 'https://flightdatamla.dfs.core.windows.net/transformation/*/*.parquet',
    FORMAT = 'PARQUET'
    ) AS [result]
)
SELECT
    ROW_NUMBER() OVER (ORDER BY full_date) AS date_id,
    *
FROM
    cte_dim_date
GO

-- 4. DimFlights 
CREATE VIEW DW.DimFlights AS
WITH cte_dim_flight AS (
SELECT
    DISTINCT flight_no AS flight_no
FROM
    OPENROWSET(
    BULK 'https://flightdatamla.dfs.core.windows.net/transformation/*/*.parquet',
    FORMAT = 'PARQUET'
    ) AS [result]
)
SELECT
    ROW_NUMBER() OVER (ORDER BY flight_no) AS flight_id,
    *
FROM
    cte_dim_flight
GO

-- 5. DimType 
CREATE VIEW DW.DimType AS
WITH cte_dim_type AS (
SELECT
    DISTINCT flight_type AS flight_type
FROM
    OPENROWSET(
    BULK 'https://flightdatamla.dfs.core.windows.net/transformation/*/*.parquet',
    FORMAT = 'PARQUET'
    ) AS [result]
)
SELECT
    ROW_NUMBER() OVER (ORDER BY flight_type) AS type_id,
    *
FROM
    cte_dim_type
GO

-- 6. FctDelays
CREATE VIEW DW.FctDelays AS
WITH cte_fact_delays AS (
SELECT
    DISTINCT CONCAT(r.scheduled_time,r.flight_no) AS unique_time,
    r.scheduled_time,
    r.actual_time,
    r.delay,
    ap.airport_id,
    ac.aircraft_id,
    d.date_id,
    f.flight_id,
    t.type_id
FROM
    OPENROWSET(
    BULK 'https://flightdatamla.dfs.core.windows.net/transformation/*/*.parquet',
    FORMAT = 'PARQUET'
    ) AS r
LEFT JOIN DW.DimAirports ap
ON r.airport_code=ap.airport_code
LEFT JOIN DW.DimAircrafts ac
ON r.aircraft_registration=ac.aircraft_registration
LEFT JOIN DW.DimDates d
ON CONVERT(DATE, r.scheduled_time)=d.full_date
LEFT JOIN DW.DimFlights f
ON r.flight_no=f.flight_no
LEFT JOIN DW.DimType t
ON r.flight_type=t.flight_type
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY unique_time) AS delay_id,
    scheduled_time,
    actual_time,
    delay,
    airport_id,
    aircraft_id,
    date_id,
    flight_id,
    type_id
FROM 
    cte_fact_delays