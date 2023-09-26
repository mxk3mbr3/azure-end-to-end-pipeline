-- Views for dashboard

-- 1. Total number of flights grouped by airline (TOP 10)
-- Condition is that total flights by airline has to be greater than or equal to 20
CREATE VIEW total_flights_by_airline 
AS
SELECT
    TOP 10 airline,
    COUNT(airline) as total_flights
FROM
    DW.FctDelays f
LEFT JOIN
    DW.DimAircrafts ac
ON 
    f.aircraft_id=ac.aircraft_id
GROUP BY
    airline
HAVING
    COUNT(airline) >=20
ORDER BY
    total_flights DESC

GO

-- 2. Airports with highest activity (TOP 5)
CREATE VIEW total_flights_by_airport_city
AS
SELECT
    TOP 5 airport_city,
    COUNT(airport_city) as total_flights
FROM
    DW.FctDelays f
LEFT JOIN
    DW.DimAirports ap
ON 
    f.airport_id=ap.airport_id
GROUP BY
    airport_city
ORDER BY
    total_flights DESC

GO

-- 3. Get flights with the biggest delays (TOP 10)
CREATE VIEW biggest_delay_decimal
AS
SELECT
    TOP 10 airline, 
	airport_city, 
    scheduled_time,
    actual_time,
    ROUND(delay/3600.0,2) AS delay_hrs_decimal
FROM
    DW.FctDelays f
LEFT JOIN
    DW.DimAircrafts ac
ON 
    f.aircraft_id=ac.aircraft_id
LEFT JOIN
    DW.DimAirports ap
ON 
    f.airport_id=ap.airport_id
ORDER BY
    delay_hrs_decimal DESC

GO

-- 4. Reliability index by airline (TOP 10)
-- Condition is that total flights by airline has to be greater than or equal to 20
-- delay of less than or equal to 900s (15 minutes) is considered as 'On time/ Small Delay'
CREATE VIEW reliability_airline
AS
WITH cte_total_flights AS (
    SELECT
        airline,
        COUNT(airline) AS total_flights
    FROM
        DW.FctDelays f
    LEFT JOIN
        DW.DimAircrafts ac
    ON 
        f.aircraft_id=ac.aircraft_id
    GROUP BY
        airline
), cte_delayed_flights AS (
SELECT
    airline,
    COUNT(delay) AS on_time_count
FROM
    DW.FctDelays f
LEFT JOIN
    DW.DimAircrafts ac
ON 
    f.aircraft_id=ac.aircraft_id
WHERE 
    delay <= 900
GROUP BY
    airline
)
SELECT
    TOP 10 df.airline,
    tf.total_flights,
    ROUND((df.on_time_count/CAST(tf.total_flights AS FLOAT))*100,1) AS airline_reliability_pct
FROM
   cte_delayed_flights df
LEFT JOIN
    cte_total_flights tf
ON
    df.airline = tf.airline
WHERE
    tf.total_flights >= 20
ORDER BY
    airline_reliability_pct DESC

GO

-- 5. Reliability index by aircraft (TOP 10)
-- Condition is that total flights by aircraft has to be greater than or equal to 10
-- delay of less than or equal to 900s (15 minutes) is considered as 'On time/ Small Delay'
CREATE VIEW reliability_aircraft
AS
WITH cte_total_flights AS (
    SELECT
        airline,
        aircraft_registration,
        COUNT(aircraft_registration) AS total_flights
    FROM
        DW.FctDelays f
    LEFT JOIN
        DW.DimAircrafts ac
    ON 
        f.aircraft_id=ac.aircraft_id
    GROUP BY
        airline, aircraft_registration
), cte_delayed_flights AS (
SELECT
    aircraft_registration,
    COUNT(delay) AS on_time_count
FROM
    DW.FctDelays f
LEFT JOIN
    DW.DimAircrafts ac
ON 
    f.aircraft_id=ac.aircraft_id
WHERE 
    delay <= 900
GROUP BY
    aircraft_registration
)
SELECT
    TOP 10 tf.airline,
    df.aircraft_registration,
    tf.total_flights,
    ROUND((df.on_time_count/CAST(tf.total_flights AS FLOAT))*100,1) AS aircraft_reliability_pct
FROM
   cte_delayed_flights df
LEFT JOIN
    cte_total_flights tf
ON
    df.aircraft_registration = tf.aircraft_registration
WHERE
    tf.total_flights >= 10
ORDER BY
    aircraft_reliability_pct DESC