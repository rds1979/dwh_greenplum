DESCRIBE TABLE default.trips;


SELECT 
	total_amount 
FROM default.trips ORDER BY total_amount DESC LIMIT 10 OFFSET 50;


SELECT 
	total_amount
FROM default.trips ORDER BY 1 DESC LIMIT 10
-- WITH TIES;


SELECT 
	passenger_count, CEIL(AVG(total_amount),2) 
FROM default.trips 
GROUP BY passenger_count ORDER BY passenger_count 
LIMIT 10;


SELECT
	MIN(pickup_date), MAX(pickup_date)
FROM default.trips;


SELECT 
	COUNT(*) 
FROM default.trips 
WHERE pickup_date BETWEEN '2015-07-01' AND '2015-08-01';


SELECT 
	DISTINCT(pickup_date) 
FROM default.trips 
WHERE pickup_date BETWEEN '2015-07-01' AND '2015-08-01' ORDER BY pickup_date;


SELECT 
	ROUND(MIN(tip_amount),2), ROUND(AVG(tip_amount),2), ROUND(MAX(tip_amount),2)
FROM default.trips;


SELECT
    count(*), passenger_count, pickup_date
FROM default.trips
GROUP BY passenger_count, pickup_date
HAVING (pickup_date > '2015-09-12') AND (passenger_count > 3)
ORDER BY passenger_count ASC, pickup_date DESC;


SELECT
    MIN(total_amount) AS MIN, MAX(total_amount) AS MAX,
    CEIL(MAX(total_amount) - MIN(total_amount), 2) AS SPREAD
FROM default.trips;


SELECT
    COUNT(*) AS "cnt", pickup_date, pickup_ntaname
FROM default.trips
GROUP BY pickup_date, pickup_ntaname
HAVING (pickup_date = '2015-09-08') AND (pickup_ntaname NOT LIKE '')
ORDER BY pickup_date ASC, cnt ASC;


SELECT
    pickup_date, pickup_ntaname, SUM(1) AS count
FROM default.trips
GROUP BY pickup_date, pickup_ntaname
HAVING (pickup_date >= '2015-09-08') AND (pickup_date <= '2015-09-10')
ORDER BY pickup_date ASC, count ASC;


SELECT
	ROUND(AVG(tip_amount),2) AS "avg_tip",
	ROUND(AVG(fare_amount),2) AS "avg_fire",
	ROUND(AVG(passenger_count),2) AS "avg_pass",
	COUNT() AS "cnt",
	TRUNCATE(date_diff('second', pickup_datetime, dropoff_datetime)/60) AS "trip_min"
FROM default.trips
WHERE (trip_min >= 100) AND (trip_min <= 200)
GROUP BY trip_min
ORDER BY trip_min;
