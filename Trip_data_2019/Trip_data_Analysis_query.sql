#------------Peak Hours for Taxi Usage-------------------------

SELECT HOUR(tpep_pickup_datetime) AS hour, COUNT(*) AS total_trips
FROM yellow_tripdata_2019
GROUP BY hour
ORDER BY hour;

#----------------Passenger Count Effect on Trip Fare-----------

SELECT passenger_count, AVG(fare_amount) AS average_fare
FROM yellow_tripdata_2019
GROUP BY passenger_count
ORDER BY passenger_count;

#-------------Trends in Usage Over the Year---------------------

SELECT MONTH(tpep_pickup_datetime) AS month, COUNT(*) AS total_trips
FROM yellow_tripdata_2019
GROUP BY month
ORDER BY month;
