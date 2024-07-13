LOAD DATA INFILE 'C:/Users/ashki/Downloads/Aggregate_trip_data/Aggregate_data.csv'
INTO TABLE yellow_tripdata_2019
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    @dummy,  -- Skip the first column if it's an index or unnecessary
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    fare_amount,
    tip_amount,
    total_amount
);
