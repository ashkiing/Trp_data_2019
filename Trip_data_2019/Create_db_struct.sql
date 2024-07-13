CREATE DATABASE trip_data_db;

USE trip_data_db;

CREATE TABLE yellow_tripdata_2019 (
    trip_id INT AUTO_INCREMENT PRIMARY KEY,
    tpep_pickup_datetime DATETIME NOT NULL,
    tpep_dropoff_datetime DATETIME NOT NULL,
    passenger_count INT,
    trip_distance DOUBLE NOT NULL,
    pickup_longitude DOUBLE,
    pickup_latitude DOUBLE,
    dropoff_longitude DOUBLE,
    dropoff_latitude DOUBLE,
    fare_amount DOUBLE NOT NULL,
    tip_amount DOUBLE,
    total_amount DOUBLE NOT NULL,
    trip_duration DOUBLE GENERATED ALWAYS AS ((UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime)) / 3600) STORED,
    average_speed DOUBLE GENERATED ALWAYS AS (trip_distance / trip_duration) STORED,
    INDEX idx_pickup_datetime (tpep_pickup_datetime),
    INDEX idx_dropoff_datetime (tpep_dropoff_datetime)
);
