from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, unix_timestamp, round, avg, count
from pyspark.sql.types import DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("Trip_data_2019").getOrCreate()

# Directory containing the Parquet files
input_dir = 'C:/Users/ashki/Downloads/Trip_data_2019'

# Load the Parquet files
df = spark.read.parquet(f'{input_dir}/*.parquet')

# Remove trips with missing or corrupt data
clean_df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount"]) \
             .filter((col("trip_distance") > 0) & (col("fare_amount") > 0))

# Derive new columns
clean_df = clean_df.withColumn(
    "trip_duration", 
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600
)

clean_df = clean_df.withColumn(
    "average_speed", 
    round(col("trip_distance") / col("trip_duration"), 2)
)

# Filter out records with non-positive trip duration or unrealistic average speeds
clean_df = clean_df.filter((col("trip_duration") > 0) & (col("average_speed") < 100))

# Aggregate data to calculate total trips and average fare per day
aggregated_df = clean_df.withColumn("pickup_date", col("tpep_pickup_datetime").cast("date")) \
                        .groupBy("pickup_date") \
                        .agg(
                            count("*").alias("total_trips"),
                            round(avg("fare_amount"), 2).alias("average_fare")
                        )

# Show the aggregated data
aggregated_df.show()

# Save the aggregated data to a CSV file
output_path = 'C:/Users/ashki/Downloads/Aggregate_trip_data/Aggregate_data.csv'
aggregated_df.write.csv(output_path, header=True)

# Stop the Spark session
spark.stop()
