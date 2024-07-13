import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pymysql
from sqlalchemy import create_engine

# Database connection provide your own username and password
db_connection_str = f'mysql+pymysql://{username}:{password}@{host}/{database}'
db_connection = create_engine(db_connection_str)

# Query 1: Peak Hours for Taxi Usage
query1 = """
SELECT HOUR(tpep_pickup_datetime) AS hour, COUNT(*) AS total_trips
FROM yellow_tripdata_2019
GROUP BY hour
ORDER BY hour;
"""
df_peak_hours = pd.read_sql(query1, con=db_connection)

plt.figure(figsize=(10, 6))
sns.barplot(x='hour', y='total_trips', data=df_peak_hours, palette='viridis')
plt.title('Peak Hours for Taxi Usage')
plt.xlabel('Hour of the Day')
plt.ylabel('Total Trips')
plt.show()

# Query 2: Passenger Count Effect on Trip Fare
query2 = """
SELECT passenger_count, AVG(fare_amount) AS average_fare
FROM yellow_tripdata_2019
GROUP BY passenger_count
ORDER BY passenger_count;
"""
df_passenger_fare = pd.read_sql(query2, con=db_connection)

plt.figure(figsize=(10, 6))
sns.barplot(x='passenger_count', y='average_fare', data=df_passenger_fare, palette='viridis')
plt.title('Effect of Passenger Count on Trip Fare')
plt.xlabel('Passenger Count')
plt.ylabel('Average Fare ($)')
plt.show()

# Query 3: Trends in Usage Over the Year
query3 = """
SELECT MONTH(tpep_pickup_datetime) AS month, COUNT(*) AS total_trips
FROM yellow_tripdata_2019
GROUP BY month
ORDER BY month;
"""
df_monthly_trends = pd.read_sql(query3, con=db_connection)

plt.figure(figsize=(10, 6))
sns.lineplot(x='month', y='total_trips', data=df_monthly_trends, marker='o')
plt.title('Trends in Taxi Usage Over the Year')
plt.xlabel('Month')
plt.ylabel('Total Trips')
plt.xticks(range(1, 13))
plt.show()
