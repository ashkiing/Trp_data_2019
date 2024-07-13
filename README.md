# Trp_data_2019

Project Overview: This Project is to design and implement a scalable data pipeline that extracts New York Taxi Trip data for year 2019, processes it to derive analytical insights, and loads the processed data into a data warehouse for insightful analysis.

Environment Setup: Install a Jupyter notebook on your system, then pip install request tenacity and pyspark.
Running the Project: First install items mentioned above then import the following :
                    -import os
                    -import requests
                    -from tenacity import retry, stop_after_attempt, wait_exponential
                    -from pyspark.sql import SparkSession
                    -from pyspark.sql.functions import col, when, unix_timestamp, round, avg, count
                    -from pyspark.sql.types import DoubleType

*In Jupyter notebook you have to run the cell using ctrl+enter or the icon is provide to run the cell*
Now you're good to run the script which first download the file into the system then read the file from the location mentioned and run the neccessary transformation and then load the data after aggregating to database.

Data Analysis: Follow the steps :
              -Design the schema
              -Implement the schema
              -Load data effieiently
*Make sure the data file is accessible by the database server*

