from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, to_timestamp, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, LongType
import time
import os

# sets the environment variable to point to the correct Hadoop installation directory.
os.environ['HADOOP_HOME'] = r"C:\hadoop"

# Define the schema of our weather data
schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType())
    ])),
    StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType()),
        StructField("main", StringType()),
        StructField("description", StringType()),
        StructField("icon", StringType())
    ]))),
    StructField("base", StringType()),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("feels_like", DoubleType()),
        StructField("temp_min", DoubleType()),
        StructField("temp_max", DoubleType()),
        StructField("pressure", IntegerType()),
        StructField("humidity", IntegerType())
    ])),
    StructField("visibility", IntegerType()),
    StructField("wind", StructType([
        StructField("speed", DoubleType()),
        StructField("deg", IntegerType())
    ])),
    StructField("clouds", StructType([
        StructField("all", IntegerType())
    ])),
    StructField("dt", LongType()),
    StructField("sys", StructType([
        StructField("type", IntegerType()),
        StructField("id", IntegerType()),
        StructField("country", StringType()),
        StructField("sunrise", LongType()),
        StructField("sunset", LongType())
    ])),
    StructField("timezone", IntegerType()),
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("cod", IntegerType())
])

# Create a Spark session
spark = SparkSession.builder \
    .appName("WeatherDataProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.postgresql:postgresql:42.7.3") \
    .config("spark.driver.extraClassPath", "/weather-data-pipeline/lib/postgresql-42.7.3.jar") \
    .config("spark.executor.extraClassPath", "/weather-data-pipeline/lib/postgresql-42.7.3.jar") \
    .getOrCreate()

# Clear any existing cache
spark.catalog.clearCache()

# Create a streaming DataFrame representing the stream of input lines from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "new_weather_data") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON data
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Flatten the nested structures and explode the weather array
flattened_df = parsed_df.select(
    col("name").alias("city"),
    col("coord.lon").alias("longitude"),
    col("coord.lat").alias("latitude"),
    col("main.temp").alias("temperature"),
    col("main.feels_like").alias("feels_like"),
    col("main.temp_min").alias("temp_min"),
    col("main.temp_max").alias("temp_max"),
    col("main.pressure").alias("pressure"),
    col("main.humidity").alias("humidity"),
    col("visibility"),
    col("wind.speed").alias("wind_speed"),
    col("wind.deg").alias("wind_direction"),
    col("clouds.all").alias("cloudiness"),
    to_timestamp(col("dt")).alias("timestamp"),
    to_timestamp(col("sys.sunrise")).alias("sunrise"),
    to_timestamp(col("sys.sunset")).alias("sunset"),
    col("sys.country").alias("country"),
    explode("weather").alias("weather")
)

# Further flatten the weather struct
final_df = flattened_df.select(
    "city",
    "longitude",
    "latitude",
    "temperature",
    "feels_like",
    "temp_min",
    "temp_max",
    "pressure",
    "humidity",
    "visibility",
    "wind_speed",
    "wind_direction",
    "cloudiness",
    "timestamp",
    "sunrise",
    "sunset",
    "country",
    col("weather.main").alias("weather_main"),
    col("weather.description").alias("weather_description"),
    col("weather.icon").alias("weather_icon")
)

# Add a comfort index calculation
from pyspark.sql.functions import when

final_df = final_df.withColumn(
    "comfort_index",
    when((col("temperature").between(18, 24)) & (col("humidity").between(40, 60)) & (col("wind_speed") < 5.5), "Very Comfortable")
    .when((col("temperature").between(15, 28)) & (col("humidity").between(30, 70)) & (col("wind_speed") < 8), "Comfortable")
    .otherwise("Less Comfortable")
)

# Calculate daylight hours
from pyspark.sql.functions import (col, when, lit)

final_df = final_df.withColumn(
    "daylight_hours",
    (col("sunset").cast("long") - col("sunrise").cast("long")) / 3600
)

# Function to write data to PostgreSQL
def write_to_postgres(df, epoch_id):
    try:
        print(f"Processing batch {epoch_id}")
        print(f"Number of records in this batch: {df.count()}")
        print("Sample data:")
        df.show(5, truncate=False)

        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/weatherdb") \
            .option("dbtable", "weather_data") \
            .option("user", "USER_NAME") \
            .option("password", "PASSWORD") \
            .mode("append") \
            .save()

        print(f"Successfully wrote batch {epoch_id} to PostgreSQL")
    except Exception as e:
        print(f"Error writing batch {epoch_id} to PostgreSQL: {str(e)}")
        print("Error details:", e)

# Write the data to PostgreSQL and MongoDB
query = final_df \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: (write_to_postgres(df, epoch_id))) \
    .outputMode("update") \
    .start()


# Track the time and stop the query after 12 hours
start_time = time.time()
while time.time() - start_time < 3600 * 12:
    time.sleep(10)  # Sleep for a short time to avoid busy-waiting

# Stop the query gracefully
query.stop()



query.awaitTermination()


