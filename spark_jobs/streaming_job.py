import argparse
import psycopg2
from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

@task(name="Write Aggregates to PostgreSQL", retries=2, retry_delay_seconds=10)
def write_to_postgres(df, epoch_id):
    """
    A Prefect task to write a micro-batch of aggregated data to PostgreSQL.
    It performs an idempotent "upsert" (INSERT ... ON CONFLICT) operation.
    """
    # Define a temporary table name for the micro-batch write
    temp_table_name = "city_metrics_micro_batch"
    
    # Select and rename columns to match the target table schema
    output_df = df.select(
        col("city"),
        col("total_trips"),
        col("average_fare"),
        col("window.end").alias("last_updated")
    )

    # --- Step 1: Write the micro-batch to a temporary table in Overwrite mode ---
    (output_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/rides")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", temp_table_name)
        .option("user", "user")
        .option("password", "password")
        .mode("overwrite") # Overwrite the temp table each batch
        .save())

    # --- Step 2: Execute a MERGE SQL command from the temp table to the final table ---
    merge_sql = f"""
        INSERT INTO city_metrics (city, total_trips, average_fare, last_updated)
        SELECT city, total_trips, average_fare, last_updated FROM {temp_table_name}
        ON CONFLICT (city) DO UPDATE SET
            total_trips = city_metrics.total_trips + EXCLUDED.total_trips,
            average_fare = EXCLUDED.average_fare,
            last_updated = EXCLUDED.last_updated;
    """
    
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect("dbname=rides user=user password=password host=localhost")
        cursor = conn.cursor()
        # Execute the merge operation
        cursor.execute(merge_sql)
        conn.commit()
        print(f"Successfully merged micro-batch for epoch {epoch_id} into city_metrics.")
        cursor.close()
    except psycopg2.Error as e:
        print(f"Failed to merge data into PostgreSQL. Error: {e}")
        raise  # Re-raise the exception to trigger Prefect's retry mechanism
    finally:
        if conn is not None:
            conn.close()

@flow(name="Spark Streaming Flow")
def spark_streaming_flow(broker: str = "localhost:9092", topic: str = "ride_events"):
    """
    A Prefect flow that runs the main Spark Structured Streaming job.
    """
    # --- 1. Create a SparkSession ---
    spark = (SparkSession.builder
             .appName("RealTimeRideHailingPipeline")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession created and configured.")

    # --- 2. Define the Schema for Incoming Kafka Data ---
    schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("pickup_location", StructType([
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True)
        ]), True),
        StructField("dropoff_location", StructType([
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True)
        ]), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("city", StringType(), True),
        StructField("event_timestamp", DoubleType(), True) # Read as double to cast later
    ])

    # --- 3. Read Data from Kafka ---
    kafka_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", broker)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load())
    
    # --- 4. Parse and Transform Data ---
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    parsed_df = parsed_df.withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))

    # --- 5. Apply Watermark for Late Data Handling ---
    watermarked_df = parsed_df.withWatermark("event_timestamp", "10 minutes")

    # --- 6. Perform Windowed Aggregation ---
    aggregated_df = (watermarked_df
                     .groupBy(
                         window(col("event_timestamp"), "1 minute", "1 minute"),
                         col("city")
                     )
                     .agg(
                         count("trip_id").alias("total_trips"),
                         avg("fare_amount").alias("average_fare")
                     ))

    # --- 7. Write Stream to PostgreSQL using foreachBatch ---
    query = (aggregated_df.writeStream
             .outputMode("update")
             .foreachBatch(write_to_postgres)  # Calling our Prefect task
             .trigger(processingTime="1 minute")
             .start())

    print("Spark Streaming query has started. This Prefect flow will run until manually stopped.")
    query.awaitTermination()


if __name__ == "__main__":
    # This block allows you to run the script manually for local testing
    # without needing Prefect.
    print("Starting Spark Streaming job manually...")
    spark_streaming_flow()


# ----  The code before using prefect ----

# import argparse
# import psycopg2
# from prefect import flow, task
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, window, avg, count
# from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType

# def main(args):
#     # --- 1. Create a SparkSession ---
#     # The SparkSession is the entry point to any Spark functionality.
#     # When you create the SparkSession, you can configure it.
#     # We specify the packages Spark needs to connect to Kafka and PostgreSQL.
#     # Spark will automatically download these from Maven central.
#     spark = (SparkSession.builder
#              .appName("RealTimeRideHailingPipeline")
#              .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0")
#              .getOrCreate())

#     # Set log level to WARN to reduce verbosity
#     spark.sparkContext.setLogLevel("WARN")
#     print("SparkSession created and configured.")

#     # --- 2. Define the Schema for Incoming Kafka Data ---
#     # This schema must match the data structure produced by our Python producer.
#     # Defining a schema is crucial for performance and reliability in production.
#     schema = StructType([
#         StructField("trip_id", StringType(), True),
#         StructField("driver_id", StringType(), True),
#         StructField("customer_id", StringType(), True),
#         StructField("pickup_datetime", StringType(), True), # Read as string initially
#         StructField("dropoff_datetime", StringType(), True),# Read as string initially
#         StructField("pickup_location", StructType([
#             StructField("latitude", StringType(), True),
#             StructField("longitude", StringType(), True)
#         ]), True),
#         StructField("dropoff_location", StructType([
#             StructField("latitude", StringType(), True),
#             StructField("longitude", StringType(), True)
#         ]), True),
#         StructField("fare_amount", DoubleType(), True),
#         StructField("tip_amount", DoubleType(), True),
#         StructField("city", StringType(), True),
#         StructField("event_timestamp", DoubleType(), True)
#     ])

#     # --- 3. Read Data from Kafka as a Streaming DataFrame ---
#     # 'readStream' creates a DataFrame representing the stream of data from Kafka.
#     kafka_df = (spark.readStream
#                 .format("kafka")
#                 .option("kafka.bootstrap.servers", args.broker)
#                 .option("subscribe", args.topic)
#                 .option("startingOffsets", "earliest") # Process all messages from the beginning
#                 .load())
#     print("Streaming DataFrame created from Kafka topic.")

#     # --- 4. Parse the Kafka Message ---
#     # Kafka messages are delivered with 'key', 'value', 'topic', etc. columns.
#     # The actual data we sent is in the 'value' column.
#     # We first cast the binary 'value' to a string, then parse the JSON.
#     parsed_df = kafka_df.select(
#         from_json(col("value").cast("string"), schema).alias("data")
#     ).select("data.*")

#     # Convert timestamp fields from string/double to actual TimestampType
#     parsed_df = parsed_df.withColumn("pickup_datetime", col("pickup_datetime").cast(TimestampType())) \
#                          .withColumn("dropoff_datetime", col("dropoff_datetime").cast(TimestampType())) \
#                          .withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))

#     # --- 5. Define a Watermark for Handling Late Data ---
#     # A watermark tells Spark how long to wait for late-arriving data.
#     # If an event from 10:05 arrives at 10:11, it's still included because it's within the 10-minute watermark.
#     # This is essential for robust windowed aggregations.
#     watermarked_df = parsed_df.withWatermark("event_timestamp", "10 minutes")
#     print("Watermark defined on 'event_timestamp' column.")

#     # --- 6. Perform Windowed Aggregation ---
#     # We group data by city and a 1-minute 'tumbling' window.
#     # A tumbling window means the windows are fixed and don't overlap (e.g., 10:00-10:01, 10:01-10:02).
#     aggregated_df = (watermarked_df
#                      .groupBy(
#                          window(col("event_timestamp"), "1 minute"),
#                          col("city")
#                      )
#                      .agg(
#                          count("trip_id").alias("total_trips"),
#                          avg("fare_amount").alias("average_fare")
#                      ))
#     print("Windowed aggregation defined.")

#     # --- 7. Define the Function to Write a Micro-Batch to PostgreSQL ---
#     # This function will be called for each micro-batch of the streaming query.
#     # It handles writing the aggregated data to our Postgres table.
#     @task(name="Write to PostgreSQL Task", retries=2, retry_delay_seconds=10)
#     def write_to_postgres(df, epoch_id):
#         # We need to handle "upserts" (UPDATE or INSERT).
#         # We write to a temporary table first, then merge into the final table.
#         # This is an idempotent operation, meaning it's safe to re-run.
#         temp_table_name = "city_metrics"
        
#         # Select and rename columns to match the target table
#         output_df = df.select(
#             col("city"),
#             col("total_trips"),
#             col("average_fare"),
#             col("window.end").alias("last_updated") # Use window end time as update timestamp
#         )

#         (output_df.write
#          .format("jdbc")
#          .option("url", "jdbc:postgresql://localhost:5432/rides")
#          .option("driver", "org.postgresql.Driver")
#          .option("dbtable", temp_table_name)
#          .option("user", "user")
#          .option("password", "password")
#          .mode("overwrite") # Overwrite the temp table each time
#          .save())

#         # Now, perform the MERGE operation from the temp table to the final table
#         # This is a common pattern for idempotent writes in data engineering.
#         # In Postgres, this is done with INSERT ... ON CONFLICT.
#         merge_sql = f"""
#             INSERT INTO city_metrics (city, total_trips, average_fare, last_updated)
#             SELECT city, total_trips, average_fare, last_updated FROM {temp_table_name}
#             ON CONFLICT (city) DO UPDATE SET
#                 total_trips = city_metrics.total_trips + EXCLUDED.total_trips,
#                 average_fare = EXCLUDED.average_fare,
#                 last_updated = EXCLUDED.last_updated;
#         """
        
#         # You need a way to execute this SQL. This is typically done with a separate library
#         # like psycopg2, or by getting a Spark JDBC connection. For simplicity, we print it.
#         # In a real job, you would execute this command against Postgres.
#         print(f"Executing MERGE SQL for epoch {epoch_id}")
#         # NOTE: A simple way to run this in a real scenario:
#         # with psycopg2.connect(dbname="rides", user="user", password="password", host="localhost") as conn:
#         #     with conn.cursor() as cur:
#         #         cur.execute(merge_sql)

#     # --- 8. Start the Streaming Query to Write to PostgreSQL ---
#     # 'foreachBatch' is a powerful sink that lets us run custom logic on each micro-batch.
#     query = (aggregated_df.writeStream
#              .outputMode("update") # 'update' mode is needed for windowed aggregations with watermarks
#              .foreachBatch(write_to_postgres)
#              .trigger(processingTime="1 minute") # Process data every 1 minute
#              .start())

#     print("Streaming query started. Waiting for termination...")
#     query.awaitTermination()


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Spark Structured Streaming job from Kafka to PostgreSQL")
#     parser.add_argument("--broker", default="localhost:9092", help="Kafka broker address")
#     parser.add_argument("--topic", default="ride_events", help="Kafka topic name")
#     args = parser.parse_args()
    
#     main(args)
 