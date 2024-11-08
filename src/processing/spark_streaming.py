# src/processing/spark_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

def create_spark_session():
    return SparkSession.builder \
        .appName("YouTubeTrendingProcessor") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .master("local[*]") \
        .getOrCreate()

def process_stream():
    # Create Spark session
    spark = create_spark_session()
    print("Created Spark Session")

    # Define schema for incoming data
    schema = StructType([
        StructField("video_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("channel_title", StringType(), True),
        StructField("publish_time", StringType(), True),
        StructField("view_count", LongType(), True),
        StructField("like_count", LongType(), True),
        StructField("comment_count", LongType(), True),
        StructField("duration", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("category_id", StringType(), True),
        StructField("fetch_time", StringType(), True)
    ])

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "youtube_trending") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON and process
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    processed_df = parsed_df.withColumn("processing_time", current_timestamp())

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
            
        print(f"Writing batch {batch_id} to PostgreSQL...")
        jdbc_url = "jdbc:postgresql://localhost:5432/youtube_trending"
        properties = {
            "driver": "org.postgresql.Driver",
            "user": "postgres",
            "password": "postgres123",
            "url": jdbc_url
        }
        
        try:
            batch_df.write \
                .jdbc(url=jdbc_url,
                      table="video_metrics",
                      mode="append",
                      properties=properties)
            print(f"Successfully wrote batch {batch_id}")
        except Exception as e:
            print(f"Error writing batch {batch_id}: {str(e)}")

    # Start streaming
    query = processed_df.writeStream \
        .foreachBatch(write_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    try:
        process_stream()
    except Exception as e:
        print(f"Error in main process: {str(e)}")
        raise e