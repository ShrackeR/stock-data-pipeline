from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import happybase

# Kafka Configuration
KAFKA_SERVER = "172.19.50.120:9092"
KAFKA_TOPIC = "tick-data-new"

# HBase Configuration
HBASE_HOST = "172.19.50.120"
HBASE_TABLE = "stock_ticks"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("RealTimeStockDataProcessing") \
    .getOrCreate()

# Define schema for incoming data
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("size", FloatType(), True),
    StructField("timestamp", TimestampType(), True)
])

def create_streaming_dataframe():
    # Reading from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON and select fields
    parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(F.from_json(F.col("value"), schema).alias("data")) \
        .select("data.*")  # Flatten the structure

    # Apply watermark to timestamp
    watermarked_stream = parsed_stream.withWatermark("timestamp", "5 seconds")
    
    return watermarked_stream

def calculate_technical_indicators(pdf):
    """Calculate RSI and MACD using pandas"""
    # Calculate RSI
    delta = pdf['price'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.rolling(window=14, min_periods=1).mean()
    avg_loss = loss.rolling(window=14, min_periods=1).mean()
    
    rs = avg_gain / avg_loss
    pdf['rsi'] = 100 - (100 / (1 + rs))
    
    # Calculate MACD
    ema12 = pdf['price'].ewm(span=12, adjust=False).mean()
    ema26 = pdf['price'].ewm(span=26, adjust=False).mean()
    pdf['macd'] = ema12 - ema26
    pdf['macd_signal'] = pdf['macd'].ewm(span=9, adjust=False).mean()
    
    return pdf

def process_batch(batch_df, batch_id):
    """Process each micro-batch"""
    pdf = batch_df.toPandas()
    pdf = calculate_technical_indicators(pdf)
    return spark.createDataFrame(pdf)

def write_to_hbase(df, epoch_id):
    """Write processed data to HBase"""
    connection = happybase.Connection(HBASE_HOST)
    table = connection.table(HBASE_TABLE)
    
    for row in df.collect():
        row_key = f"{row['symbol']}_{int(row['timestamp'].timestamp())}"
        
        table.put(row_key, {
            "cf:price": str(row["price"]),
            "cf:size": str(row["size"]),
            "cf:timestamp": str(row["timestamp"]),
            "indicators:rsi": str(row.asDict().get("rsi", "")),
            "indicators:macd": str(row.asDict().get("rsi", ""))
        })
    
    connection.close()

def process_stream(stream_df):
    """Process the stream with windowed aggregations"""
    # Windowed aggregations with watermark
    windowed_agg = stream_df \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            col("symbol")
        ).agg(
            avg("price").alias("avg_price"),
            sum("size").alias("total_size"),
            count("symbol").alias("tick_count")
        )
    
    # Process technical indicators
    processed_stream = windowed_agg.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoints") \
        .start()
    
    return processed_stream

def start_streaming():
    streaming_df = create_streaming_dataframe()
    processed_stream = process_stream(streaming_df)
    
    # Write final output to HBase
    query = streaming_df.writeStream \
        .foreachBatch(write_to_hbase) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints_hbase") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()