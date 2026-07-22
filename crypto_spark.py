"""
Crypto Price Tracker - Spark Structured Streaming Consumer
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

ATLAS_URI = "mongodb+srv://furkmez20_db_user:Hitnap129.@cryptoflow.n8fpzx2.mongodb.net/cryptodb.prices?retryWrites=true&w=majority&appName=cryptoflow"

SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("symbol", StringType()),
    StructField("current_price", DoubleType()),
    StructField("price_change_pct_24h", DoubleType()),
    StructField("market_cap", LongType()),
    StructField("total_volume", LongType()),
    StructField("timestamp", StringType()),
])

def main(bootstrap_servers):
    spark = SparkSession.builder \
        .appName("CryptoPriceTracker") \
        .config("spark.mongodb.write.connection.uri", ATLAS_URI) \
        .config("spark.mongodb.output.uri", ATLAS_URI) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", "crypto-prices") \
        .option("startingOffsets", "latest") \
        .load()

    parsed = raw.select(
        from_json(col("value").cast("string"), SCHEMA).alias("data")
    ).select("data.*") \
     .withColumn("timestamp", col("timestamp").cast("timestamp"))

    def write_to_mongo(batch_df, batch_id):
        if batch_df.count() == 0:
            return
        batch_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "cryptodb") \
            .option("collection", "prices") \
            .save()
        print(f"[Spark] Batch {batch_id} written to MongoDB Atlas")
        batch_df.show(truncate=False)

    query = parsed.writeStream \
        .foreachBatch(write_to_mongo) \
        .option("checkpointLocation", "/tmp/crypto_checkpoint") \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit ... crypto_spark.py <bootstrap_servers>")
        sys.exit(1)
    main(sys.argv[1])
