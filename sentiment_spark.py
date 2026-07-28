"""
CryptoFlow - Sentiment Spark Consumer
"""
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pymongo import MongoClient

ATLAS_URI = "mongodb+srv://furkmez20_db_user:Hitnap129.@cryptoflow.n8fpzx2.mongodb.net/cryptodb.sentiment?retryWrites=true&w=majority&appName=cryptoflow"
MONGO_URI = "mongodb+srv://furkmez20_db_user:Hitnap129.@cryptoflow.n8fpzx2.mongodb.net/?retryWrites=true&w=majority&appName=cryptoflow"

SCHEMA = StructType([
    StructField("coin", StringType()),
    StructField("title", StringType()),
    StructField("sentiment_score", DoubleType()),
    StructField("sentiment_label", StringType()),
    StructField("source", StringType()),
    StructField("published_at", StringType()),
    StructField("timestamp", StringType()),
])

def main(bootstrap_servers):
    spark = SparkSession.builder \
        .appName("CryptoSentiment") \
        .config("spark.mongodb.write.connection.uri", ATLAS_URI) \
        .config("spark.mongodb.output.uri", ATLAS_URI) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", "crypto-sentiment") \
        .option("startingOffsets", "latest") \
        .load()

    parsed = raw.select(
        from_json(col("value").cast("string"), SCHEMA).alias("data")
    ).select("data.*") \
     .withColumn("timestamp", col("timestamp").cast("timestamp"))

    def write_sentiment(batch_df, batch_id):
        if batch_df.count() == 0:
            return

        # Save raw articles to MongoDB
        batch_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "cryptodb") \
            .option("collection", "sentiment_raw") \
            .save()

        # Aggregate per coin and save
        agg_df = batch_df.groupBy("coin").agg(
            avg("sentiment_score").alias("avg_sentiment"),
            count("*").alias("article_count")
        )

        pandas_df = agg_df.toPandas()
        client = MongoClient(MONGO_URI)
        db = client["cryptodb"]
        now = datetime.utcnow()
        for _, row in pandas_df.iterrows():
            db.sentiment_agg.insert_one({
                "coin": row["coin"],
                "avg_sentiment": float(row["avg_sentiment"]),
                "article_count": int(row["article_count"]),
                "timestamp": now
            })
        client.close()

        print(f"[Sentiment Spark] Batch {batch_id} written")
        agg_df.show()

    query = parsed.writeStream \
        .foreachBatch(write_sentiment) \
        .option("checkpointLocation", "/tmp/sentiment_checkpoint") \
        .trigger(processingTime="60 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit ... sentiment_spark.py <bootstrap_servers>")
        sys.exit(1)
    main(sys.argv[1])
