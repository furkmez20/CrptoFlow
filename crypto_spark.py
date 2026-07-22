"""
CryptoFlow - Spark Structured Streaming Consumer with Email Alerts
"""
import sys
import smtplib
from email.mime.text import MIMEText
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

ATLAS_URI = "mongodb+srv://furkmez20_db_user:Hitnap129.@cryptoflow.n8fpzx2.mongodb.net/cryptodb.prices?retryWrites=true&w=majority&appName=cryptoflow"

GMAIL_SENDER = "furkmez20@gmail.com"
GMAIL_APP_PASSWORD = "giui uzgy nspr ylht"
ALERT_EMAIL = "furkmez20@gmail.com"
ALERT_THRESHOLD = 2.0

SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("symbol", StringType()),
    StructField("current_price", DoubleType()),
    StructField("price_change_pct_24h", DoubleType()),
    StructField("market_cap", LongType()),
    StructField("total_volume", LongType()),
    StructField("timestamp", StringType()),
])

def send_email(subject, body):
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = GMAIL_SENDER
        msg["To"] = ALERT_EMAIL
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_SENDER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_SENDER, ALERT_EMAIL, msg.as_string())
        print(f"[Alert] Email sent: {subject}")
    except Exception as e:
        print(f"[Alert] Email failed: {e}")

def check_alerts(batch_df):
    rows = batch_df.collect()
    for row in rows:
        change = row["price_change_pct_24h"]
        coin = row["id"].capitalize()
        price = row["current_price"]
        if change <= -ALERT_THRESHOLD:
            send_email(
                f"CryptoFlow Alert: {coin} dropped {change:.2f}%!",
                f"{coin} is down {abs(change):.2f}% in the last 24h.\nCurrent price: ${price:,.4f}"
            )
        elif change >= ALERT_THRESHOLD:
            send_email(
                f"CryptoFlow Alert: {coin} surged {change:.2f}%!",
                f"{coin} is up {change:.2f}% in the last 24h.\nCurrent price: ${price:,.4f}"
            )

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
        check_alerts(batch_df)

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
