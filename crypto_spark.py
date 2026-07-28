"""
CryptoFlow - Spark Structured Streaming with Subscription Alerts
"""
import sys
import time
import smtplib
from email.mime.text import MIMEText
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pymongo import MongoClient

ATLAS_URI = "mongodb+srv://furkmez20_db_user:Hitnap129.@cryptoflow.n8fpzx2.mongodb.net/cryptodb.prices?retryWrites=true&w=majority&appName=cryptoflow"
MONGO_URI = "mongodb+srv://furkmez20_db_user:Hitnap129.@cryptoflow.n8fpzx2.mongodb.net/?retryWrites=true&w=majority&appName=cryptoflow"

GMAIL_SENDER = "furkmez20@gmail.com"
GMAIL_APP_PASSWORD = "giui uzgy nspr ylht"
COOLDOWN_SECONDS = 3600
last_alert_time = {}

SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("symbol", StringType()),
    StructField("current_price", DoubleType()),
    StructField("price_change_pct_24h", DoubleType()),
    StructField("market_cap", LongType()),
    StructField("total_volume", LongType()),
    StructField("timestamp", StringType()),
])

def send_email(to_email, subject, body):
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = GMAIL_SENDER
        msg["To"] = to_email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_SENDER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_SENDER, to_email, msg.as_string())
        print(f"[Alert] Email sent to {to_email}: {subject}")
    except Exception as e:
        print(f"[Alert] Email failed: {e}")

def get_subscribers(coin, direction):
    try:
        client = MongoClient(MONGO_URI)
        db = client["cryptodb"]
        subs = list(db.subscriptions.find({
            "coin": coin,
            "direction": direction
        }))
        client.close()
        return subs
    except Exception as e:
        print(f"[Alert] Failed to fetch subscribers: {e}")
        return []

def check_alerts(batch_df):
    rows = batch_df.collect()
    for row in rows:
        coin = row["id"]
        change = row["price_change_pct_24h"]
        price = row["current_price"]
        now = time.time()

        if coin in last_alert_time:
            if now - last_alert_time[coin] < COOLDOWN_SECONDS:
                print(f"[Alert] Skipping {coin} - cooldown active")
                continue

        direction = None
        if change < 0:
            direction = "drop"
        elif change > 0:
            direction = "rise"

        if direction:
            subscribers = get_subscribers(coin, direction)
            for sub in subscribers:
                threshold = sub.get("threshold", 2.0)
                if (direction == "drop" and change <= -threshold) or \
                   (direction == "rise" and change >= threshold):
                    arrow = "📉" if direction == "drop" else "📈"
                    send_email(
                        sub["email"],
                        f"CryptoFlow {arrow} {coin.capitalize()} {'dropped' if direction == 'drop' else 'surged'} {abs(change):.2f}%!",
                        f"Hi,\n\n{coin.capitalize()} has {'dropped' if direction == 'drop' else 'risen'} {abs(change):.2f}% in the last 24h.\n\nCurrent price: ${price:,.4f}\nYour threshold: {threshold}%\n\n— CryptoFlow"
                    )
            last_alert_time[coin] = now

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
