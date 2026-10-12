"""
CryptoFlow - Price Prediction using Spark MLlib
Trains a Random Forest classifier to predict price direction (up/down)
Usage:
    spark-submit price_predictor.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pymongo import MongoClient
from datetime import datetime
import pandas as pd

MONGO_URI = "mongodb+srv://furkmez20_db_user:Hitnap129.@cryptoflow.n8fpzx2.mongodb.net/?retryWrites=true&w=majority&appName=cryptoflow"
ATLAS_URI = "mongodb+srv://furkmez20_db_user:Hitnap129.@cryptoflow.n8fpzx2.mongodb.net/cryptodb.prices?retryWrites=true&w=majority&appName=cryptoflow"

def main():
    spark = SparkSession.builder \
        .appName("CryptoPricePredictor") \
        .config("spark.mongodb.read.connection.uri", ATLAS_URI) \
        .config("spark.mongodb.input.uri", ATLAS_URI) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("[Predictor] Loading price data from MongoDB...")

    df = spark.read \
        .format("mongodb") \
        .option("database", "cryptodb") \
        .option("collection", "prices") \
        .load()

    print(f"[Predictor] Loaded {df.count()} records")

    coins = ["bitcoin", "ethereum", "binancecoin", "solana", "dogecoin"]
    results = []

    for coin in coins:
        print(f"[Predictor] Training model for {coin}...")

        coin_df = df.filter(col("id") == coin) \
            .select("timestamp", "current_price", "price_change_pct_24h", "total_volume") \
            .orderBy("timestamp")

        if coin_df.count() < 20:
            print(f"[Predictor] Not enough data for {coin}, skipping...")
            continue

        # Feature engineering
        w = Window.orderBy("timestamp")
        coin_df = coin_df \
            .withColumn("prev_price", lag("current_price", 1).over(w)) \
            .withColumn("prev_price_2", lag("current_price", 2).over(w)) \
            .withColumn("prev_volume", lag("total_volume", 1).over(w)) \
            .withColumn("price_change", (col("current_price") - col("prev_price")) / col("prev_price")) \
            .withColumn("next_price", lag("current_price", -1).over(w)) \
            .withColumn("label", when(col("next_price") > col("current_price"), 1.0).otherwise(0.0)) \
            .dropna()

        if coin_df.count() < 15:
            print(f"[Predictor] Still not enough data for {coin} after feature engineering, skipping...")
            continue

        # Assemble features
        feature_cols = ["current_price", "price_change_pct_24h", "total_volume", "price_change"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features")
        rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50)
        pipeline = Pipeline(stages=[assembler, scaler, rf])

        # Train/test split
        train, test = coin_df.randomSplit([0.8, 0.2], seed=42)

        if train.count() < 5 or test.count() < 2:
            print(f"[Predictor] Not enough data to split for {coin}, skipping...")
            continue

        model = pipeline.fit(train)
        predictions = model.transform(test)

        evaluator = BinaryClassificationEvaluator(labelCol="label")
        auc = evaluator.evaluate(predictions)

        # Predict on latest data point
        latest = coin_df.orderBy(col("timestamp").desc()).limit(1)
        latest_pred = model.transform(latest)
        pred_row = latest_pred.select("prediction", "probability").first()
        direction = "up" if pred_row["prediction"] == 1.0 else "down"
        confidence = float(pred_row["probability"][int(pred_row["prediction"])])

        print(f"[Predictor] {coin}: direction={direction}, confidence={confidence:.2%}, AUC={auc:.3f}")

        results.append({
            "coin": coin,
            "prediction": direction,
            "confidence": confidence,
            "auc": auc,
            "timestamp": datetime.utcnow()
        })

    # Save predictions to MongoDB
    if results:
        client = MongoClient(MONGO_URI)
        db = client["cryptodb"]
        db.predictions.delete_many({})  # clear old predictions
        db.predictions.insert_many(results)
        client.close()
        print(f"[Predictor] Saved {len(results)} predictions to MongoDB")
    else:
        print("[Predictor] No predictions to save — need more data")

    spark.stop()

if __name__ == "__main__":
    main()
