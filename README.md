# CryptoFlow

A real-time cryptocurrency price tracking pipeline built with Apache Kafka, Spark Structured Streaming, MongoDB Atlas, and Streamlit.

## Live Demo
https://cryptoflow.streamlit.app/

## Architecture
CoinGecko API -> Apache Kafka -> Spark Structured Streaming -> MongoDB Atlas -> Streamlit Dashboard

## Tech Stack
- Apache Kafka - real-time message streaming
- Apache Spark (PySpark) - structured streaming and data processing
- MongoDB Atlas - cloud database for storing price history
- Streamlit - live dashboard and visualization
- CoinGecko API - free real-time crypto price data

## Project Structure
- crypto_producer.py - Fetches prices from CoinGecko and sends to Kafka
- crypto_spark.py - Spark Structured Streaming consumer, writes to MongoDB
- crypto_dashboard.py - Streamlit dashboard, reads from MongoDB Atlas
- requirements.txt - Python dependencies

## Running Locally
1. Install dependencies: pip install -r requirements.txt
2. Start Zookeeper and Kafka broker
3. Run crypto_producer.py
4. Run crypto_spark.py with spark-submit
5. Run streamlit run crypto_dashboard.py

## Features
- Live price tracking for BTC, ETH, BNB, SOL, DOGE
- 24h price change indicators
- Price history charts
- Auto-refreshes every 30 seconds

## Roadmap
- Email price alerts
- Reddit sentiment analysis per coin
- Price prediction using Spark MLlib
