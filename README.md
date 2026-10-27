# CryptoFlow

CryptoFlow is a real-time cryptocurrency monitoring pipeline. It collects market data, streams it through Kafka and Spark, stores results in MongoDB Atlas, and displays price and analytics views in a Streamlit dashboard.

[Open the live dashboard](https://cryptoflow.streamlit.app/)

## Features

- Tracks Bitcoin, Ethereum, BNB, Solana, and Dogecoin prices and 24-hour changes.
- Displays recent price history in an auto-refreshing dashboard.
- Includes separate scripts for price ingestion, sentiment processing, and a Spark MLlib price-direction experiment.

## Stack

Python, Streamlit, Apache Kafka, Apache Spark Structured Streaming, MongoDB Atlas, CoinGecko API, Pandas, and Plotly.

## Architecture

```text
CoinGecko API -> Kafka price topic -> Spark Structured Streaming -> MongoDB Atlas -> Streamlit dashboard
News sources -> sentiment producer -> Kafka sentiment topic -> Spark aggregation -> MongoDB Atlas
MongoDB price history -> Spark MLlib price predictor
```

The dashboard reads stored data from MongoDB. The price predictor is a separate batch workflow and is not required to run the dashboard.

## Run locally

### Prerequisites

- Python and the packages listed in `requirements.txt`.
- A Kafka broker (and the Spark runtime/connectors needed to read Kafka and write to MongoDB).
- A MongoDB Atlas database configured for the application.
- CoinGecko API access.

Install the Python dependencies:

```bash
pip install -r requirements.txt
```

Review the Kafka and MongoDB connection settings in the scripts and configure them for your own services before running. Keep credentials out of source control.

Start the price producer, replacing the broker address if needed:

```bash
python crypto_producer.py localhost:9092
```

In a second terminal, start the Spark streaming consumer:

```bash
spark-submit crypto_spark.py localhost:9092
```

Then start the dashboard:

```bash
streamlit run crypto_dashboard.py
```

The sentiment pipeline and price predictor are optional workflows; see their scripts for their topic and Spark requirements.

## Repository

[furkmez20/CrptoFlow](https://github.com/furkmez20/CrptoFlow)
