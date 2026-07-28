"""
CryptoFlow - Sentiment Producer
Pulls news headlines from NewsAPI, runs sentiment analysis, sends to Kafka
Usage:
    python3 sentiment_producer.py <kafka_bootstrap_servers> <newsapi_key>
Example:
    python3 sentiment_producer.py localhost:9092 YOUR_API_KEY
"""
import sys
import json
import time
import argparse
from datetime import datetime
from textblob import TextBlob
from kafka import KafkaProducer
from newsapi import NewsApiClient

COIN_QUERIES = {
    "bitcoin": "bitcoin BTC crypto",
    "ethereum": "ethereum ETH crypto",
    "binancecoin": "binance BNB crypto",
    "solana": "solana SOL crypto",
    "dogecoin": "dogecoin DOGE crypto"
}

def create_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

def get_sentiment(text):
    analysis = TextBlob(text)
    score = analysis.sentiment.polarity
    if score > 0.1:
        label = "positive"
    elif score < -0.1:
        label = "negative"
    else:
        label = "neutral"
    return score, label

def fetch_and_send(api_key, producer, interval_seconds=300):
    newsapi = NewsApiClient(api_key=api_key)
    print(f"[Sentiment] Starting. Polling every {interval_seconds}s ...")

    while True:
        for coin, query in COIN_QUERIES.items():
            try:
                response = newsapi.get_everything(
                    q=query,
                    language="en",
                    sort_by="publishedAt",
                    page_size=10
                )
                articles = response.get("articles", [])
                scores = []

                for article in articles:
                    title = article.get("title") or ""
                    description = article.get("description") or ""
                    text = f"{title}. {description}"
                    score, label = get_sentiment(text)
                    scores.append(score)

                    message = {
                        "coin": coin,
                        "title": title,
                        "sentiment_score": score,
                        "sentiment_label": label,
                        "source": article.get("source", {}).get("name", "unknown"),
                        "published_at": article.get("publishedAt", ""),
                        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                    }
                    producer.send("crypto-sentiment", value=message)

                avg_score = sum(scores) / len(scores) if scores else 0
                print(f"[Sentiment] {coin}: {len(articles)} articles, avg score: {avg_score:.3f}")

            except Exception as e:
                print(f"[Sentiment] Error for {coin}: {e}")

        producer.flush()
        print(f"[Sentiment] Batch done. Sleeping {interval_seconds}s ...")
        time.sleep(interval_seconds)

def main():
    parser = argparse.ArgumentParser(description="NewsAPI -> Kafka Sentiment Producer")
    parser.add_argument("bootstrap_servers", help="Kafka bootstrap servers")
    parser.add_argument("api_key", help="NewsAPI key")
    parser.add_argument("--interval", type=int, default=300, help="Poll interval in seconds")
    args = parser.parse_args()

    producer = create_producer(args.bootstrap_servers)
    fetch_and_send(args.api_key, producer, args.interval)

if __name__ == "__main__":
    main()
