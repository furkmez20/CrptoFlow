"""
Crypto Price Tracker - Producer
Reads live prices from CoinGecko and writes to Kafka topic: crypto-prices
Usage:
    python crypto_producer.py <kafka_bootstrap_servers> [--interval SECONDS]
Example:
    python crypto_producer.py localhost:9092 --interval 30
"""
import json
import time
import argparse
import requests
from kafka import KafkaProducer

COINS = ['bitcoin', 'ethereum', 'solana', 'dogecoin', 'binancecoin']
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

def create_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

def fetch_and_send(producer, topic, interval_seconds=30):
    print(f"[Producer] Starting. Sending to topic '{topic}' every {interval_seconds}s ...")
    while True:
        try:
            params = {
                'vs_currency': 'usd',
                'ids': ','.join(COINS),
                'price_change_percentage': '24h'
            }
            response = requests.get(COINGECKO_URL, params=params, timeout=10)
            data = response.json()

            for coin in data:
                message = {
                    'id': coin['id'],
                    'symbol': coin['symbol'],
                    'current_price': coin['current_price'],
                    'price_change_pct_24h': coin.get('price_change_percentage_24h', 0),
                    'market_cap': coin['market_cap'],
                    'total_volume': coin['total_volume'],
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                }
                producer.send(topic, value=message)
                print(f"[Producer] Sent: {coin['id']} @ ${coin['current_price']}")

            producer.flush()
            print(f"[Producer] Batch done. Sleeping {interval_seconds}s ...")

        except Exception as e:
            print(f"[Producer] Error: {e}")

        time.sleep(interval_seconds)

def main():
    parser = argparse.ArgumentParser(description="CoinGecko -> Kafka Producer")
    parser.add_argument("bootstrap_servers", help="Kafka bootstrap servers e.g. localhost:9092")
    parser.add_argument("--topic", default="crypto-prices", help="Kafka topic name")
    parser.add_argument("--interval", type=int, default=30, help="Poll interval in seconds")
    args = parser.parse_args()

    producer = create_producer(args.bootstrap_servers)
    fetch_and_send(producer, args.topic, args.interval)

if __name__ == "__main__":
    main()
