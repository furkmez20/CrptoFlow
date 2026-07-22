"""
Crypto Price Tracker - Streamlit Dashboard
"""
import time
import streamlit as st
import pandas as pd
from pymongo import MongoClient

st.set_page_config(page_title="CryptoFlow", layout="wide")
st.title("CryptoFlow — Real-Time Crypto Price Tracker")

@st.cache_resource
def get_mongo_client():
    return MongoClient("mongodb+srv://furkmez20_db_user:Hitnap129.@cryptoflow.n8fpzx2.mongodb.net/?retryWrites=true&w=majority&appName=cryptoflow")

def load_data():
    client = get_mongo_client()
    db = client["cryptodb"]
    docs = list(db.prices.find({}, {"_id": 0}))
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

placeholder = st.empty()

while True:
    df = load_data()

    with placeholder.container():
        if df.empty:
            st.info("Waiting for data... make sure producer and Spark are running.")
        else:
            latest = df.sort_values("timestamp").groupby("id").last().reset_index()

            st.subheader("Current Prices")
            cols = st.columns(len(latest))
            for i, row in latest.iterrows():
                change = row["price_change_pct_24h"]
                arrow = "▲" if change >= 0 else "▼"
                cols[i].metric(
                    label=row["id"].capitalize(),
                    value=f"${row['current_price']:,.4f}",
                    delta=f"{arrow} {change:.2f}% 24h"
                )

            st.subheader("Price History")
            for coin in df["id"].unique():
                coin_df = df[df["id"] == coin].sort_values("timestamp")
                st.line_chart(
                    coin_df.set_index("timestamp")["current_price"],
                    use_container_width=True
                )
                st.caption(coin.capitalize())

            st.caption(f"Last updated: {df['timestamp'].max()}")

    time.sleep(30)
