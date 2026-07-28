"""
CryptoFlow - Enhanced Streamlit Dashboard (Branded)
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pymongo import MongoClient

st.set_page_config(page_title="CryptoFlow", layout="wide", page_icon="⚡")

# ── Brand tokens ──────────────────────────────────────────────────────────
BRAND_PRIMARY = "#3B82F6"      # electric blue — logo, active tab, primary accents
BRAND_PRIMARY_ALT = "#00F2FE"  # cyan — gradient partner for logo/glow
BG = "#0d1117"
CARD_BG = "#161b22"
CARD_BORDER = "#30363d"
TEXT_MUTED = "#8b949e"
TEXT_MAIN = "#e6edf3"
GREEN = "#3fb950"
RED = "#f85149"
GRID_SUBTLE = "#2A2E39"

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"] {{ font-family: 'Inter', sans-serif; }}

.stApp {{ background-color: {BG}; }}

/* ── Top nav bar ── */
.brand-nav {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 14px 22px;
    background: {CARD_BG};
    border: 1px solid {CARD_BORDER};
    border-radius: 14px;
    margin-bottom: 22px;
}}
.brand-nav .logo {{
    display: flex;
    align-items: center;
    gap: 10px;
    font-size: 1.4rem;
    font-weight: 800;
}}
.brand-nav .logo .glyph {{
    background: linear-gradient(135deg, {BRAND_PRIMARY_ALT}, {BRAND_PRIMARY});
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    filter: drop-shadow(0 0 6px {BRAND_PRIMARY_ALT}66);
}}
.brand-nav .logo .wordmark span {{ color: {TEXT_MUTED}; font-weight: 500; }}
.brand-nav .links {{
    display: flex;
    gap: 28px;
    font-size: 0.92rem;
    font-weight: 600;
    color: {TEXT_MUTED};
}}
.brand-nav .links .active {{ color: {BRAND_PRIMARY}; }}
.brand-nav .status {{
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 0.85rem;
    font-weight: 600;
    color: {TEXT_MUTED};
}}
.pulse-dot {{
    width: 9px; height: 9px;
    border-radius: 50%;
    background: {GREEN};
    box-shadow: 0 0 0 0 {GREEN}99;
    animation: pulse 1.8s infinite;
}}
@keyframes pulse {{
    0%   {{ box-shadow: 0 0 0 0 {GREEN}66; }}
    70%  {{ box-shadow: 0 0 0 7px rgba(0,0,0,0); }}
    100% {{ box-shadow: 0 0 0 0 rgba(0,0,0,0); }}
}}

/* ── Sparkline metric cards ── */
.coin-card {{
    background: {CARD_BG};
    border: 1px solid {CARD_BORDER};
    border-radius: 14px;
    padding: 14px 16px 4px 16px;
    transition: border-color 0.15s ease;
}}
.coin-card:hover {{ border-color: {BRAND_PRIMARY}55; }}
.coin-card .label {{
    color: {TEXT_MUTED};
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 1px;
    font-weight: 600;
}}
.coin-card .price {{
    color: {TEXT_MAIN};
    font-size: 1.5rem;
    font-weight: 700;
    margin: 2px 0 2px 0;
}}
.coin-card .delta {{
    font-size: 0.82rem;
    font-weight: 600;
}}
.coin-card .delta.up {{ color: {GREEN}; }}
.coin-card .delta.down {{ color: {RED}; }}

[data-testid="metric-container"] {{
    background-color: {CARD_BG};
    border: 1px solid {CARD_BORDER};
    border-radius: 12px;
    padding: 16px;
}}
[data-testid="metric-container"] label {{
    color: {TEXT_MUTED} !important;
    font-size: 12px !important;
    text-transform: uppercase;
    letter-spacing: 1px;
}}
[data-testid="stMetricValue"] {{ color: {TEXT_MAIN} !important; font-weight: 700 !important; }}
[data-testid="stMetricDelta"] svg {{ display: none; }}

/* ── Subscription panel ── */
.sub-card {{
    background: {CARD_BG};
    border: 1px solid {CARD_BORDER};
    border-radius: 14px;
    padding: 18px 20px;
}}
.sub-row {{
    background: {CARD_BG};
    border: 1px solid {CARD_BORDER};
    border-radius: 10px;
    padding: 10px 14px;
    margin-bottom: 8px;
}}
div[data-testid="stTextInput"] input,
div[data-testid="stNumberInput"] input,
div[data-testid="stSelectbox"] > div {{
    background-color: {CARD_BG} !important;
    border-color: {CARD_BORDER} !important;
    color: {TEXT_MAIN} !important;
}}
.stButton > button[kind="primary"] {{
    background: linear-gradient(90deg, {BRAND_PRIMARY_ALT}, {BRAND_PRIMARY});
    border: none;
    font-weight: 700;
}}

h1 {{
    background: linear-gradient(90deg, {BRAND_PRIMARY_ALT}, {BRAND_PRIMARY}, #9945ff);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-size: 2.2rem !important;
    font-weight: 800 !important;
}}
h2, h3, h4 {{ color: {TEXT_MAIN} !important; }}
[data-testid="stText"] p {{ color: {TEXT_MAIN} !important; }}
[data-testid="stMarkdownContainer"] p {{ color: {TEXT_MAIN} !important; }}
div[data-testid="column"] p {{ color: {TEXT_MAIN} !important; }}
hr {{ border-color: {CARD_BORDER} !important; }}
.stCaption {{ color: {TEXT_MAIN} !important; }}
</style>
""", unsafe_allow_html=True)

COIN_COLORS = {
    "bitcoin": "#f7931a",
    "ethereum": "#627eea",
    "binancecoin": "#f3ba2f",
    "solana": "#9945ff",
    "dogecoin": "#c2a633"
}

PLOTLY_THEME = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor=CARD_BG,
    font=dict(color=TEXT_MUTED, family="Inter"),
)


def hex_to_rgba(hex_color, alpha=0.15):
    r = int(hex_color[1:3], 16)
    g = int(hex_color[3:5], 16)
    b = int(hex_color[5:7], 16)
    return f"rgba({r},{g},{b},{alpha})"


def tight_range(series, padding=0.02):
    mn = series.min()
    mx = series.max()
    pad = (mx - mn) * padding if mx != mn else mn * 0.01
    return [mn - pad, mx + pad]


def resample_coin(coin_df, freq="30min"):
    """Resample to regular intervals and forward fill gaps to avoid diagonal lines"""
    coin_df = coin_df.set_index("timestamp")
    coin_df = coin_df.resample(freq).last()
    coin_df = coin_df.ffill()
    coin_df = coin_df.reset_index()
    return coin_df


def render_brand_nav():
    st.markdown(f"""
    <div class="brand-nav">
        <div class="logo">
            <span class="glyph">⚡</span>
            <span class="wordmark">Crypto<span>Flow</span></span>
        </div>
        <div class="links">
            <span class="active">Market</span>
            <span>Analytics</span>
            <span>Portfolio</span>
        </div>
        <div class="status">
            <span class="pulse-dot"></span> Live Updates
        </div>
    </div>
    """, unsafe_allow_html=True)


def make_sparkline(coin_df, color, up):
    """Tiny axis-less trend line for the top metric cards."""
    fig = go.Figure(go.Scatter(
        x=coin_df["timestamp"],
        y=coin_df["current_price"],
        mode="lines",
        line=dict(color=GREEN if up else RED, width=2),
        fill="tozeroy",
        fillcolor=hex_to_rgba(GREEN if up else RED, 0.12),
        hoverinfo="skip",
    ))
    fig.update_layout(
        height=52,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(visible=False),
        yaxis=dict(visible=False, range=tight_range(coin_df["current_price"], padding=0.15)),
        showlegend=False,
    )
    return fig


def render_coin_cards(df, latest, coin_list):
    st.markdown('<div class="label" style="margin-bottom:8px;">Current Prices</div>', unsafe_allow_html=True)
    cols = st.columns(len(coin_list))
    for i, coin in enumerate(coin_list):
        row = latest[latest["id"] == coin].iloc[0]
        change = row["price_change_pct_24h"]
        up = change >= 0
        arrow = "▲" if up else "▼"
        with cols[i]:
            st.markdown(f"""
            <div class="coin-card">
                <div class="label">{coin.capitalize()}</div>
                <div class="price">${row['current_price']:,.4f}</div>
                <div class="delta {'up' if up else 'down'}">{arrow} {change:.2f}% 24h</div>
            </div>
            """, unsafe_allow_html=True)
            coin_df = df[df["id"] == coin].sort_values("timestamp").copy()
            if len(coin_df) > 1:
                coin_df = resample_coin(coin_df)
                st.plotly_chart(
                    make_sparkline(coin_df, COIN_COLORS.get(coin, "#fff"), up),
                    use_container_width=True,
                    config={"staticPlot": True, "displayModeBar": False},
                )


@st.cache_resource
def get_mongo_client():
    uri = st.secrets["MONGO_URI"]
    return MongoClient(uri)


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


def save_subscription(email, coin, threshold, direction):
    client = get_mongo_client()
    db = client["cryptodb"]
    existing = db.subscriptions.find_one({"email": email, "coin": coin})
    if existing:
        db.subscriptions.update_one(
            {"email": email, "coin": coin},
            {"$set": {"threshold": threshold, "direction": direction}}
        )
        return "updated"
    else:
        db.subscriptions.insert_one({
            "email": email,
            "coin": coin,
            "threshold": threshold,
            "direction": direction,
            "created_at": pd.Timestamp.now()
        })
        return "created"


def delete_subscription(email, coin):
    client = get_mongo_client()
    db = client["cryptodb"]
    db.subscriptions.delete_one({"email": email, "coin": coin})


def render_detail_panel(coin, coin_df, color):
    st.markdown(f"### 🔍 {coin.capitalize()} — Detailed View")

    latest = coin_df.iloc[-1]
    first = coin_df.iloc[0]
    high = coin_df["current_price"].max()
    low = coin_df["current_price"].min()
    total_change = ((latest["current_price"] - first["current_price"]) / first["current_price"]) * 100

    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("Current", f"${latest['current_price']:,.4f}")
    s2.metric("24h Change", f"{latest['price_change_pct_24h']:.2f}%")
    s3.metric("Period High", f"${high:,.4f}")
    s4.metric("Period Low", f"${low:,.4f}")
    s5.metric("Total Change", f"{total_change:.2f}%")

    price_range = tight_range(coin_df["current_price"])

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        row_heights=[0.8, 0.2],
        vertical_spacing=0.02
    )

    fig.add_trace(go.Scatter(
        x=coin_df["timestamp"],
        y=coin_df["current_price"],
        mode="lines",
        line=dict(color=color, width=2),
        name="Price",
        hovertemplate="$%{y:,.4f}<extra></extra>"
    ), row=1, col=1)

    fig.add_trace(go.Bar(
        x=coin_df["timestamp"],
        y=coin_df["total_volume"],
        name="Volume",
        marker=dict(color=hex_to_rgba(color, 0.6)),
        hovertemplate="Vol: $%{y:,.0f}<extra></extra>"
    ), row=2, col=1)

    fig.update_layout(
        **PLOTLY_THEME,
        height=500,
        margin=dict(l=0, r=0, t=10, b=0),
        hovermode="x unified",
        showlegend=False,
        xaxis=dict(gridcolor="#21262d", showticklabels=False),
        yaxis=dict(gridcolor="#21262d", title="Price (USD)", range=price_range),
        xaxis2=dict(gridcolor="#21262d"),
        yaxis2=dict(gridcolor="#21262d", title="Vol"),
    )
    st.plotly_chart(fig, use_container_width=True)


if "selected_coin" not in st.session_state:
    st.session_state.selected_coin = "bitcoin"

render_brand_nav()

st.title("CryptoFlow")
st.caption("⚡ Live prices powered by CoinGecko · Updates every 30s")

df = load_data()

if df.empty:
    st.info("Waiting for data...")
else:
    latest = df.sort_values("timestamp").groupby("id").last().reset_index()
    coin_list = list(df["id"].unique())

    render_coin_cards(df, latest, coin_list)

    st.divider()

    # Comparison chart — resampled to fix diagonal lines
    st.subheader("Price Movement Comparison")
    fig_compare = go.Figure()
    for coin in coin_list:
        coin_df = df[df["id"] == coin].sort_values("timestamp").copy()
        if len(coin_df) > 1:
            coin_df = resample_coin(coin_df)
            base = coin_df["current_price"].iloc[0]
            pct_change = ((coin_df["current_price"] - base) / base) * 100
            color = COIN_COLORS.get(coin, "#ffffff")
            fig_compare.add_trace(go.Scatter(
                x=coin_df["timestamp"],
                y=pct_change,
                name=coin.capitalize(),
                mode="lines",
                line=dict(color=color, width=2),
                hovertemplate="%{y:.2f}%<extra>" + coin.capitalize() + "</extra>"
            ))
    fig_compare.update_layout(
        **PLOTLY_THEME,
        height=350,
        margin=dict(l=0, r=0, t=10, b=40),
        legend=dict(orientation="h", y=-0.15, font=dict(color=TEXT_MUTED)),
        yaxis_title="% Change",
        hovermode="x unified",
        xaxis=dict(gridcolor="#21262d"),
        yaxis=dict(gridcolor="#21262d"),
    )
    st.plotly_chart(fig_compare, use_container_width=True)

    st.divider()

    # Mini charts
    st.subheader("Individual Price History")
    st.caption("Click a coin to see detailed view below")

    btn_cols = st.columns(len(coin_list))
    for i, coin in enumerate(coin_list):
        with btn_cols[i]:
            st.button(
                f"● {coin.capitalize()}",
                key=f"btn_{coin}",
                on_click=lambda c=coin: st.session_state.update({"selected_coin": c})
            )

    n = len(coin_list)
    fig_sub = make_subplots(rows=1, cols=n, subplot_titles=[c.capitalize() for c in coin_list])
    for i, coin in enumerate(coin_list):
        coin_df = df[df["id"] == coin].sort_values("timestamp").copy()
        coin_df = resample_coin(coin_df)
        color = COIN_COLORS.get(coin, "#ffffff")
        price_range = tight_range(coin_df["current_price"])
        fig_sub.add_trace(
            go.Scatter(
                x=coin_df["timestamp"],
                y=coin_df["current_price"],
                mode="lines",
                line=dict(color=color, width=2),
                showlegend=False
            ),
            row=1, col=i + 1
        )
        fig_sub.update_xaxes(showticklabels=False, gridcolor="#21262d", row=1, col=i + 1)
        fig_sub.update_yaxes(gridcolor="#21262d", range=price_range, row=1, col=i + 1)

    fig_sub.update_layout(
        **PLOTLY_THEME,
        height=250,
        margin=dict(l=0, r=0, t=30, b=0),
    )
    for i, ann in enumerate(fig_sub.layout.annotations):
        ann.font.color = COIN_COLORS.get(coin_list[i], "#ffffff")
        ann.font.size = 13

    st.plotly_chart(fig_sub, use_container_width=True)

    vol_cols = st.columns(n)
    for i, coin in enumerate(coin_list):
        coin_df = df[df["id"] == coin].sort_values("timestamp")
        vol = coin_df["total_volume"].iloc[-1]
        vol_cols[i].caption(f"Vol: ${vol/1e9:.2f}B")

    st.divider()

    # Detail panel
    selected = st.session_state.selected_coin
    color = COIN_COLORS.get(selected, "#ffffff")
    coin_df = df[df["id"] == selected].sort_values("timestamp").copy()
    coin_df = resample_coin(coin_df)
    render_detail_panel(selected, coin_df, color)

    st.divider()

    # Bottom row
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Market Cap Share")
        fig_pie = go.Figure(go.Pie(
            labels=latest["id"].str.capitalize(),
            values=latest["market_cap"],
            hole=0.5,
            marker=dict(colors=[COIN_COLORS.get(c, "#fff") for c in latest["id"]])
        ))
        fig_pie.update_layout(
            **PLOTLY_THEME,
            height=300,
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(font=dict(color=TEXT_MUTED))
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        st.subheader("24h Change")
        fig_bar = go.Figure(go.Bar(
            x=latest["id"].str.capitalize(),
            y=latest["price_change_pct_24h"],
            marker=dict(
                color=latest["price_change_pct_24h"].apply(
                    lambda x: GREEN if x >= 0 else RED
                )
            ),
            text=latest["price_change_pct_24h"].apply(lambda x: f"{x:.2f}%"),
            textposition="outside",
            textfont=dict(color=TEXT_MAIN)
        ))
        fig_bar.update_layout(
            **PLOTLY_THEME,
            height=300,
            margin=dict(l=0, r=0, t=10, b=0),
            yaxis_title="% Change",
            showlegend=False,
            xaxis=dict(gridcolor=GRID_SUBTLE),
            yaxis=dict(gridcolor=GRID_SUBTLE),
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    st.divider()

    # Subscription form
    st.subheader("🔔 Price Alert Subscriptions")
    st.caption("Get emailed when your coin moves past your threshold")

    with st.container():
        f1, f2, f3, f4 = st.columns([2, 1, 1, 1])
        with f1:
            sub_email = st.text_input("Email address", placeholder="you@example.com")
        with f2:
            sub_coin = st.selectbox("Coin", [c.capitalize() for c in coin_list])
        with f3:
            sub_direction = st.selectbox("Alert when", ["Drops by", "Rises by"])
        with f4:
            sub_threshold = st.number_input("% threshold", min_value=0.1, max_value=50.0, value=2.0, step=0.5)

        if st.button("Subscribe", type="primary"):
            if not sub_email or "@" not in sub_email:
                st.error("Please enter a valid email address.")
            else:
                result = save_subscription(
                    sub_email,
                    sub_coin.lower(),
                    sub_threshold,
                    "drop" if sub_direction == "Drops by" else "rise"
                )
                if result == "created":
                    st.success(f"✅ Subscribed! You'll get alerts when {sub_coin} {sub_direction.lower()} {sub_threshold}%")
                else:
                    st.info(f"✅ Subscription updated for {sub_coin}!")

    # Show existing subscriptions
    st.markdown("#### Active Subscriptions")
    check_email = st.text_input("Enter your email to view/manage your subscriptions", key="check_email")
    if check_email and "@" in check_email:
        client = get_mongo_client()
        subs = list(client["cryptodb"].subscriptions.find({"email": check_email}, {"_id": 0}))
        if subs:
            for sub in subs:
                sc1, sc2, sc3, sc4 = st.columns([2, 2, 2, 1])
                sc1.write(sub["coin"].capitalize())
                sc2.write(f"{'Drops' if sub['direction'] == 'drop' else 'Rises'} by {sub['threshold']}%")
                sc3.write(f"Email: {sub['email']}")
                if sc4.button("Remove", key=f"del_{sub['coin']}"):
                    delete_subscription(check_email, sub["coin"])
                    st.rerun()
        else:
            st.caption("No subscriptions found for this email.")

    st.caption(f"Last updated: {df['timestamp'].max()}")
