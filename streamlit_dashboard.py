import streamlit as st
import pandas as pd
import altair as alt
from google.oauth2 import service_account
from google.cloud import bigquery
from streamlit_autorefresh import st_autorefresh
from datetime import datetime, timedelta
import plotly.graph_objects as go

st.set_page_config(page_title="Crypto Dashboard", layout="wide")
st.title("📈 Real-Time Cryptocurrency Dashboard")

# Auto-refresh every 60 seconds
st_autorefresh(interval=60 * 1000, key="dashboard_refresh")

# Load credentials
credentials = service_account.Credentials.from_service_account_file(
    "C:/Users/SrinidhiV/Downloads/crypto-data-pipeline-462115-b111275f7723.json"
)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Sidebar filters
st.sidebar.header("🔍 Filter Data")

# Time range selector
time_range = st.sidebar.selectbox("Select time range", [
    "Last 30 minutes", "Last 1 hour", "Last 4 hours", "Last 8 hours", "Last 24 hours"
])

now = datetime.utcnow()
time_map = {
    "Last 30 minutes": timedelta(minutes=30),
    "Last 1 hour": timedelta(hours=1),
    "Last 4 hours": timedelta(hours=4),
    "Last 8 hours": timedelta(hours=8),
    "Last 24 hours": timedelta(days=1)
}
time_cutoff = now - time_map[time_range]

# Fetch data from BigQuery
query = f"""
SELECT * FROM `crypto-data-pipeline-462115.crypto_data.crypto_prices`
WHERE timestamp >= TIMESTAMP("{time_cutoff.strftime('%Y-%m-%d %H:%M:%S')}")
ORDER BY timestamp DESC
"""

df = client.query(query).to_dataframe()
df['timestamp'] = pd.to_datetime(df['timestamp'])

if 'last_updated_at' in df.columns:
    df['last_updated_at'] = pd.to_datetime(df['last_updated_at'])

# Sidebar coin filter
coins = df["symbol"].unique().tolist()
selected_coins = st.sidebar.multiselect("Select coins", coins, default=coins)
df_filtered = df[df["symbol"].isin(selected_coins)]

# Z-score normalization (per coin)
df_filtered["z_score_price"] = df_filtered.groupby("symbol")["price_usd"].transform(
    lambda x: (x - x.mean()) / x.std() if x.std() != 0 else 0
)

#  Min-Max normalization (per coin)
df_filtered["minmax_price"] = df_filtered.groupby("symbol")["price_usd"].transform(
    lambda x: (x - x.min()) / (x.max() - x.min()) if (x.max() - x.min()) != 0 else 0
)

#  % Change Per Minute (Momentum)
df_filtered["pct_change_1m"] = df_filtered.groupby("symbol")["price_usd"].pct_change() * 100

#  Rolling Averages per coin (5-point window)
df_filtered["sma_5"] = df_filtered.groupby("symbol")["price_usd"].transform(lambda x: x.rolling(window=5).mean())
df_filtered["ema_5"] = df_filtered.groupby("symbol")["price_usd"].transform(lambda x: x.ewm(span=5, adjust=False).mean())

#  Latest Prices Table
st.subheader(" Latest Prices")
st.dataframe(df_filtered.sort_values("timestamp", ascending=False).reset_index(drop=True))

#  Price/Volume Ratio (per coin entry)
df_filtered["price_volume_ratio"] = df_filtered["price_usd"] / df_filtered["vol_24h_usd"]


#  KPIs
st.subheader(" KPIs (Latest Entry Per Coin)")

if selected_coins:
    latest = df_filtered.sort_values("timestamp").groupby("symbol").tail(1)
    kpi_cols = st.columns(len(selected_coins))
    for i, row in enumerate(latest.itertuples()):
        with kpi_cols[i]:
            price = f"${row.price_usd:,.2f}"
            delta = f"{row.change_24h_pct:.2f}%" if hasattr(row, 'change_24h_pct') else "N/A"
            st.metric(label=row.symbol, value=price, delta=delta)
else:
    st.info("Please select at least one coin from the sidebar to show KPIs.")
    latest = pd.DataFrame()

# Sidebar Candlestick Controls
st.sidebar.subheader(" Candlestick Charts")
show_candles = st.sidebar.checkbox("Show candlestick charts for selected coins")
interval = st.sidebar.selectbox("Resample Interval (min)", [1, 5, 15, 30, 60], index=0)

if show_candles and selected_coins:
    st.subheader(f"🕯️ Candlestick Charts ({interval}-min intervals)")

    for i in range(0, len(selected_coins), 2):
        cols = st.columns(2)
        for j in range(2):
            if i + j >= len(selected_coins):
                break

            symbol = selected_coins[i + j]
            coin_df = df_filtered[df_filtered["symbol"] == symbol].copy()
            coin_df.set_index("timestamp", inplace=True)
            ohlc = coin_df["price_usd"].resample(f"{interval}min").ohlc().dropna().reset_index()

            if ohlc.empty:
                cols[j].warning(f"No OHLC data for {symbol}.")
                continue

            fig = go.Figure(data=[go.Candlestick(
                x=ohlc['timestamp'],
                open=ohlc['open'],
                high=ohlc['high'],
                low=ohlc['low'],
                close=ohlc['close'],
                increasing_line_color='green',
                decreasing_line_color='red'
            )])

            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Price (USD)",
                xaxis_rangeslider_visible=False,
                height=500,
                margin=dict(l=0, r=0, t=0, b=0)  # No title margin
            )

            with cols[j]:
                with st.container():
                    # Start box with heading
                    st.markdown(
                        f"""
                        <div style='
                            border: 1px solid #cccccc;
                            border-radius: 10px;
                            padding: 10px 10px 0px 10px;
                            margin-bottom: 20px;
                            background-color: #fdfdfd;
                        '>
                        <h5 style='margin-top: 0; margin-bottom: 15px;'>{symbol} Candlestick Chart</h5>
                        """,
                        unsafe_allow_html=True
                    )

                    # Plot the chart
                    st.plotly_chart(fig, use_container_width=True)

                    # Close box div
                    st.markdown("</div>", unsafe_allow_html=True)





#  Line Charts with Tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📉 Raw Price Trends", 
    "📈 Min-Max Normalized Trends", 
    "📊 SMA/EMA Overlay", 
    "💹 Price/Volume Ratio", 
    "🏆 Market Dominance Share"
])

with tab1:
    st.subheader(f" Price Over Time ({time_range})")
    for symbol in selected_coins:
        coin_df = df_filtered[df_filtered["symbol"] == symbol]
        if coin_df.shape[0] > 1:
            chart = alt.Chart(coin_df).mark_line().encode(
                x=alt.X("timestamp:T", title="Time", axis=alt.Axis(format="%H:%M", labelAngle=-45)),
                y=alt.Y("price_usd:Q", title="Price (USD)", scale=alt.Scale(zero=False)),
                tooltip=["timestamp:T", "price_usd:Q"]
            ).properties(
                title=f"{symbol} Price Trend",
                width=700,
                height=300
            )
            st.altair_chart(chart)
        else:
            st.warning(f"Not enough data points to show a line for {symbol}.")

with tab2:
    st.subheader("📈 Min-Max Normalized Price Over Time")
    for symbol in selected_coins:
        coin_df = df_filtered[df_filtered["symbol"] == symbol]
        if coin_df.shape[0] > 1:
            chart = alt.Chart(coin_df).mark_line().encode(
                x=alt.X("timestamp:T", title="Time"),
                y=alt.Y("minmax_price:Q", title="Normalized Price (0–1)"),
                tooltip=["timestamp:T", "minmax_price:Q", "price_usd:Q"]
            ).properties(
                title=f"{symbol} (Min-Max Normalized)",
                width=700,
                height=300
            )
            st.altair_chart(chart)
        else:
            st.warning(f"Not enough data points for normalized view: {symbol}.")

with tab3:
    st.subheader("📊 Price with SMA / EMA Overlay")
    for symbol in selected_coins:
        coin_df = df_filtered[df_filtered["symbol"] == symbol].dropna()
        if coin_df.shape[0] > 5:
            base = alt.Chart(coin_df).encode(x="timestamp:T")

            price_line = base.mark_line(color="blue").encode(y="price_usd:Q", tooltip=["timestamp:T", "price_usd:Q"])
            sma_line = base.mark_line(color="orange").encode(y="sma_5:Q")
            ema_line = base.mark_line(color="green").encode(y="ema_5:Q")

            chart = alt.layer(price_line, sma_line, ema_line).properties(
                title=f"{symbol} Price + SMA/EMA",
                width=700,
                height=300
            )

            st.altair_chart(chart)
        else:
            st.warning(f"Not enough data points for SMA/EMA view: {symbol}.")

with tab4:
    st.subheader("💹 Price/Volume Ratio (Latest per Coin)")

    latest_ratios = df_filtered.sort_values("timestamp").groupby("symbol").tail(1).copy()
    latest_ratios["price_volume_ratio"] = latest_ratios["price_usd"] / latest_ratios["vol_24h_usd"]

    if latest_ratios.empty:
        st.info("No data available for Price/Volume Ratio.")
    else:
        bar_chart = alt.Chart(latest_ratios).mark_bar().encode(
            x=alt.X("symbol:N", title="Coin"),
            y=alt.Y("price_volume_ratio:Q", title="Price / Volume"),
            tooltip=["symbol", "price_usd", "vol_24h_usd", "price_volume_ratio"]
        ).properties(
            title="Price/Volume Ratio Comparison",
            width=700,
            height=400
        )
        st.altair_chart(bar_chart)
with tab5:
    st.subheader("🏆 Market Dominance by Market Cap")

    dominance_df = df_filtered.sort_values("timestamp").groupby("symbol").tail(1).copy()
    dominance_df = dominance_df[dominance_df["market_cap_usd"] > 0]
    dominance_df["dominance_pct"] = dominance_df["market_cap_usd"] / dominance_df["market_cap_usd"].sum() * 100

    if dominance_df.empty:
        st.info("No market cap data available to compute dominance.")
    else:
        pie = alt.Chart(dominance_df).mark_arc().encode(
            theta=alt.Theta(field="dominance_pct", type="quantitative"),
            color=alt.Color(field="symbol", type="nominal"),
            tooltip=["symbol", alt.Tooltip("market_cap_usd:Q", format=",.2f"), alt.Tooltip("dominance_pct:Q", format=".2f")]
        ).properties(
            title="Market Dominance by Market Cap (Selected Coins)",
            width=600,
            height=400
        )
        st.altair_chart(pie)


# 💰 Market Cap & 24h Volume
st.subheader("💰 Market Cap & 24h Volume")
if not latest.empty and {'market_cap_usd', 'vol_24h_usd'}.issubset(latest.columns):
    st.bar_chart(latest.set_index("symbol")[["market_cap_usd", "vol_24h_usd"]])
else:
    st.info("Market cap or volume data not available.")
