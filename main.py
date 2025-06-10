import requests
import pandas as pd
import numpy as np
from datetime import datetime
from google.cloud import bigquery

def get_crypto_prices():
    coins = [
        "bitcoin", "ethereum", "tether", "binancecoin", "solana",
        "usd-coin", "ripple", "toncoin", "dogecoin", "cardano",
        "avalanche-2", "shiba-inu", "wrapped-bitcoin", "polkadot",
        "tron", "chainlink", "uniswap", "litecoin", "polygon", "internet-computer"
    ]
    url = "https://api.coingecko.com/api/v3/simple/price"
    response = requests.get(url, params={
        "ids": ",".join(coins),
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_last_updated_at": "true"
    })

    data = response.json()
    print("⚙️ Coins received:", list(data.keys()), flush=True)
    return data

def insert_prices_to_bigquery(data):
    client = bigquery.Client()
    table_id = "crypto-data-pipeline-462115.crypto_data.crypto_prices"
    rows_to_insert = []
    timestamp = datetime.utcnow().isoformat()

    for coin, info in data.items():
        try:
            last_updated = datetime.utcfromtimestamp(info["last_updated_at"]).isoformat()
            rows_to_insert.append({
                "timestamp": timestamp,
                "symbol": coin.upper(),
                "price_usd": float(info["usd"]),
                "market_cap_usd": float(info.get("usd_market_cap", 0)),
                "vol_24h_usd": float(info.get("usd_24h_vol", 0)),
                "change_24h_pct": float(info.get("usd_24h_change", 0)),
                "last_updated_at": last_updated,
                "volume": float(np.random.uniform(1000, 10000))
            })
        except Exception as e:
            print(f"❌ Error formatting coin {coin}: {e}", flush=True)

    print(f"🧾 Prepared {len(rows_to_insert)} rows @ {timestamp}", flush=True)
    if rows_to_insert:
        print("🧪 First row:", rows_to_insert[0], flush=True)

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        print("✅ Price data inserted.", flush=True)
    else:
        print("❌ Insert errors:", errors, flush=True)

    return pd.DataFrame(rows_to_insert)

def resample_ohlc(df, freq="1H"):
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index("timestamp", inplace=True)

    ohlc_data = []

    for symbol in df["symbol"].unique():
        coin_df = df[df["symbol"] == symbol]
        ohlc = coin_df["price_usd"].resample(freq).ohlc().dropna()
        ohlc["symbol"] = symbol
        ohlc["timestamp"] = ohlc.index
        ohlc_data.append(ohlc)

    return pd.concat(ohlc_data).reset_index(drop=True)

def insert_ohlc_to_bigquery(df, table):
    client = bigquery.Client()
    table_id = f"crypto-data-pipeline-462115.{table}"
    records = df.to_dict(orient="records")
    errors = client.insert_rows_json(table_id, records)
    if not errors:
        print(f"✅ OHLC data inserted into {table}", flush=True)
    else:
        print(f"❌ OHLC insert errors in {table}:", errors, flush=True)

if __name__ == "__main__":
    raw_data = get_crypto_prices()
    if raw_data:
        raw_df = insert_prices_to_bigquery(raw_data)
        if not raw_df.empty:
            ohlc_hourly_df = resample_ohlc(raw_df, freq="1H")
            insert_ohlc_to_bigquery(ohlc_hourly_df, table="crypto_data.crypto_ohlc_hourly")
    else:
        print("⚠️ No data returned from Coingecko!", flush=True)
