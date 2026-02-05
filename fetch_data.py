import yfinance as yf
import pandas as pd
import requests
from datetime import datetime
import ta

BOT_TOKEN = "PASTE_YOUR_TOKEN"
CHAT_ID = "PASTE_YOUR_CHAT_ID"

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": msg})

# -------- LOAD SYMBOLS --------
with open("symbols.txt") as f:
    symbols = [s.strip() for s in f.readlines() if s.strip()]

watchlist = []

for sym in symbols:
    try:
        df = yf.download(sym + ".NS", period="120d", interval="1d", progress=False)
        df.dropna(inplace=True)

        # Tight + Thin
        last6 = df.tail(6)
        tight = (last6["Close"].max() - last6["Close"].min()) / last6["Close"].min() <= 0.05
        thin = last6["Volume"].iloc[-1] < last6["Volume"].mean()

        if not (tight and thin):
            continue

        # Indicators
        df["RSI"] = ta.momentum.RSIIndicator(df["Close"]).rsi()
        df["ADX"] = ta.trend.ADXIndicator(df["High"], df["Low"], df["Close"]).adx()
        df["SMA50"] = df["Close"].rolling(50).mean()

        rsi = df["RSI"].iloc[-1]
        adx = df["ADX"].iloc[-1]
        sma = df["SMA50"].iloc[-1]
        close = df["Close"].iloc[-1]

        # Volume score
        vol_score = min((last6["Volume"].iloc[-1] / last6["Volume"].mean()) * 25, 25)

        # Scores
        rsi_score = 100 - abs(rsi - 50)
        sma_score = 80 if close > sma else 30

        score = (vol_score * 0.35) + (adx * 0.25) + \
                (rsi_score * 0.15) + (sma_score * 0.15)

        watchlist.append((sym, round(score,2)))

    except:
        continue

# -------- RANK --------
top = sorted(watchlist, key=lambda x: x[1], reverse=True)[:5]

# -------- MESSAGE --------
msg = "🎯 TOP SETUPS TODAY\n\n"
for i,(s,sc) in enumerate(top,1):
    msg += f"{i}. {s} — Score: {sc}\n"

msg += f"\nTime: {datetime.now()}"

send_telegram(msg)
