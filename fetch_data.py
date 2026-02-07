import yfinance as yf
import pandas as pd
import time

# --- CONFIGURATION: NIFTY 100 UNIVERSE ---
# We scan the Top 100 Liquid Stocks to ensure we get signals daily.
SYMBOLS = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS", "HINDUNILVR.NS", 
    "ITC.NS", "SBIN.NS", "BHARTIARTL.NS", "KOTAKBANK.NS", "LICI.NS", "LT.NS", 
    "AXISBANK.NS", "HCLTECH.NS", "ASIANPAINT.NS", "MARUTI.NS", "SUNPHARMA.NS", 
    "TITAN.NS", "BAJFINANCE.NS", "ULTRACEMCO.NS", "TATASTEEL.NS", "NTPC.NS", 
    "POWERGRID.NS", "M&M.NS", "JSWSTEEL.NS", "ADANIENT.NS", "ADANIGREEN.NS", 
    "ADANIPORTS.NS", "COALINDIA.NS", "ONGC.NS", "GRASIM.NS", "HINDALCO.NS", 
    "BAJAJFINSV.NS", "TECHM.NS", "WIPRO.NS", "NESTLEIND.NS", "BRITANNIA.NS", 
    "TATACHEM.NS", "TATAMOTORS.NS", "DIVISLAB.NS", "CIPLA.NS", "DRREDDY.NS", 
    "EICHERMOT.NS", "HEROMOTOCO.NS", "INDUSINDBK.NS", "BPCL.NS", "APOLLOHOSP.NS", 
    "TATACONSUM.NS", "UPL.NS", "SBILIFE.NS", "HDFCLIFE.NS", "BAJAJ-AUTO.NS",
    "LTIM.NS", "PIDILITIND.NS", "SIEMENS.NS", "IOC.NS", "VEDL.NS", "SHREECEM.NS",
    "BEL.NS", "TRENT.NS", "AMBUJACEM.NS", "INDIGO.NS", "TVSMOTOR.NS", "HAL.NS",
    "ZOMATO.NS", "VBL.NS", "CHOLAFIN.NS", "HAVELLS.NS", "DABUR.NS", "ABB.NS",
    "GODREJCP.NS", "GAIL.NS", "BOSCHLTD.NS", "DLF.NS", "SRF.NS", "CANBK.NS",
    "BANKBARODA.NS", "JINDALSTEL.NS", "VARROC.NS", "MOTHERSON.NS", "ICICIPRULI.NS",
    "MARICO.NS", "BERGEPAINT.NS", "NAUKRI.NS", "ICICIGI.NS", "SBICARD.NS",
    "PNB.NS", "RECLTD.NS", "PFC.NS", "MUTHOOTFIN.NS", "PIIND.NS", "PAGEIND.NS"
]

def harvest_data():
    print(f"🚀 TARA HARVEST: Scanning {len(SYMBOLS)} Stocks (Nifty 100)...")
    all_data = []
    
    for symbol in SYMBOLS:
        try:
            # Fetch data
            ticker = yf.Ticker(symbol)
            # period="1y" ensures we get enough history for rolling calculations
            df = ticker.history(period="1y")
            
            if df.empty: continue
            
            # Format Data
            df = df.reset_index()
            df = df.rename(columns={
                "Date": "TIMESTAMP", "Open": "OPEN", "High": "HIGH", 
                "Low": "LOW", "Close": "CLOSE", "Volume": "TOTTRDQTY"
            })
            
            df['SYMBOL'] = symbol.replace(".NS", "")
            
            # Keep only essential columns to save space
            cols = ['SYMBOL', 'TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']
            all_data.append(df[cols])
            print(f"   ✅ Fetched: {symbol}")
            
        except Exception as e:
            print(f"   ❌ Error {symbol}: {e}")
            
    if all_data:
        final_df = pd.concat(all_data)
        final_df.to_csv("smart_db.csv", index=False)
        print(f"🎉 SUCCESS: Database Updated with {len(final_df)} rows.")
    else:
        print("❌ CRITICAL: No data fetched.")
        exit(1)

if __name__ == "__main__":
    harvest_data()
