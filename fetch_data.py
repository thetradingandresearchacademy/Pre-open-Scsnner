import yfinance as yf
import pandas as pd
import requests
import io
import time

def get_nifty500_symbols():
    """
    Attempts to fetch the official Nifty 500 list. 
    Falls back to Top 50 if NSE website blocks the request.
    """
    print("⏳ Connecting to NSE for Nifty 500 List...")
    try:
        url = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
        headers = {'User-Agent': 'Mozilla/5.0'}
        s = requests.get(url, headers=headers, timeout=10).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        symbols = [x + ".NS" for x in df['Symbol'].tolist()]
        print(f"✅ SUCCESS: Found {len(symbols)} stocks in Nifty 500.")
        return symbols
    except Exception as e:
        print(f"⚠️ NSE Connection Failed ({e}). Using Emergency Backup (Top 50).")
        return [
            "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "ICICIBANK.NS", "INFY.NS", 
            "SBIN.NS", "BHARTIARTL.NS", "ITC.NS", "KOTAKBANK.NS", "LICI.NS", 
            "LT.NS", "AXISBANK.NS", "HCLTECH.NS", "ASIANPAINT.NS", "MARUTI.NS",
            "SUNPHARMA.NS", "TITAN.NS", "BAJFINANCE.NS", "ULTRACEMCO.NS", "TATASTEEL.NS",
            "NTPC.NS", "M&M.NS", "JSWSTEEL.NS", "ADANIENT.NS", "POWERGRID.NS",
            "ADANIPORTS.NS", "COALINDIA.NS", "ONGC.NS", "GRASIM.NS", "HINDALCO.NS",
            "BAJAJFINSV.NS", "TECHM.NS", "WIPRO.NS", "NESTLEIND.NS", "BRITANNIA.NS",
            "TATACHEM.NS", "TATAMOTORS.NS", "DIVISLAB.NS", "CIPLA.NS", "DRREDDY.NS",
            "EICHERMOT.NS", "HEROMOTOCO.NS", "INDUSINDBK.NS", "BPCL.NS", "APOLLOHOSP.NS",
            "TATACONSUM.NS", "UPL.NS", "SBILIFE.NS", "HDFCLIFE.NS", "BAJAJ-AUTO.NS"
        ]

def harvest_data():
    symbols = get_nifty500_symbols()
    
    print(f"🚀 STARTING HEAVY HARVEST: {len(symbols)} Stocks...")
    all_data = []
    
    # Batch processing to prevent memory overload
    batch_size = 50
    total_batches = len(symbols) // batch_size + 1
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        print(f"   ...Processing Batch {i//batch_size + 1}/{total_batches} ({len(batch)} stocks)")
        
        try:
            # Download batch
            tickers = " ".join(batch)
            data = yf.download(tickers, period="1y", group_by='ticker', progress=False, threads=True)
            
            # Process batch
            for symbol in batch:
                try:
                    if len(batch) == 1:
                         df = data.copy() # Handle single stock case
                    else:
                         if symbol not in data: continue
                         df = data[symbol].copy()
                    
                    if df.empty: continue
                    
                    df = df.reset_index()
                    df['SYMBOL'] = symbol.replace(".NS", "")
                    
                    # Clean Columns (Handle MultiIndex)
                    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
                    
                    # Rename
                    df = df.rename(columns={
                        "Date": "TIMESTAMP", "Open": "OPEN", "High": "HIGH", 
                        "Low": "LOW", "Close": "CLOSE", "Volume": "TOTTRDQTY"
                    })
                    
                    # Keep necessary columns
                    cols = ['SYMBOL', 'TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']
                    # Ensure columns exist before appending
                    if all(col in df.columns for col in cols):
                         all_data.append(df[cols])
                    
                except Exception:
                    continue
                    
        except Exception as e:
            print(f"   ⚠️ Batch Error: {e}")
            
    if all_data:
        final_df = pd.concat(all_data)
        # Ensure numeric types
        for c in ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']:
            final_df[c] = pd.to_numeric(final_df[c], errors='coerce')
            
        final_df.to_csv("smart_db.csv", index=False)
        print(f"🎉 SUCCESS: Harvested {len(final_df)} rows from {len(symbols)} stocks.")
    else:
        print("❌ FAILURE: No data collected.")
        exit(1)

if __name__ == "__main__":
    harvest_data()
