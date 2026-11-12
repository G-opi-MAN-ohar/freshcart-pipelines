import pandas as pd
import sys, os, importlib
import snowflake_setup
import snowflake_io as sf

importlib.reload(sf)
conn = sf.connect()

def show(sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=[c[0] for c in cur.description])

# Optional: widen pandas output so JSON columns in 'raw' aren't truncated in PowerShell
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)

df = show("SELECT ingest_seq, source, raw FROM bronze.orders_raw ORDER BY ingest_seq")
print(df)

conn.close()