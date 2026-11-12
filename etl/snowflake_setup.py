import os

# ---- Snowflake login ----
os.environ["SNOWFLAKE_ACCOUNT"]   = "IJRWYJW-LT11479"
os.environ["SNOWFLAKE_USER"]      = "GOPI8MANOHAR"
os.environ["SNOWFLAKE_PASSWORD"]  = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
os.environ["SNOWFLAKE_ROLE"]      = "ACCOUNTADMIN"
os.environ["SNOWFLAKE_WAREHOUSE"] = "FRESHCART_WH"
os.environ["SNOWFLAKE_DATABASE"]  = "FRESHCART"

# ---- Pipeline settings ----
os.environ["LANDING_DIR"]         = "/freshcart-pipelines/landing"
os.environ["QUARANTINE_MAX_RATE"] = "0.05"

# Make sure no old key-pair setting is left over from an earlier run
os.environ.pop("SNOWFLAKE_PRIVATE_KEY_FILE", None)
os.environ.pop("SNOWFLAKE_PRIVATE_KEY_FILE_PWD", None)
os.environ.pop("SNOWFLAKE_AUTHENTICATOR", None)

# ---- Check nothing is missing ----
required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
missing = [k for k in required if not os.getenv(k)]
print("Missing:", missing) if missing else print("Login settings loaded for", os.environ["SNOWFLAKE_USER"])