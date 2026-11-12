
import sys, os, importlib
import snowflake_setup
# # Make the project folder importable, and reload the module in case an older copy is cached
# sys.path.insert(0, "/freshcart-pipelines")
# if not os.path.exists("/freshcart-pipelines/snowflake_io.py"):
#     raise SystemExit("snowflake_io.py not found: run the step 2 cells (the %%writefile ones) first.")
import snowflake_io as sf
importlib.reload(sf)

print("Connecting to", os.environ.get("SNOWFLAKE_ACCOUNT"), "as", os.environ.get("SNOWFLAKE_USER"), "...")

try:
    conn = sf.connect(bootstrap=True)
    account, user, role, version = sf.query_one(
        conn, "SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_VERSION()")
    print(f" Connected!  account={account}  user={user}  role={role}  version={version}")
    conn.close()

except KeyError as e:
    print(f"Setting {e} is missing: run the login cell (step 3) first.")

except Exception as e:
    msg = str(e)
    print(" Connection failed:\n ", msg[:500])
    low = msg.lower()
    if "incorrect username or password" in low:
        print("→ Check the user name and password in step 3.")
    elif "mfa" in low or "multi-factor" in low:
        print("→ Your account needs MFA. In Snowflake: your name → Settings → Authentication →")
        print("  Programmatic access tokens → Generate. Put the token in SNOWFLAKE_PASSWORD in step 3.")
    elif "404" in low or "could not connect" in low or "account" in low or "certificate" in low:
        print("→ Account identifier is probably wrong. In Snowflake: your name (bottom left) →")
        print("  your account → 'Copy account identifier' (looks like ABCDEFG-XY12345),")
        print("  put it in SNOWFLAKE_ACCOUNT in step 3, re-run step 3, then this cell.")