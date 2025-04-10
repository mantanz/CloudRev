import sqlite3
import pandas as pd
import os

# === CONFIGURABLE INPUTS ===
csv_file = input("Enter full path to your CSV file: ").strip()
table_name = input("Enter the SQLite table name to store data: ").strip()
db_name = "../sqlite/mydatabase.db"  # Change if you want to use another DB file

# === CHECK FILE EXISTS ===
if not os.path.isfile(csv_file):
    print(f"❌ File not found: {csv_file}")
    exit()

# === LOAD CSV INTO DATAFRAME ===
try:
    df = pd.read_csv(csv_file, parse_dates=["month"])  # Automatically parse 'month' if exists
    if "month" in df.columns:
        df["month"] = df["month"].dt.strftime("%Y-%m-%d")
except ValueError:
    df = pd.read_csv(csv_file)  # Fallback if 'month' column doesn't exist or parse fails

# === CONNECT TO SQLITE DB ===
conn = sqlite3.connect(db_name)

# === SAVE TO SQLITE ===
df.to_sql(table_name, conn, if_exists="append", index=False)

# === CLOSE CONNECTION ===
conn.commit()
conn.close()

print(f"✅ Data from '{csv_file}' successfully imported into '{db_name}' in table '{table_name}'.")

