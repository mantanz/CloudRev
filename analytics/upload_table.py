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
    # Read the CSV with all columns as strings initially
    df = pd.read_csv(csv_file, dtype=str)
    
    # Convert month column to datetime if it exists
    if "month" in df.columns:
        # # First try to parse as datetime
        # try:
        #     df["month"] = pd.to_datetime(df["month"])
        #     df["month"] = df["month"].dt.strftime("%Y-%m-%d")
        # except:
        #     # If parsing fails, keep the original format
        #     pass
        df["month"] = pd.to_datetime(df["month"], format="%B-%Y")
        df["month"] = df["month"].dt.strftime("%Y-%m-%d")
        
    # Convert spend to float and calculate sum
    if "spend" in df.columns:
        df["spend"] = df["spend"].str.replace(",", "").astype(float)
        total_spend = df["spend"].sum()
        print(f"\n💰 Total Spend: ${total_spend:,.2f}")
            
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit()

# === CONNECT TO SQLITE DB ===
conn = sqlite3.connect(db_name)

# === SAVE TO SQLITE ===
df.to_sql(table_name, conn, if_exists="append", index=False)

# === CLOSE CONNECTION ===
conn.commit()
conn.close()

print(f"✅ Data from '{csv_file}' successfully imported into '{db_name}' in table '{table_name}'.")
