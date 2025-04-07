import sqlite3
import pandas as pd

# Define database and table name
db_name = "mydatabase.db"
table_name = "actual_spend"
csv_file = "/Users/manishtaneja/Downloads/actual_spend.csv"  # Replace with the actual CSV file path
# Load CSV into Pandas DataFrame
df = pd.read_csv(csv_file, parse_dates=["month"])  # Ensure 'month' is recognized as a date column

# Format 'month' column to only contain date (YYYY-MM-DD)
df["month"] = df["month"].dt.strftime("%Y-%m-%d")

# Connect to SQLite database (it creates the database if it doesn't exist)
conn = sqlite3.connect(db_name)

# Save DataFrame to SQLite (replace if table exists)
df.to_sql(table_name, conn, if_exists="append", index=False)

# Close the connection
conn.commit()
conn.close()

print(f"Data from {csv_file} successfully imported into {db_name} in table {table_name}.")
