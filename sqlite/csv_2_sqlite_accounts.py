import sqlite3
import pandas as pd

# Define database and table name
db_name = "mydatabase.db"
table_name = "account_details"
csv_file = "/Users/manishtaneja/Downloads/combined_account_details.csv"  # Replace with the actual CSV file path
df = pd.read_csv(csv_file)

# Connect to SQLite database (it creates the database if it doesn't exist)
conn = sqlite3.connect(db_name)

# Save DataFrame to SQLite (replace if table exists)
df.to_sql(table_name, conn, if_exists="append", index=False)

# Close the connection
conn.commit()
conn.close()

print(f"Data from {csv_file} successfully imported into {db_name} in table {table_name}.")
