import sqlite3
import pandas as pd

# Connect to your SQLite database (change this to your DB file)
db_path = 'mydatabase.db'  # <-- update this
conn = sqlite3.connect(db_path)

# Ask user for table name
table_name = input("Enter the table name to export: ")

try:
    # Read table into a DataFrame
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    
    # Save to CSV
    csv_filename = f"{table_name}.csv"
    df.to_csv(csv_filename, index=False)
    print(f"✅ Data from table '{table_name}' saved to '{csv_filename}'")
except Exception as e:
    print(f"❌ Error: {e}")

# Close the connection
conn.close()

