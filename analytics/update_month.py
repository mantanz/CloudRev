import sqlite3
import pandas as pd

def normalize_date_column_interactive(db_path):
    # Get input from user
    table_name = input("Enter the table name: ").strip()
    date_column = input("Enter the date column name to normalize: ").strip()

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)

    try:
        # Read table
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

        if date_column not in df.columns:
            raise ValueError(f"Column '{date_column}' not found in table '{table_name}'.")

        # Normalize the date column
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce').dt.strftime('%Y-%m-%d')

        # Drop rows with invalid dates (optional)
        df = df.dropna(subset=[date_column])

        # Save updated table back
        df.to_sql(table_name, conn, if_exists="replace", index=False)

        print(f"\n✅ Successfully updated '{table_name}'. '{date_column}' is now in 'YYYY-MM-DD' format.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        conn.close()

# Example usage
if __name__ == "__main__":
    db_path = input("Enter the path to your SQLite DB file (e.g., 'mydata.db'): ").strip()
    normalize_date_column_interactive(db_path)

