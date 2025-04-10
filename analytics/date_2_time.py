import sqlite3

# Connect to SQLite DB
db_path = 'mydatabase.db'  # <-- update this to your actual DB file
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get table and column info from user
table_name = input("Enter the table name: ")
column_name = input("Enter the column name to upgrade from DATE to DATETIME: ")

try:
    # Step 1: Rename the old column
    temp_column = f"{column_name}_old"
    cursor.execute(f"ALTER TABLE {table_name} RENAME COLUMN {column_name} TO {temp_column};")

    # Step 2: Add new DATETIME column
    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} DATETIME;")

    # Step 3: Update new column with datetime values (add time to existing dates)
    cursor.execute(f"""
        UPDATE {table_name}
        SET {column_name} = datetime({temp_column} || ' 00:00:00');
    """)

    # Step 4: (Optional) Drop the old column - SQLite doesn’t support DROP COLUMN directly
    # So we need to recreate the table if we want to truly drop it
    print(f"\n✅ Column '{column_name}' upgraded to DATETIME with time '00:00:00'.")
    print(f"ℹ️ Note: '{temp_column}' still exists. SQLite doesn't support DROP COLUMN directly.")
    
    conn.commit()
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    conn.close()

