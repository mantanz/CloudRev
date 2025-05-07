import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime

def get_db_connection():
    """Create a database connection"""
    try:
        # Get the absolute path to the database file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        db_path = os.path.join(project_root, 'final', 'sqlite', 'mydatabase.db')
        
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        return None

def update_aop_budget_monthly(transformed_file):
    """Update aop_budget_monthly table with transformed AOP data"""
    try:
        # Read the transformed file
        df = pd.read_csv(transformed_file)
        
        # Convert month to datetime
        df['month'] = pd.to_datetime(df['month'])
        
        # Connect to database
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Track statistics
        updated_records = 0
        inserted_records = 0
        
        # Process each record
        for _, row in df.iterrows():
            account_id = row['account_id']
            month = row['month'].strftime('%Y-%m-%d')
            aop_amount = float(row['aop_amount'])  # Convert to float for comparison
            
            # Check if record exists
            cursor.execute("""
                SELECT aop_amount FROM aop_budget_monthly 
                WHERE account_id = ? AND month = ?
            """, (account_id, month))
            
            result = cursor.fetchone()
            
            if result:
                # Update existing record if amount is different
                existing_amount = float(result[0])  # Convert to float for comparison
                if not np.isclose(existing_amount, aop_amount, rtol=1e-5):
                    cursor.execute("""
                        UPDATE aop_budget_monthly 
                        SET aop_amount = ? 
                        WHERE account_id = ? AND month = ?
                    """, (aop_amount, account_id, month))
                    updated_records += 1
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO aop_budget_monthly (account_id, month, aop_amount)
                    VALUES (?, ?, ?)
                """, (account_id, month, aop_amount))
                inserted_records += 1
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        print(f"\nAOP Budget Monthly table update completed:")
        print(f"- {updated_records} records updated")
        print(f"- {inserted_records} records inserted")
        
        return True
        
    except Exception as e:
        print(f"Error updating AOP budget monthly table: {str(e)}")
        if 'conn' in locals():
            conn.close()
        return False 