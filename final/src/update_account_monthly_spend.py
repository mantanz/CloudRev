import os
import pandas as pd
from transform_spend import transform_monthly_spend
from update_spend import update_monthly_spend
from update_account_details import validate_and_update_single_account
from validate_spend import validate_pre_transpose, validate_post_transpose
import sqlite3
from datetime import datetime
import numpy as np

def update_account_monthly_spend(file_path, entity):
    """Update account monthly spend data."""
    try:
        # Read input file with header=None to treat first row as data
        df = pd.read_csv(file_path, header=None)
        
        # Pre-transpose validation
        is_valid, message, pre_totals = validate_pre_transpose(df, 3)  # 3 for monthly spend
        if not is_valid:
            print(f"Pre-transpose validation failed: {message}")
            return False
            
        # Transform data
        df_transformed = transform_monthly_spend(df)
        if df_transformed is None:
            print(f"Failed to transform file: {file_path}")
            return False
            
        # Post-transpose validation
        if not validate_post_transpose(df_transformed, 3, pre_totals):
            print(f"Post-transpose validation failed")
            return False
            
        # Save transformed data
        output_path = os.path.join(os.path.dirname(file_path), 
                                 f"transformed_{os.path.basename(file_path)}")
        df_transformed.to_csv(output_path, index=False)
        print(f"Transformed data saved to: {output_path}")
            
        # First update account details for all accounts
        account_update_success = True
        for _, row in df_transformed.iterrows():
            if not validate_and_update_single_account(row['account_id'], row['account_name'], entity):
                account_update_success = False
                print(f"\nFailed to update account details for account: {row['account_id']}")
                break
                
        if not account_update_success:
            print(f"\nFailed to update account details for file: {file_path}")
            return False
            
        # Get absolute path to database
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        db_path = os.path.join(project_root, 'final', 'sqlite', 'mydatabase.db')
            
        # Then update spend tables
        if update_monthly_spend(df_transformed, db_path):
            print(f"\nSuccessfully processed file: {file_path}")
            return True
        else:
            print(f"\nFailed to update spend data for file: {file_path}")
            return False
            
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return False

def update_account_monthly_spend(transformed_file):
    """Update as_acct_monthly table with transformed spend data.
    
    Args:
        transformed_file (str): Path to the transformed CSV file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Read the transformed file
        df = pd.read_csv(transformed_file)
        
        # Convert month to datetime
        df['month'] = pd.to_datetime(df['month'])
        
        # Connect to database
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sqlite', 'mydatabase.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Track statistics
        updated_records = 0
        inserted_records = 0
        
        # Process each record
        for _, row in df.iterrows():
            account_id = row['account_id']
            month = row['month'].strftime('%Y-%m-%d')
            spend = float(row['spend'])
            
            # Check if record exists
            cursor.execute("""
                SELECT spend FROM as_acct_monthly 
                WHERE account_id = ? AND month = ?
            """, (account_id, month))
            
            result = cursor.fetchone()
            
            if result:
                # Update existing record if spend is different
                existing_spend = float(result[0])
                if not np.isclose(existing_spend, spend, rtol=1e-5):
                    cursor.execute("""
                        UPDATE as_acct_monthly 
                        SET spend = ? 
                        WHERE account_id = ? AND month = ?
                    """, (spend, account_id, month))
                    updated_records += 1
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO as_acct_monthly (account_id, month, spend)
                    VALUES (?, ?, ?)
                """, (account_id, month, spend))
                inserted_records += 1
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        print(f"\nDatabase update completed:")
        print(f"- {updated_records} records updated")
        print(f"- {inserted_records} new records inserted")
        
        return True
        
    except Exception as e:
        print(f"Error updating monthly spend: {str(e)}")
        if 'conn' in locals():
            conn.close()
        return False

if __name__ == "__main__":
    # This file is meant to be imported and used by the main program
    print("This module should be imported and used by the main program.") 