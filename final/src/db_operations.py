import sqlite3
import pandas as pd
import os
from datetime import datetime

def get_db_connection():
    """Create a database connection"""
    try:
        # Get the absolute path to the database file
        current_dir = os.path.dirname(os.path.abspath(__file__))  # Get current directory
        project_root = os.path.dirname(os.path.dirname(current_dir))  # Go up two levels to CloudRev
        db_path = os.path.join(project_root, 'final', 'sqlite', 'mydatabase.db')
        
        # Ensure the sqlite directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        return None

def validate_and_update_accounts(transformed_file):
    """Validate accounts against database and update if needed"""
    try:
        # Read the transformed file
        df = pd.read_csv(transformed_file)
        
        # Get unique accounts
        accounts = df[['account_id', 'account_name']].drop_duplicates()
        
        # Connect to database
        conn = get_db_connection()
        if not conn:
            return
        
        cursor = conn.cursor()
        
        # Create missing accounts file
        missing_accounts = []
        
        # Check each account
        for _, row in accounts.iterrows():
            account_id = row['account_id']
            account_name = row['account_name']
            
            # Check if account exists
            cursor.execute("SELECT account_id FROM account_details WHERE account_id = ?", (account_id,))
            result = cursor.fetchone()
            
            if not result:
                # Account doesn't exist, add to missing accounts
                missing_accounts.append({
                    'account_id': account_id,
                    'account_name': account_name
                })
                
                # Insert new account
                cursor.execute("""
                    INSERT INTO account_details 
                    (account_id, account_name, hod_id, entity, cloud_id, business_id, 
                     percentage, prod_flg, account_creation_date, cls_flg, cls_date)
                    VALUES (?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """, (account_id, account_name))
        
        # Save missing accounts to CSV if any
        if missing_accounts:
            missing_accounts_df = pd.DataFrame(missing_accounts)
            # Get the absolute path for output directory
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            output_dir = os.path.join(project_root, 'final', 'data_files', 'missing_accounts')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(output_dir, f"missing_accounts_{timestamp}.csv")
            missing_accounts_df.to_csv(output_file, index=False)
            print(f"\nMissing accounts saved to: {output_file}")
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        print(f"\nDatabase update completed. {len(missing_accounts)} new accounts added.")
        
    except Exception as e:
        print(f"Error updating database: {str(e)}")
        if 'conn' in locals():
            conn.close() 