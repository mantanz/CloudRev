import sqlite3
import pandas as pd
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

def get_next_hod_id(cursor):
    """Get the next available HOD ID for new entries"""
    cursor.execute("""
        SELECT hod_id FROM hod_details 
        WHERE hod_id LIKE 'NEW_%' 
        ORDER BY CAST(SUBSTR(hod_id, 5) AS INTEGER) DESC 
        LIMIT 1
    """)
    result = cursor.fetchone()
    
    if result:
        last_num = int(result[0].split('_')[1])
        return f"NEW_{last_num + 1}"
    else:
        return "NEW_1"

def handle_hod_details(cursor, hod_name):
    """Handle HOD details and return HOD ID"""
    # Check if HOD exists
    cursor.execute("SELECT hod_id FROM hod_details WHERE hod_name = ?", (hod_name,))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    else:
        # Create new HOD entry
        new_hod_id = get_next_hod_id(cursor)
        cursor.execute("""
            INSERT INTO hod_details (hod_id, hod_name, hod_email_id, tech_leader)
            VALUES (?, ?, NULL, NULL)
        """, (new_hod_id, hod_name))
        
        return new_hod_id

def validate_and_update_accounts(transformed_file):
    """Validate accounts against database and update if needed"""
    try:
        # Read the transformed file
        df = pd.read_csv(transformed_file)
        
        # Get unique accounts with HOD and entity information
        accounts = df[['account_id', 'account_name', 'hod_name', 'entity']].drop_duplicates()
        
        # Connect to database
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Create missing accounts file
        missing_accounts = []
        new_hod_entries = []
        
        # Check each account
        for _, row in accounts.iterrows():
            account_id = row['account_id']
            account_name = row['account_name']
            hod_name = row['hod_name']
            entity = row['entity']
            
            # Check if account exists
            cursor.execute("SELECT account_id FROM account_details WHERE account_id = ?", (account_id,))
            result = cursor.fetchone()
            
            if not result:
                # Handle HOD details
                hod_id = handle_hod_details(cursor, hod_name)
                
                # If it's a new HOD entry, add to tracking list
                if hod_id.startswith('NEW_'):
                    new_hod_entries.append({
                        'hod_id': hod_id,
                        'hod_name': hod_name
                    })
                
                # Account doesn't exist, add to missing accounts
                missing_accounts.append({
                    'account_id': account_id,
                    'account_name': account_name,
                    'hod_id': hod_id,
                    'entity': entity
                })
                
                # Insert new account
                cursor.execute("""
                    INSERT INTO account_details 
                    (account_id, account_name, hod_id, entity, cloud_id, business_id, 
                     percentage, prod_flg, account_creation_date, cls_flg, cls_date)
                    VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """, (account_id, account_name, hod_id, entity))
        
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
        
        # Save new HOD entries to CSV if any
        if new_hod_entries:
            new_hod_df = pd.DataFrame(new_hod_entries)
            output_dir = os.path.join(project_root, 'final', 'data_files', 'new_hod_entries')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(output_dir, f"new_hod_entries_{timestamp}.csv")
            new_hod_df.to_csv(output_file, index=False)
            print(f"\nNew HOD entries saved to: {output_file}")
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        print(f"\nDatabase update completed:")
        print(f"- {len(missing_accounts)} new accounts added")
        print(f"- {len(new_hod_entries)} new HOD entries created")
        return True
        
    except Exception as e:
        print(f"Error updating database: {str(e)}")
        if 'conn' in locals():
            conn.close()
        return False 