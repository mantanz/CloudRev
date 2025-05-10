import sqlite3
import pandas as pd
import os
from datetime import datetime

def get_db_connection():
    """Get a connection to the SQLite database"""
    try:
        # Get the absolute path to the database file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        db_path = os.path.join(project_root, 'final', 'sqlite', 'mydatabase.db')
        
        # Ensure the database directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        return False

def get_next_hod_id(cursor):
    """Get the next available HOD ID"""
    cursor.execute("SELECT MAX(hod_id) FROM hod_details")
    max_id = cursor.fetchone()[0]
    
    if max_id is None:
        return "HOD_001"
    
    # Extract the numeric part and increment
    num = int(max_id.split('_')[1])
    return f"HOD_{num + 1:03d}"

def handle_hod_details(cursor, hod_name, entity):
    """Handle HOD details, creating new entries if needed"""
    # Check if HOD exists
    cursor.execute("""
        SELECT hod_id FROM hod_details 
        WHERE hod_name = ? AND entity = ?
    """, (hod_name, entity))
    
    result = cursor.fetchone()
    if result:
        return result[0]
    
    # Create new HOD entry
    new_hod_id = get_next_hod_id(cursor)
    cursor.execute("""
        INSERT INTO hod_details (hod_id, hod_name, entity)
        VALUES (?, ?, ?)
    """, (new_hod_id, hod_name, entity))
    
    return new_hod_id

def validate_and_update_single_account(account_id, account_name, entity):
    """Validate and update a single account in the database"""
    try:
        # Get database connection
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Check if account exists
        cursor.execute("SELECT * FROM account_details WHERE account_id = ?", (account_id,))
        existing_account = cursor.fetchone()
        
        if not existing_account:
            # Insert new account with default HOD ID
            cursor.execute("""
                INSERT INTO account_details (
                    account_id, account_name, hod_id, entity, 
                    cloud_id, business_id, percentage, prod_flg,
                    account_creation_date, cls_flg, cls_date
                ) VALUES (?, ?, '00000001', ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            """, (account_id, account_name, entity))
            
            print(f"Added new account: {account_id} - {account_name}")
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"Error updating account {account_id}: {str(e)}")
        if 'conn' in locals():
            conn.close()
        return False

def validate_and_update_accounts_from_file(transformed_file, selected_entity):
    """Validate accounts and update the database from a transformed file"""
    try:
        # Read the transformed data
        df = pd.read_csv(transformed_file)
        
        # Get database connection
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Track new accounts and HOD entries
        new_accounts = []
        new_hod_entries = []
        
        # Get unique accounts
        unique_accounts = df[['account_id', 'account_name']].drop_duplicates()
        
        # Check if we have HOD column
        has_hod = 'hod_name' in df.columns
        
        for _, row in unique_accounts.iterrows():
            account_id = row['account_id']
            account_name = row['account_name']
            
            # Skip if the input account_name contains 'Redacted'
            if 'Redacted' in account_name:
                continue
                
            # Check if account exists based only on account_id
            cursor.execute("SELECT account_name FROM account_details WHERE account_id = ?", (account_id,))
            existing_account = cursor.fetchone()
            
            if existing_account:
                # If account exists and name is different, update the account_name
                if existing_account['account_name'] != account_name:
                    cursor.execute("""
                        UPDATE account_details 
                        SET account_name = ? 
                        WHERE account_id = ?
                    """, (account_name, account_id))
                    print(f"Updated account_name for account_id {account_id} from {existing_account['account_name']} to {account_name}")
            else:
                # Get HOD details if available
                hod_id = '00000001'  # Default HOD ID
                if has_hod:
                    hod_name = df[df['account_id'] == account_id]['hod_name'].iloc[0]
                    hod_id = handle_hod_details(cursor, hod_name, selected_entity)
                    new_hod_entries.append({
                        'hod_id': hod_id,
                        'hod_name': hod_name,
                        'entity': selected_entity
                    })
                # Insert only if account_id does not exist (INSERT OR IGNORE for extra safety)
                cursor.execute("""
                    INSERT OR IGNORE INTO account_details (
                        account_id, account_name, hod_id, entity, 
                        cloud_id, business_id, percentage, prod_flg,
                        account_creation_date, cls_flg, cls_date
                    ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """, (account_id, account_name, hod_id, selected_entity))
                # Add more details to new_accounts for better tracking
                new_accounts.append({
                    'account_id': account_id,
                    'account_name': account_name,
                    'hod_id': hod_id,
                    'entity': selected_entity,
                    'source_file': os.path.basename(transformed_file),
                    'added_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'status': 'New Account Added'
                })
        
        # Save new HOD entries to CSV if any
        if new_hod_entries:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            output_dir = os.path.join(os.path.dirname(os.path.dirname(current_dir)), 'final', 'data_files', 'new_hod_entries')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(output_dir, f'new_hod_entries_{timestamp}.csv')
            
            pd.DataFrame(new_hod_entries).to_csv(output_file, index=False)
            print(f"\nNew HOD entries saved to {output_file}")
        
        # Save new accounts to CSV if any
        if new_accounts:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            output_dir = os.path.join(os.path.dirname(os.path.dirname(current_dir)), 'final', 'data_files', 'new_accounts')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(output_dir, f'new_accounts_{timestamp}.csv')
            
            # Create DataFrame and save with detailed information
            new_accounts_df = pd.DataFrame(new_accounts)
            new_accounts_df.to_csv(output_file, index=False)
            print(f"\nNew accounts saved to {output_file}")
            print("\nNew accounts added:")
            print(new_accounts_df[['account_id', 'account_name', 'entity', 'added_date']].to_string())
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        print(f"\nDatabase update completed:")
        print(f"- {len(new_accounts)} new accounts added")
        print(f"- {len(new_hod_entries)} new HOD entries created")
        
        return True
        
    except Exception as e:
        print(f"Error updating database: {str(e)}")
        if 'conn' in locals():
            conn.close()
        return False 