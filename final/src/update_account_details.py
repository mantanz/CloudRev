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
            # Insert new account
            cursor.execute("""
                INSERT INTO account_details (
                    account_id, account_name, hod_id, entity, 
                    account_type, account_status, account_owner,
                    cost_center, business_unit, region, country
                ) VALUES (?, ?, NULL, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
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

def validate_and_update_accounts_from_file(transformed_file):
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
        
        # Check if we have HOD and entity columns
        has_hod_entity = all(col in df.columns for col in ['hod_name', 'entity'])
        
        for _, row in unique_accounts.iterrows():
            account_id = row['account_id']
            account_name = row['account_name']
            
            # Check if account exists
            cursor.execute("SELECT * FROM account_details WHERE account_id = ?", (account_id,))
            existing_account = cursor.fetchone()
            
            if not existing_account:
                # Get HOD details if available
                hod_id = None
                if has_hod_entity:
                    hod_name = df[df['account_id'] == account_id]['hod_name'].iloc[0]
                    entity = df[df['account_id'] == account_id]['entity'].iloc[0]
                    hod_id = handle_hod_details(cursor, hod_name, entity)
                    new_hod_entries.append({
                        'hod_id': hod_id,
                        'hod_name': hod_name,
                        'entity': entity
                    })
                
                # Insert new account
                cursor.execute("""
                    INSERT INTO account_details (
                        account_id, account_name, hod_id, entity, 
                        account_type, account_status, account_owner,
                        cost_center, business_unit, region, country
                    ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """, (account_id, account_name, hod_id, entity if has_hod_entity else None))
                
                new_accounts.append({
                    'account_id': account_id,
                    'account_name': account_name,
                    'hod_id': hod_id,
                    'entity': entity if has_hod_entity else None
                })
        
        # Save new HOD entries to CSV if any
        if new_hod_entries:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(current_dir)), 'final', 'data_files', 'new_hod_entries')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(output_dir, f'new_hod_entries_{timestamp}.csv')
            
            pd.DataFrame(new_hod_entries).to_csv(output_file, index=False)
            print(f"\nNew HOD entries saved to {output_file}")
        
        # Save new accounts to CSV if any
        if new_accounts:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(current_dir)), 'final', 'data_files', 'new_accounts')
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(output_dir, f'new_accounts_{timestamp}.csv')
            
            pd.DataFrame(new_accounts).to_csv(output_file, index=False)
            print(f"\nNew accounts saved to {output_file}")
        
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