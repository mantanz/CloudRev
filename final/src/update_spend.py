import sqlite3
import pandas as pd
from datetime import datetime
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def get_service_type(service_name, db_path):
    """Determine service type using AI-based categorization."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get existing service mappings
        cursor.execute("SELECT service_name, service_type FROM service_details")
        existing_services = cursor.fetchall()
        
        if not existing_services:
            return 'Others'
        
        # Create TF-IDF vectors
        vectorizer = TfidfVectorizer()
        service_names = [s[0] for s in existing_services]
        service_names.append(service_name)
        tfidf_matrix = vectorizer.fit_transform(service_names)
        
        # Calculate similarity with existing services
        similarities = cosine_similarity(tfidf_matrix[-1:], tfidf_matrix[:-1])[0]
        most_similar_idx = np.argmax(similarities)
        
        if similarities[most_similar_idx] > 0.3:  # Threshold for similarity
            return existing_services[most_similar_idx][1]
        
        # If no good match, use keyword-based categorization
        service_name_lower = service_name.lower()
        if any(word in service_name_lower for word in ['ec2', 'lambda', 'compute', 'server']):
            return 'Compute'
        elif any(word in service_name_lower for word in ['s3', 'storage', 'backup']):
            return 'Storage'
        elif any(word in service_name_lower for word in ['vpc', 'network', 'route', 'dns']):
            return 'Network'
        elif any(word in service_name_lower for word in ['iops', 'throughput']):
            return 'IOPS'
        elif any(word in service_name_lower for word in ['rds', 'dynamodb', 'database']):
            return 'DB'
        elif any(word in service_name_lower for word in ['ml', 'ai', 'sagemaker', 'comprehend']):
            return 'AI'
        
        return 'Others'
        
    except Exception as e:
        print(f"Error in service type determination: {str(e)}")
        return 'Others'
    finally:
        conn.close()

def get_next_service_id(db_path):
    """Get next available service_id."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(service_id) FROM service_details")
        max_id = cursor.fetchone()[0]
        return (max_id or 0) + 1
    finally:
        conn.close()

def get_service_id(service_name, db_path):
    """Get or create service_id for a service_name."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if service exists
        cursor.execute("SELECT service_id FROM service_details WHERE service_name = ?", (service_name,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        
        # Create new service
        service_id = get_next_service_id(db_path)
        service_type = get_service_type(service_name, db_path)
        
        cursor.execute("""
            INSERT INTO service_details (service_id, service_name, service_type)
            VALUES (?, ?, ?)
        """, (service_id, service_name, service_type))
        
        conn.commit()
        return service_id
        
    finally:
        conn.close()

def update_monthly_spend(df, db_path):
    """Update monthly spend table."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get existing accounts from account_details
        cursor.execute("SELECT account_id FROM account_details")
        existing_accounts = {row[0] for row in cursor.fetchall()}
        
        # Get accounts from aop_budget_monthly
        cursor.execute("SELECT DISTINCT account_id FROM aop_budget_monthly")
        aop_accounts = {row[0] for row in cursor.fetchall()}
        
        # Track new accounts and their status
        new_accounts = []
        unique_accounts = df[['account_id', 'account_name']].drop_duplicates()
        
        for _, row in unique_accounts.iterrows():
            account_id = row['account_id']
            account_name = row['account_name']
            
            # Check if this is a new account
            is_new = account_id not in existing_accounts
            has_aop = account_id in aop_accounts
            
            if is_new:
                new_accounts.append({
                    'account_id': account_id,
                    'account_name': account_name,
                    'has_aop': has_aop
                })
            
            # Update account_details table
            cursor.execute("""
                INSERT OR REPLACE INTO account_details
                (account_id, account_name, hod_id)
                VALUES (?, ?, ?)
            """, (account_id, account_name, '00000001'))
        
        # Save new accounts info to CSV if any found
        if new_accounts:
            new_accounts_df = pd.DataFrame(new_accounts)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'new_accounts_{timestamp}.csv'
            new_accounts_df.to_csv(output_file, index=False)
            print(f"\nNew accounts found and saved to {output_file}:")
            print(new_accounts_df.to_string())
        
        # Then update monthly spend table (without account_name)
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO as_acct_monthly
                (account_id, month, spend)
                VALUES (?, ?, ?)
            """, (row['account_id'], row['month'], row['spend']))
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"Error updating monthly spend: {str(e)}")
        return False
    finally:
        conn.close()

def update_daily_spend(df, db_path):
    """Update daily spend table."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # First, ensure all services exist in service_details table
        unique_services = df['service_name'].unique()
        for service_name in unique_services:
            # This will create the service if it doesn't exist
            get_service_id(service_name, db_path)
        
        # Now get service IDs for each service
        df['service_id'] = df['service_name'].apply(lambda x: get_service_id(x, db_path))
        
        # Verify all services have valid IDs
        if df['service_id'].isna().any():
            print("Error: Some services could not be mapped to service IDs")
            return False
        
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO as_acct_service_daily
                (account_id, day, spend, service_id)
                VALUES (?, ?, ?, ?)
            """, (row['account_id'], row['day'], row['spend'], row['service_id']))
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"Error updating daily spend: {str(e)}")
        return False
    finally:
        conn.close()

def update_service_daily_spend(df, db_path):
    """Update service daily spend and cascade updates to monthly tables."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get unique months from the input data
        df['month'] = pd.to_datetime(df['day']).dt.strftime('%Y-%m')
        unique_months = df['month'].unique()
        
        # First update service daily table
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO as_acct_service_daily
                (account_id, service_name, day, spend)
                VALUES (?, ?, ?, ?)
            """, (row['account_id'], row['service_name'], row['day'], row['spend']))
        
        # Get service IDs for each service
        df['service_id'] = df['service_name'].apply(lambda x: get_service_id(x, db_path))
        
        # Roll up to service monthly level
        service_monthly_data = df.groupby(['account_id', 'service_id', 'month'])['spend'].sum().reset_index()
        
        # Update service monthly table
        for _, row in service_monthly_data.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO as_acct_service_monthly
                (account_id, month, spend, service_id)
                VALUES (?, ?, ?, ?)
            """, (row['account_id'], row['month'], row['spend'], row['service_id']))
        
        # Roll up to account monthly level
        account_monthly_data = df.groupby(['account_id', 'month'])['spend'].sum().reset_index()
        
        # Update account monthly table
        for _, row in account_monthly_data.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO as_acct_monthly
                (account_id, month, spend)
                VALUES (?, ?, ?)
            """, (row['account_id'], row['month'], row['spend']))
        
        # Validate summaries
        discrepancies = []
        
        # Check service monthly summaries
        for month in unique_months:
            # Get service monthly data from daily rollup
            cursor.execute("""
                SELECT account_id, service_id, SUM(spend) as daily_sum
                FROM as_acct_service_daily
                WHERE month = ?
                GROUP BY account_id, service_id
            """, (month,))
            daily_rollup = {f"{row[0]}_{row[1]}": row[2] for row in cursor.fetchall()}
            
            # Get service monthly data directly
            cursor.execute("""
                SELECT account_id, service_id, spend
                FROM as_acct_service_monthly
                WHERE month = ?
            """, (month,))
            monthly_data = {f"{row[0]}_{row[1]}": row[2] for row in cursor.fetchall()}
            
            # Compare and record discrepancies
            for key in set(daily_rollup.keys()) | set(monthly_data.keys()):
                daily_sum = daily_rollup.get(key, 0)
                monthly_sum = monthly_data.get(key, 0)
                if not np.isclose(daily_sum, monthly_sum, rtol=1e-5):
                    account_id, service_id = key.split('_')
                    discrepancies.append({
                        'month': month,
                        'account_id': account_id,
                        'service_id': service_id,
                        'daily_sum': daily_sum,
                        'monthly_sum': monthly_sum,
                        'difference': daily_sum - monthly_sum
                    })
        
        # Save discrepancies to CSV if any found
        if discrepancies:
            discrepancies_df = pd.DataFrame(discrepancies)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'spend_discrepancies_{timestamp}.csv'
            discrepancies_df.to_csv(output_file, index=False)
            print(f"\nDiscrepancies found and saved to {output_file}:")
            print(discrepancies_df.to_string())
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"Error updating service daily spend: {str(e)}")
        return False
    finally:
        conn.close()

def update_service_monthly_spend(df, db_path):
    """Update service monthly spend and cascade updates to account monthly table."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get unique months from the input data
        unique_months = df['month'].unique()
        
        # Get service IDs for each service
        df['service_id'] = df['service_name'].apply(lambda x: get_service_id(x, db_path))
        
        # Update service monthly table
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO as_acct_service_monthly
                (account_id, month, spend, service_id)
                VALUES (?, ?, ?, ?)
            """, (row['account_id'], row['month'], row['spend'], row['service_id']))
        
        # Roll up to account monthly level
        account_monthly_data = df.groupby(['account_id', 'month'])['spend'].sum().reset_index()
        
        # Update account monthly table
        for _, row in account_monthly_data.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO as_acct_monthly
                (account_id, month, spend)
                VALUES (?, ?, ?)
            """, (row['account_id'], row['month'], row['spend']))
        
        # Validate summaries
        discrepancies = []
        
        # Check account monthly summaries
        for month in unique_months:
            # Get account monthly data from service monthly rollup
            cursor.execute("""
                SELECT account_id, SUM(spend) as service_sum
                FROM as_acct_service_monthly
                WHERE month = ?
                GROUP BY account_id
            """, (month,))
            service_rollup = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Get account monthly data directly
            cursor.execute("""
                SELECT account_id, spend
                FROM as_acct_monthly
                WHERE month = ?
            """, (month,))
            monthly_data = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Compare and record discrepancies
            for account_id in set(service_rollup.keys()) | set(monthly_data.keys()):
                service_sum = service_rollup.get(account_id, 0)
                monthly_sum = monthly_data.get(account_id, 0)
                if not np.isclose(service_sum, monthly_sum, rtol=1e-5):
                    discrepancies.append({
                        'month': month,
                        'account_id': account_id,
                        'service_sum': service_sum,
                        'monthly_sum': monthly_sum,
                        'difference': service_sum - monthly_sum
                    })
        
        # Save discrepancies to CSV if any found
        if discrepancies:
            discrepancies_df = pd.DataFrame(discrepancies)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'spend_discrepancies_{timestamp}.csv'
            discrepancies_df.to_csv(output_file, index=False)
            print(f"\nDiscrepancies found and saved to {output_file}:")
            print(discrepancies_df.to_string())
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"Error updating service monthly spend: {str(e)}")
        return False
    finally:
        conn.close()

def update_spend_data(df, file_type, db_path):
    """Main function to update spend data in database."""
    try:
        if file_type == 1:  # Daily spend
            return update_daily_spend(df, db_path)
        elif file_type == 2:  # Service monthly spend
            return update_service_monthly_spend(df, db_path)
        else:  # Monthly spend
            return update_monthly_spend(df, db_path)
            
    except Exception as e:
        print(f"Error updating spend data: {str(e)}")
        return False 