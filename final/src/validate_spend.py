import pandas as pd
import numpy as np
import re

def extract_account_details_from_filename(filename):
    """Extract account name and ID from filename."""
    try:
        # Remove .csv extension
        name = filename.replace('.csv', '')
        
        # Extract account ID (12 digits)
        account_id_match = re.search(r'\((\d{12})\)', name)
        if not account_id_match:
            return None, None
            
        account_id = account_id_match.group(1)
        
        # Extract account name (everything before the account ID)
        account_name = name.split('(')[0].strip()
        
        return account_id, account_name
    except:
        return None, None

def validate_pre_transpose(df, file_type, filename=None):
    """Validate data before transformation."""
    try:
        if file_type == 3:  # Monthly spend
            # Remove total rows before validation
            df = df[~df.iloc[:, 0].isin(['Linked account total', 'Total costs ($)'])]
            
            # Calculate column sums
            col_sums = df.iloc[3:, 1:-1].astype(float).sum()  # Exclude last column
            
            # Store account-wise monthly totals for post-validation
            account_totals = {}
            for i in range(len(df.columns)-2):  # Exclude first and last columns
                account_id = str(df.iloc[0, i+1]).strip()  # First row has account IDs
                account_name = str(df.iloc[1, i+1]).strip()  # Second row has account names
                
                if account_id and account_id != 'Total costs ($)':
                    # Remove ($) from account name
                    account_name = account_name.replace(' ($)', '')
                    
                    # Extract numeric part from account_id if it contains non-numeric characters
                    account_id = ''.join(filter(str.isdigit, account_id))
                    if len(account_id) == 12:  # Only store if account_id is valid
                        # Store monthly totals for this account
                        account_totals[account_id] = {}
                        for j, date in enumerate(df.iloc[3:, 0]):
                            spend = float(df.iloc[j+3, i+1])
                            if pd.notna(spend) and spend > 0:
                                account_totals[account_id][str(date).strip()] = spend
            
            return True, "Pre-transpose validation successful", account_totals
                
        else:  # Daily or service monthly spend
            # Remove total rows before validation
            df = df[~df.iloc[:, 0].isin(['Total', 'Service total'])]
            
            # Store account and service-wise totals for post-validation
            account_totals = {}
            if filename:
                account_id, _ = extract_account_details_from_filename(filename)
                if account_id:
                    account_totals[account_id] = {}
                    # Group by service and date/month
                    for idx, row in df.iterrows():
                        if isinstance(row['service_name'], str):  # Skip total rows
                            service_name = row['service_name'].strip()
                            if service_name not in account_totals[account_id]:
                                account_totals[account_id][service_name] = {}
                            
                            # Get spend values for each date/month
                            for col in df.columns[3:]:  # Skip first 3 columns (account_id, account_name, service_name)
                                spend = float(row[col])
                                if pd.notna(spend) and spend > 0:
                                    account_totals[account_id][service_name][str(col).strip()] = spend
            
            return True, "Pre-transpose validation successful", account_totals
        
    except Exception as e:
        return False, f"Pre-transpose validation error: {str(e)}", {}

def validate_post_transpose(df: pd.DataFrame, file_type: str, pre_totals: dict = None, filename: str = None) -> bool:
    """Validate data after transpose operation."""
    try:
        # Determine the correct date column name
        if file_type == 'daily' or file_type == 1:
            date_col = 'day'
        elif file_type == 'monthly' or file_type == 3:
            date_col = 'month'
        else:
            date_col = 'month'  # For service monthly, still 'month'

        # Check required columns
        required_columns = ['account_id', date_col, 'spend']
        if file_type == 1 or file_type == 2:  # Service Daily or Service Monthly
            required_columns.append('service_name')
            
        if not all(col in df.columns for col in required_columns):
            print(f"Missing required columns after transpose: {required_columns}")
            print(f"Found columns: {df.columns.tolist()}")
            return False

        # Remove rows with null or zero spend values
        df = df[df['spend'].notna() & (df['spend'] != 0)]

        # Check for negative spend values
        if (df['spend'] < 0).any():
            print("Found negative spend values")
            return False

        # Validate account IDs
        def process_account_id(acc_id):
            if pd.isna(acc_id):
                return None
            acc_id = str(acc_id).strip()
            numeric_id = ''.join(filter(str.isdigit, acc_id))
            if numeric_id and numeric_id.isdigit():
                return numeric_id.zfill(12)
            return acc_id

        # For Service Daily and Service Monthly files, get account_id from filename
        if file_type == 1 or file_type == 2:  # Service Daily or Service Monthly
            if not filename:
                print("Filename required for Service Daily and Service Monthly files")
                return False
            account_id, _ = extract_account_details_from_filename(filename)
            if not account_id:
                print("Could not extract account ID from filename")
                return False
            # Set all rows to have the same account_id from filename
            df['account_id'] = account_id
        else:  # Account Monthly
            # For Account Monthly, process account IDs from the data
            df['account_id'] = df['account_id'].apply(process_account_id)
            # Remove rows with invalid account IDs
            df = df[df['account_id'].notna()]

        # Validate date format based on file type
        if date_col == 'day':
            date_format = '%Y-%m-%d'
        else:
            date_format = '%Y-%m'
        try:
            # Just verify the dates are valid, but don't modify them
            pd.to_datetime(df[date_col])
        except ValueError:
            print(f"Invalid date format in {date_col} column")
            return False

        # Validate spend totals match pre-transpose totals
        if pre_totals:
            for account_id in pre_totals:
                account_df = df[df['account_id'] == account_id]
                if file_type == 1 or file_type == 2:  # Service Daily or Service Monthly
                    # Group by service and date
                    post_totals = account_df.groupby(['service_name', date_col])['spend'].sum()
                    for (service, date), spend in post_totals.items():
                        if service in pre_totals[account_id] and date in pre_totals[account_id][service]:
                            expected_total = pre_totals[account_id][service][date]
                            if not np.isclose(spend, expected_total, rtol=1e-5):
                                print(f"Total spend mismatch for account {account_id}, service {service}, {date_col} {date}. Expected: {expected_total}, Got: {spend}")
                                return False
                else:  # Account Monthly
                    post_totals = account_df.groupby('month')['spend'].sum().to_dict()
                    for month, expected_total in pre_totals[account_id].items():
                        if month in post_totals:
                            actual_total = post_totals[month]
                            if not np.isclose(actual_total, expected_total, rtol=1e-5):
                                print(f"Total spend mismatch for account {account_id}, month {month}. Expected: {expected_total}, Got: {actual_total}")
                                return False

        return True

    except Exception as e:
        print(f"Error in post-transpose validation: {str(e)}")
        return False 