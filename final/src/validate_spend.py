import pandas as pd
import numpy as np
import re
import traceback

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
        # Convert all null/NaN values to 0 first
        df = df.fillna(0)
        
        if file_type == 3:  # Monthly spend
            # Remove total rows before validation
            df = df[~df.iloc[:, 0].isin(['Linked account total', 'Total costs ($)'])]
            df = df.reset_index(drop=True)
            # print(f"[DEBUG] DataFrame shape after removing total rows: {df.shape}")
            # print("[DEBUG] DataFrame preview after removing total rows:")
            # print(df.head(10))
            
            # Check that there are at least 4 rows (header + at least 1 month)
            if df.shape[0] < 4:
                print("[ERROR] DataFrame does not have enough rows after removing total rows.")
                return False, "Not enough rows after removing total rows", {}, {}
            
            # Check that there are at least 3 columns (date + at least 1 account + total)
            if df.shape[1] < 3:
                print("[ERROR] DataFrame does not have enough columns after removing total rows.")
                return False, "Not enough columns after removing total rows", {}, {}
            
            # Calculate column sums (account_id-wise totals)
            try:
                col_sums = df.iloc[2:, 1:-1].astype(float).sum()  # Exclude last column
            except Exception as e:
                print(f"[ERROR] Failed to calculate column sums: {e}")
                print(df.iloc[2:, 1:-1].head())
                return False, f"Failed to calculate column sums: {e}", {}, {}
            
            # Store account-wise monthly totals for post-validation
            account_totals = {}
            month_totals = {}
            
            # Debug: Print DataFrame shape and column count
            # print(f"[DEBUG] Processing DataFrame with shape: {df.shape}")
            # print(f"[DEBUG] Number of columns to process: {len(df.columns)-2}")
            
            for i in range(len(df.columns)-2):  # Exclude first and last columns
                try:
                    account_id = str(df.iloc[0, i+1]).strip()  # First row has account IDs
                    account_name = str(df.iloc[1, i+1]).strip()  # Second row has account names
                    # print(f"[DEBUG] Processing column {i+1}: account_id={account_id}, account_name={account_name}")
                except Exception as e:
                    print(f"[ERROR] Failed to access account_id/account_name at column {i+1}: {e}")
                    continue
                
                if account_id and account_id != 'Total costs ($)':
                    # Remove ($) from account name
                    account_name = account_name.replace(' ($)', '')
                    # Extract numeric part from account_id if it contains non-numeric characters
                    account_id = ''.join(filter(str.isdigit, account_id))
                    
                    if len(account_id) == 12:  # Only store if account_id is valid
                        account_totals[account_id] = {}
                        for j, date in enumerate(df.iloc[2:, 0]):
                            # Debug: Print current iteration details
                            # print(f"[DEBUG] Processing row {j+2}, column {i+1}")
                            # print(f"[DEBUG] DataFrame shape: {df.shape}")
                            # print(f"[DEBUG] Current date: {date}")
                            
                            # Bounds check before accessing DataFrame
                            if j+2 >= df.shape[0] or i+1 >= df.shape[1]:
                                print(f"[ERROR] Index out of bounds: j+2={j+2}, i+1={i+1}, shape={df.shape}")
                                continue
                            
                            try:
                                spend = float(df.iloc[j+2, i+1])
                                # print(f"[DEBUG] Retrieved spend value: {spend}")
                            except Exception as e:
                                print(f"[ERROR] Failed to access spend at row {j+2}, col {i+1}: {e}")
                                continue
                            
                            if pd.notna(spend) and spend > 0:
                                account_totals[account_id][str(date).strip()] = spend
                                # Update month-wise totals
                                if str(date).strip() not in month_totals:
                                    month_totals[str(date).strip()] = 0
                                month_totals[str(date).strip()] += spend
            
            return True, "Pre-transpose validation successful", account_totals, month_totals
                
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
                                try:
                                    spend = float(row[col])
                                except Exception as e:
                                    print(f"[ERROR] Failed to access spend for service {service_name}, col {col}: {e}")
                                    continue
                                if pd.notna(spend) and spend > 0:
                                    account_totals[account_id][service_name][str(col).strip()] = spend
            
            return True, "Pre-transpose validation successful", account_totals, {}
        
    except Exception as e:
        print(f"[ERROR] Exception in validate_pre_transpose: {e}")
        return False, f"Pre-transpose validation error: {str(e)}", {}, {}

def validate_post_transpose(df, file_type, pre_validation_data=None):
    """Validate data after transformation."""
    try:
        # print("[DEBUG] Entered validate_post_transpose")
        # print(f"[DEBUG] DataFrame columns: {df.columns.tolist()}")
        # print(f"[DEBUG] DataFrame shape: {df.shape}")
        # print("[DEBUG] DataFrame head:")
        # print(df.head())
        if file_type == 3:  # Monthly spend
            # print("[DEBUG] Starting post-transpose validation")
            # print(f"[DEBUG] DataFrame shape: {df.shape}")
            # print("[DEBUG] DataFrame preview:")
            # print(df.head())
            
            if pre_validation_data is None:
                print("[ERROR] Pre-validation data is required for post-transpose validation")
                return False, "Pre-validation data is required for post-transpose validation"
            
            account_totals, month_totals = pre_validation_data
            
            # Validate account-wise totals
            # print("[DEBUG] Validating account-wise totals")
            for account_id in account_totals:
                # print(f"[DEBUG] Processing account_id: {account_id}")
                account_data = df[df['account_id'] == account_id]
                if account_data.empty:
                    print(f"[WARNING] No data found for account_id: {account_id}")
                    continue
                
                for month, expected_spend in account_totals[account_id].items():
                    # print(f"[DEBUG] Checking month: {str(month)}, expected spend: {expected_spend}")
                    month_data = account_data[account_data['month'] == month]
                    # print(f"[DEBUG] month_data for account_id={account_id}, month={str(month)}:")
                    # print(month_data)
                    if month_data.empty:
                        print(f"[WARNING] No data found for month: {str(month)} for account_id: {account_id}")
                        continue
                    
                    # Bounds check before accessing iloc[0]
                    if len(month_data['spend']) == 0:
                        print(f"[ERROR] month_data['spend'] is empty for account_id={account_id}, month={month}")
                        continue
                    actual_spend = float(month_data['spend'].iloc[0])  # Convert to float
                    # print(f"[DEBUG] Actual spend: {actual_spend}")
                    if abs(actual_spend - expected_spend) > 0.01:  # Allow for small floating point differences
                        print(f"[ERROR] Mismatch for account {account_id}, month {str(month)}: expected {expected_spend}, got {actual_spend}")
                        return False, f"Account-wise total mismatch for account {str(account_id)}, month {str(month)}"
            
            # Validate month-wise totals
            # print("[DEBUG] Validating month-wise totals")
            for month in month_totals:
                # print(f"[DEBUG] Processing month: {str(month)}")
                month_data = df[df['month'] == month]
                if month_data.empty:
                    print(f"[WARNING] No data found for month: {str(month)}")
                    continue
                
                expected_total = month_totals[month]
                # Ensure spend column is numeric before summing
                spend_numeric = pd.to_numeric(month_data['spend'], errors='coerce')
                actual_total = float(spend_numeric.sum())  # Convert to float
                # print(f"[DEBUG] Month {str(month)}: expected total {expected_total}, actual total {actual_total}")
                if abs(actual_total - expected_total) > 0.01:  # Allow for small floating point differences
                    print(f"[ERROR] Mismatch for month {str(month)}: expected {expected_total}, got {actual_total}")
                    return False, f"Month-wise total mismatch for month {str(month)}"
            
            return True, "Post-transpose validation successful"
                
        else:  # Daily or service monthly spend
            if pre_validation_data is None:
                return False, "Pre-validation data is required for post-transpose validation"
            
            account_totals, _ = pre_validation_data
            
            # Validate account and service-wise totals
            for account_id in account_totals:
                account_data = df[df['account_id'] == account_id]
                if account_data.empty:
                    continue
                
                for service_name, service_data in account_totals[account_id].items():
                    service_rows = account_data[account_data['service_name'] == service_name]
                    if service_rows.empty:
                        continue
                    
                    for date, expected_spend in service_data.items():
                        date_data = service_rows[service_rows['day' if file_type == 1 else 'month'] == date]
                        if date_data.empty:
                            continue
                        
                        actual_spend = float(date_data['spend'].iloc[0])  # Convert to float
                        if abs(actual_spend - expected_spend) > 0.01:
                            return False, f"Service-wise total mismatch for account {account_id}, service {service_name}, date {date}"
            
            return True, "Post-transpose validation successful"
            
    except Exception as e:
        print(f"[ERROR] Exception in validate_post_transpose: {e}")
        traceback.print_exc()
        return False, f"Post-transpose validation error: {str(e)}" 