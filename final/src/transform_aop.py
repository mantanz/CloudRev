import pandas as pd
import numpy as np
from datetime import datetime
import os

def detect_file_format(df):
    """Detect the format of the input file"""
    # Check if hod_name and entity columns exist
    has_hod_entity = all(col in df.columns for col in ['hod_name', 'entity'])
    
    # Get month columns based on format
    if has_hod_entity:
        # Format 1: account_id,account_name,hod_name,entity,Apr-25,...
        month_cols = df.columns[4:]
    else:
        # Format 2: account_id,account_name,Apr-24,...
        month_cols = df.columns[2:]
    
    return has_hod_entity, month_cols

def validate_totals(df, month_cols):
    """Validate horizontal and vertical totals"""
    # Convert all month columns to float, handling any non-numeric values
    for col in month_cols:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
    
    # Horizontal validation (row totals)
    row_totals = df[month_cols].sum(axis=1)
    print("Row totals validation:")
    print(row_totals.describe())
    
    # Vertical validation (column totals)
    col_totals = df[month_cols].sum(axis=0)
    print("\nColumn totals validation:")
    print(col_totals.describe())
    
    return row_totals, col_totals

def normalize_account_id(account_id):
    """Normalize account_id to 12 characters only if it's numeric"""
    if pd.isna(account_id) or account_id == '':
        return None
    
    # Convert to string and strip whitespace
    account_id = str(account_id).strip()
    
    # If it starts with No_AC_ID or contains any non-numeric characters, return as is
    if account_id.startswith('No_AC_ID') or not account_id.isdigit():
        return account_id
    
    # For numeric account IDs, pad with leading zeros to make it 12 digits
    return account_id.zfill(12)

def check_duplicate_accounts(df):
    """Check for duplicate account IDs in the data"""
    # Normalize account IDs
    df['normalized_account_id'] = df['account_id'].apply(normalize_account_id)
    
    # Find duplicate account IDs
    duplicates = df[df['normalized_account_id'].duplicated(keep=False)].sort_values('normalized_account_id')
    
    if not duplicates.empty:
        print("\nERROR: Duplicate account IDs found in the input file:")
        for account_id, group in duplicates.groupby('normalized_account_id'):
            print(f"\nAccount ID: {account_id}")
            for _, row in group.iterrows():
                print(f"- Account Name: {row['account_name']}")
                if 'hod_name' in row and 'entity' in row:
                    print(f"  HOD: {row['hod_name']}")
                    print(f"  Entity: {row['entity']}")
        
        print("\nPlease clean the data by removing or correcting duplicate account IDs before proceeding.")
        return True
    
    return False

def transform_aop_data(input_file):
    """Transform AOP data from wide to long format"""
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        
        # Detect file format and get month columns
        has_hod_entity, month_cols = detect_file_format(df)
        print(f"\nDetected file format: {'Format 1 (with HOD and entity)' if has_hod_entity else 'Format 2 (without HOD and entity)'}")
        
        # Check for duplicate account IDs
        if check_duplicate_accounts(df):
            return None
        
        # Create month mapping based on the actual columns in the file
        month_mapping = {}
        for col in month_cols:
            # Extract month and year from column name (e.g., 'Apr-25' -> '01-Apr-2025')
            month, year = col.split('-')
            month_mapping[col] = f'01-{month}-20{year}'
        
        # Validate original data
        print("Validating original data...")
        original_row_totals, original_col_totals = validate_totals(df, month_cols)
        
        # Prepare id_vars based on format
        id_vars = ['account_id', 'account_name']
        if has_hod_entity:
            id_vars.extend(['hod_name', 'entity'])
        
        # Normalize account_id in original data
        df['account_id'] = df['account_id'].apply(normalize_account_id)
        
        # Melt the dataframe
        df_melted = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=month_cols,
            var_name='month',
            value_name='aop_amount'
        )
        
        # Convert aop_amount to numeric, handling any non-numeric values
        df_melted['aop_amount'] = pd.to_numeric(df_melted['aop_amount'].astype(str).str.replace(',', ''), errors='coerce')
        
        # Convert month to date format
        df_melted['month'] = df_melted['month'].map(month_mapping)
        df_melted['month'] = pd.to_datetime(df_melted['month'], format='%d-%b-%Y')
        
        # Validate transformed data
        print("\nValidating transformed data...")
        
        # Validate monthly totals
        transformed_totals = df_melted.groupby('month')['aop_amount'].sum()
        print("\nMonthly totals in transformed data:")
        print(transformed_totals)
        
        # Validate account-level totals
        print("\nValidating account-level totals...")
        account_totals = df_melted.groupby(['account_id'])['aop_amount'].sum()
        
        # Validate totals match between original and transformed data
        print("\nValidating totals match between original and transformed data...")
        
        # Validate monthly totals
        print("\nValidating monthly totals...")
        monthly_mismatches = False
        for month, total in transformed_totals.items():
            original_month = month.strftime('%b-%y')
            if not np.isclose(total, original_col_totals[original_month], rtol=1e-5, atol=1e-5):
                print(f"Warning: Total mismatch for {original_month}")
                print(f"Original: {original_col_totals[original_month]}")
                print(f"Transformed: {total}")
                monthly_mismatches = True
        
        # Validate account-level totals
        print("\nValidating account-level totals...")
        account_mismatches = False
        mismatch_details = []
        
        # Calculate original account totals
        original_account_totals = df.groupby('account_id')[month_cols].sum().sum(axis=1)
        
        for account_id, total in account_totals.items():
            # Get original total using normalized account ID
            original_total = original_account_totals.get(account_id, 0.0)
            
            # Use both relative and absolute tolerance for comparison
            if not np.isclose(total, original_total, rtol=1e-5, atol=1e-5):
                account_name = df[df['account_id'] == account_id]['account_name'].iloc[0]
                mismatch_details.append({
                    'account_id': account_id,
                    'account_name': account_name,
                    'original_total': original_total,
                    'transformed_total': total,
                    'difference': abs(total - original_total)
                })
                account_mismatches = True
        
        if monthly_mismatches:
            print("\nValidation failed: Monthly totals do not match between original and transformed data")
            return None
        
        if account_mismatches:
            print("\nAccount-level total mismatches detected:")
            # Sort mismatches by difference magnitude
            mismatch_details.sort(key=lambda x: x['difference'], reverse=True)
            
            for mismatch in mismatch_details:
                print(f"\nAccount: {mismatch['account_id']} ({mismatch['account_name']})")
                print(f"Original total: {mismatch['original_total']:.2f}")
                print(f"Transformed total: {mismatch['transformed_total']:.2f}")
                print(f"Difference: {mismatch['difference']:.2f}")
            
            print("\nValidation failed: Account-level totals do not match between original and transformed data")
            return None
        
        # Save transformed data
        # Get the absolute path for output directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        output_dir = os.path.join(project_root, 'final', 'data_files', 'AOP')
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = os.path.join(output_dir, os.path.basename(input_file).replace('.csv', '_transformed.csv'))
        df_melted.to_csv(output_file, index=False)
        print(f"\nTransformed data saved to {output_file}")
        
        return output_file
        
    except Exception as e:
        print(f"Error transforming data: {str(e)}")
        return None 