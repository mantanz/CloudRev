import pandas as pd
import numpy as np
from datetime import datetime
import os

def validate_totals(df):
    """Validate horizontal and vertical totals"""
    # Horizontal validation (row totals)
    row_totals = df.iloc[:, 2:].sum(axis=1)
    print("Row totals validation:")
    print(row_totals.describe())
    
    # Vertical validation (column totals)
    col_totals = df.iloc[:, 2:].sum(axis=0)
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

def transform_aop_data(input_file):
    """Transform AOP data from wide to long format"""
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        
        # Create a copy of original data with normalized account IDs for validation
        df_original = df.copy()
        df_original['account_id'] = df_original['account_id'].apply(normalize_account_id)
        
        # Validate original data
        print("Validating original data...")
        original_row_totals, original_col_totals = validate_totals(df)
        
        # Create month mapping
        month_mapping = {
            'Apr-24': '01-Apr-2024',
            'May-24': '01-May-2024',
            'Jun-24': '01-Jun-2024',
            'Jul-24': '01-Jul-2024',
            'Aug-24': '01-Aug-2024',
            'Sep-24': '01-Sep-2024',
            'Oct-24': '01-Oct-2024',
            'Nov-24': '01-Nov-2024',
            'Dec-24': '01-Dec-2024',
            'Jan-25': '01-Jan-2025',
            'Feb-25': '01-Feb-2025',
            'Mar-25': '01-Mar-2025'
        }
        
        # Melt the dataframe
        df_melted = pd.melt(
            df,
            id_vars=['account_id', 'account_name'],
            value_vars=list(month_mapping.keys()),
            var_name='month',
            value_name='aop_amount'
        )
        
        # Normalize account_id
        df_melted['account_id'] = df_melted['account_id'].apply(normalize_account_id)
        
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
        account_totals = df_melted.groupby(['account_id', 'account_name'])['aop_amount'].sum()
        
        # Validate totals match between original and transformed data
        print("\nValidating totals match between original and transformed data...")
        
        # Validate monthly totals
        print("\nValidating monthly totals...")
        monthly_mismatches = False
        for month, total in transformed_totals.items():
            original_month = month.strftime('%b-%y')
            if not np.isclose(total, original_col_totals[original_month], rtol=1e-5):
                print(f"Warning: Total mismatch for {original_month}")
                print(f"Original: {original_col_totals[original_month]}")
                print(f"Transformed: {total}")
                monthly_mismatches = True
        
        # Validate account-level totals
        print("\nValidating account-level totals...")
        account_mismatches = False
        for (account_id, account_name), total in account_totals.items():
            # Get original total using normalized account ID
            original_total = df_original[df_original['account_id'] == account_id].iloc[:, 2:].sum().sum()
            if not np.isclose(total, original_total, rtol=1e-5):
                print(f"Warning: Account total mismatch for {account_id} ({account_name})")
                print(f"Original: {original_total}")
                print(f"Transformed: {total}")
                account_mismatches = True
        
        if monthly_mismatches or account_mismatches:
            print("\nValidation failed: Totals do not match between original and transformed data")
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