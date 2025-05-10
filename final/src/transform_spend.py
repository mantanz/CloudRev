import pandas as pd
import numpy as np

def transform_monthly_spend(df):
    """
    Transform monthly spend data from wide to long format.
    Args:
        df (pd.DataFrame): Input DataFrame with monthly spend data
    Returns:
        pd.DataFrame: Transformed DataFrame with columns: account_id, account_name, month, spend
    """
    # Show the DataFrame as read from the CSV before any processing
    print("\nOriginal DataFrame as read from CSV (header=None):")
    print(df.head(10))
    
    # Ignore the last column (totals)
    df = df.iloc[:, :-1]
    # Extract account_id and account_name from the first two rows
    account_ids = df.iloc[0].values[1:]
    account_names = df.iloc[1].values[1:]
    # Extract month rows (from row 3 onwards, skipping 'Linked account total')
    months = df.iloc[3:, 0].values
    # Extract spend values (from row 3 onwards, columns 1:)
    spend_data = df.iloc[3:, 1:].values
    # Build long DataFrame
    records = []
    for i, account_id in enumerate(account_ids):
        for j, month in enumerate(months):
            spend = spend_data[j, i]
            records.append({
                'account_id': account_id,
                'account_name': account_names[i],
                'month': month,
                'spend': spend
            })
    df_long = pd.DataFrame(records)
    # Replace NaN spend values with 0
    df_long['spend'] = df_long['spend'].fillna(0)
    # Remove ' ($)' suffix from account_name
    df_long['account_name'] = df_long['account_name'].str.replace(' ($)', '')
    print("\nTransformed DataFrame (first 10 rows):")
    print(df_long.head(10))
    return df_long

def transform_daily_spend(df):
    """Transform daily spend data."""
    try:
        # Transpose and clean data
        df_transformed = df.melt(
            id_vars=['account_id', 'service_name'],
            var_name='day',
            value_name='spend'
        )
        
        # Remove total rows
        df_transformed = df_transformed[df_transformed['account_id'] != 'Total']
        
        # Process account IDs
        def process_account_id(acc_id):
            if pd.isna(acc_id):
                return None
            acc_id = str(acc_id).strip()
            numeric_id = ''.join(filter(str.isdigit, acc_id))
            if numeric_id and numeric_id.isdigit():
                return numeric_id.zfill(12)
            return acc_id

        df_transformed['account_id'] = df_transformed['account_id'].apply(process_account_id)
        
        return df_transformed
        
    except Exception as e:
        print(f"Error transforming daily spend data: {str(e)}")
        return None

def transform_service_monthly_spend(df):
    """Transform service monthly spend data."""
    try:
        # Transpose and clean data
        df_transformed = df.melt(
            id_vars=['account_id', 'service_name'],
            var_name='month',
            value_name='spend'
        )
        
        # Remove total rows
        df_transformed = df_transformed[df_transformed['account_id'] != 'Total']
        
        # Process account IDs
        def process_account_id(acc_id):
            if pd.isna(acc_id):
                return None
            acc_id = str(acc_id).strip()
            numeric_id = ''.join(filter(str.isdigit, acc_id))
            if numeric_id and numeric_id.isdigit():
                return numeric_id.zfill(12)
            return acc_id

        df_transformed['account_id'] = df_transformed['account_id'].apply(process_account_id)
        
        return df_transformed
        
    except Exception as e:
        print(f"Error transforming service monthly spend data: {str(e)}")
        return None

def validate_monthly_spend(df):
    """Validate monthly spend data."""
    try:
        # Get total row
        total_row = df[df.iloc[:, 0] == 'Total costs ($)'].iloc[0, 1:].astype(float)
        
        # Calculate column sums excluding total row
        col_sums = df.iloc[3:-1, 1:].astype(float).sum()
        
        # Compare totals
        if not np.allclose(total_row, col_sums, rtol=1e-5):
            return False, "Column totals don't match"
        
        return True, "Validation successful"
        
    except Exception as e:
        return False, f"Validation error: {str(e)}"

def validate_service_data(df):
    """Validate service data (daily or monthly)."""
    try:
        # Validate row totals
        row_totals = df.iloc[:, 3:].sum(axis=1)
        total_row = df[df.iloc[:, 0] == 'Total'].iloc[:, 3:].sum(axis=1)
        
        if not np.allclose(row_totals, total_row, rtol=1e-5):
            return False, "Row totals don't match"
        
        # Validate column totals
        col_totals = df.iloc[:, 3:].sum(axis=0)
        total_col = df[df.iloc[:, 0] == 'Total'].iloc[:, 3:].sum(axis=0)
        
        if not np.allclose(col_totals, total_col, rtol=1e-5):
            return False, "Column totals don't match"
        
        return True, "Validation successful"
        
    except Exception as e:
        return False, f"Validation error: {str(e)}"

def transform_spend_data(input_file, file_type):
    """Main function to transform spend data."""
    try:
        # Read input file
        df = pd.read_csv(input_file,header=None)
            
        # Transform data based on file type
        if file_type == 1:  # Daily spend
            return transform_daily_spend(df)
        elif file_type == 2:  # Service monthly spend
            return transform_service_monthly_spend(df)
        else:  # Monthly spend
            return transform_monthly_spend(df)
            
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return None 