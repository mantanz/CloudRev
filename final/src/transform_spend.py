import pandas as pd
import numpy as np

def transform_monthly_spend(df):
    """Transform monthly spend data from wide to long format."""
    try:
        # Remove total rows
        df = df[~df.iloc[:, 0].isin(['Linked account total', 'Total costs ($)'])]
        
        # Get account IDs and names from first two rows
        account_ids = df.iloc[0, 1:-1].values  # Exclude last column (Total)
        account_names = df.iloc[1, 1:-1].values  # Exclude last column (Total)
        
        # Get spend data from remaining rows
        spend_data = df.iloc[3:, 1:-1]  # Exclude last column (Total)
        dates = df.iloc[3:, 0].values
        
        # Create list to store transformed data
        transformed_data = []
        
        # Process each account
        for i in range(len(account_ids)):
            account_id = str(account_ids[i]).strip()
            account_name = str(account_names[i]).strip()
            
            # Skip if account_id is empty or is a total row
            if not account_id or account_id == 'Total costs ($)':
                continue
                
            # Extract numeric part from account_id
            numeric_id = ''.join(filter(str.isdigit, account_id))
            
            # If we have a numeric ID, pad it with leading zeros to make it 12 digits
            if numeric_id:
                account_id = numeric_id.zfill(12)
            
            # Remove ($) from account name
            account_name = account_name.replace(' ($)', '')
            
            # Skip if account_name is empty or contains only numbers
            if not account_name or account_name.replace('.', '').isdigit():
                continue
            
            # Process each date for this account
            for j in range(len(dates)):
                date = str(dates[j]).strip()
                spend = spend_data.iloc[j, i]
                
                # Skip if spend is null or zero
                if pd.notna(spend) and float(spend) > 0:
                    transformed_data.append({
                        'account_id': account_id,
                        'account_name': account_name,
                        'month': date,  # Keep original date format
                        'spend': float(spend)
                    })
        
        # Convert to DataFrame
        df_transformed = pd.DataFrame(transformed_data)
        
        # Sort by account_id and month
        df_transformed = df_transformed.sort_values(['account_id', 'month'])
        
        return df_transformed
        
    except Exception as e:
        print(f"Error transforming monthly spend data: {str(e)}")
        return None

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
        df = pd.read_csv(input_file)
        
        # Validate data based on file type
        if file_type == 3:  # Monthly spend
            is_valid, message = validate_monthly_spend(df)
        else:  # Daily or service monthly spend
            is_valid, message = validate_service_data(df)
            
        if not is_valid:
            print(f"Validation failed: {message}")
            return None
            
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