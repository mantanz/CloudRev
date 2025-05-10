import os
import pandas as pd
from transform_spend import transform_monthly_spend
from update_spend import update_monthly_spend
from update_account_details import validate_and_update_single_account
from validate_spend import validate_pre_transpose, validate_post_transpose

def update_account_monthly_spend(file_path, entity):
    """Update account monthly spend data."""
    try:
        # Read input file
        df = pd.read_csv(file_path)
        
        # Pre-transpose validation
        is_valid, message, pre_totals = validate_pre_transpose(df, 3)  # 3 for monthly spend
        if not is_valid:
            print(f"Pre-transpose validation failed: {message}")
            return False
            
        # Transform data
        df_transformed = transform_monthly_spend(df)
        if df_transformed is None:
            print(f"Failed to transform file: {file_path}")
            return False
            
        # Post-transpose validation
        if not validate_post_transpose(df_transformed, 3, pre_totals):
            print(f"Post-transpose validation failed")
            return False
            
        # Save transformed data
        output_path = os.path.join(os.path.dirname(file_path), 
                                 f"transformed_{os.path.basename(file_path)}")
        df_transformed.to_csv(output_path, index=False)
        print(f"Transformed data saved to: {output_path}")
            
        # First update account details for all accounts
        account_update_success = True
        for _, row in df_transformed.iterrows():
            if not validate_and_update_single_account(row['account_id'], row['account_name'], entity):
                account_update_success = False
                print(f"\nFailed to update account details for account: {row['account_id']}")
                break
                
        if not account_update_success:
            print(f"\nFailed to update account details for file: {file_path}")
            return False
            
        # Get absolute path to database
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        db_path = os.path.join(project_root, 'final', 'sqlite', 'mydatabase.db')
            
        # Then update spend tables
        if update_monthly_spend(df_transformed, db_path):
            print(f"\nSuccessfully processed file: {file_path}")
            return True
        else:
            print(f"\nFailed to update spend data for file: {file_path}")
            return False
            
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return False

if __name__ == "__main__":
    # This file is meant to be imported and used by the main program
    print("This module should be imported and used by the main program.") 