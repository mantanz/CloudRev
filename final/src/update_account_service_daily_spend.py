import os
import pandas as pd
from transform_spend import transform_daily_spend
from update_spend import update_daily_spend
from update_account_details import validate_and_update_single_account
from update_service_details import update_service_details_in_df
from validate_spend import validate_pre_transpose, validate_post_transpose, extract_account_details_from_filename

def update_account_service_daily_spend(file_path, entity):
    """Update account service daily spend data."""
    try:
        # Extract account details from filename
        account_id, account_name = extract_account_details_from_filename(os.path.basename(file_path))
        if not account_id or not account_name:
            print(f"Could not extract account details from filename: {file_path}")
            return False
            
        # Read input file
        df = pd.read_csv(file_path)
        
        # Pre-transpose validation
        is_valid, message, pre_totals = validate_pre_transpose(df, 1, os.path.basename(file_path))
        if not is_valid:
            print(f"Pre-transpose validation failed: {message}")
            return False
            
        # Transform data
        df_transformed = transform_daily_spend(df)
        if df_transformed is None:
            print(f"Failed to transform file: {file_path}")
            return False
            
        # Add account details from filename
        df_transformed['account_id'] = account_id
        df_transformed['account_name'] = account_name
            
        # Post-transpose validation
        is_valid, message = validate_post_transpose(df_transformed, 1, pre_totals)
        if not is_valid:
            print(f"Post-transpose validation failed: {message}")
            return False
            
        # Save transformed data
        output_path = os.path.join(os.path.dirname(file_path), 
                                 f"transformed_{os.path.basename(file_path)}")
        df_transformed.to_csv(output_path, index=False)
        print(f"Transformed data saved to: {output_path}")
            
        # First update account details
        if not validate_and_update_single_account(account_id, account_name, entity):
            print(f"\nFailed to update account details for file: {file_path}")
            return False
            
        # Get absolute path to database
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        db_path = os.path.join(project_root, 'final', 'sqlite', 'mydatabase.db')
            
        # Update service details and get service_ids
        success, df_with_service_ids = update_service_details_in_df(df_transformed, db_path)
        if not success:
            print(f"\nFailed to update service details for file: {file_path}")
            return False
            
        # Then update spend tables with service_ids
        if update_daily_spend(df_with_service_ids, db_path):
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