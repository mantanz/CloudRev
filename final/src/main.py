import os
import pandas as pd
import traceback
from update_account_details import validate_and_update_accounts_from_file
from update_account_service_daily_spend import update_account_service_daily_spend
from update_account_service_monthly_spend import update_account_service_monthly_spend
from update_account_monthly_spend import update_account_monthly_spend
from transform_aop import transform_aop_data
from update_aop_budget import update_aop_budget_monthly
from validate_spend import validate_pre_transpose, validate_post_transpose
from transform_spend import transform_spend_data

def get_user_choice():
    """Get user's choice of operation"""
    print("\nPlease select the type of file to process:")
    print("1. AOP (Annual Operating Plan)")
    print("2. Spend")
    print("3. Exit")
    
    while True:
        choice = input("\nEnter your choice (1-3): ").strip()
        if choice in ['1', '2', '3']:
            return int(choice)
        print("Invalid choice. Please enter 1, 2, or 3.")

def get_entity_choice():
    """Get user's choice of entity"""
    print("\nSelect entity:")
    print("1. OCL")
    print("2. PPSL")
    print("3. PIBPL")
    print("4. Nearbuy")
    print("5. PML")
    print("6. Creditmate")
    print("7. PaiPai")
    
    while True:
        choice = input("Enter option (default: 1): ").strip()
        if not choice:
            return "OCL"
        if choice in ['1', '2', '3', '4', '5', '6', '7']:
            entities = ["OCL", "PPSL", "PIBPL", "Nearbuy", "PML", "Creditmate", "PaiPai"]
            return entities[int(choice) - 1]
        print("Invalid choice. Please enter a number between 1 and 7.")

def get_file_type_choice():
    """Get user's choice of file type"""
    print("\nSelect file type:")
    print("1. Account Service Daily")
    print("2. Account Service Monthly")
    print("3. Account Monthly")
    
    while True:
        choice = input("Enter option (default: 1): ").strip()
        if not choice:
            return 1
        if choice in ['1', '2', '3']:
            return int(choice)
        print("Invalid choice. Please enter 1, 2, or 3.")

def main():
    """Main function to process files."""
    print("\nWelcome to CloudRev Data Processing Tool")
    
    choice = get_user_choice()
    if choice == 3:
        print("Exiting.")
        return
    
    entity = get_entity_choice()
    
    if choice == 1:
        # AOP
        file_path = input("\nEnter path to AOP file: ").strip()
        if not os.path.isfile(file_path):
            print(f"File not found: {file_path}")
            return
        try:
            transformed_file = transform_aop_data(file_path)
            if not transformed_file:
                print("Error transforming AOP data")
                return
            if update_aop_budget_monthly(transformed_file):
                print("AOP budget updated successfully")
            else:
                print("Failed to update AOP budget")
        except Exception as e:
            print(f"Error processing AOP file {file_path}: {str(e)}")
            traceback.print_exc()
        return
    elif choice == 2:
        # Spend
        file_type = get_file_type_choice()
        file_path = input("\nEnter path to Spend file: ").strip()
        if not os.path.isfile(file_path):
            print(f"File not found: {file_path}")
            return
        try:
            # Read the file
            df = pd.read_csv(file_path, header=None)
            # Validate pre-transpose
            is_valid, message, account_totals, month_totals = validate_pre_transpose(df, file_type)
            if not is_valid:
                print(f"Pre-transpose validation failed: {message}")
                return
            # Transform the data
            transformed_df = transform_spend_data(file_path, file_type)
            if transformed_df is None:
                print("Error transforming data")
                return
            # Debug: Print before post-transpose validation
            print("[DEBUG] About to call validate_post_transpose")
            print(f"[DEBUG] transformed_df columns: {transformed_df.columns.tolist()}")
            print(f"[DEBUG] transformed_df shape: {transformed_df.shape}")
            print(transformed_df.head())
            print(f"[DEBUG] account_totals keys: {list(account_totals.keys())}")
            print(f"[DEBUG] month_totals keys: {list(month_totals.keys())}")
            # Validate post-transpose
            result = validate_post_transpose(transformed_df, file_type, (account_totals, month_totals))
            print(f"[DEBUG] validate_post_transpose result: {result}")
            if isinstance(result, tuple):
                is_valid_post, message_post = result
            else:
                is_valid_post = result
                message_post = ''
            if not is_valid_post:
                print(f"Post-transpose validation failed: {message_post}")
                return
            # Save the transformed data
            output_file = os.path.join(os.path.dirname(file_path), f"transformed_{os.path.basename(file_path)}")
            transformed_df.to_csv(output_file, index=False)
            print(f"Transformed data saved to: {output_file}")
            # Update account details
            if validate_and_update_accounts_from_file(output_file, entity):
                print("Account details updated successfully")
            else:
                print("Failed to update account details")
            # Update monthly spend
            if update_account_monthly_spend(output_file):
                print("Monthly spend updated successfully")
            else:
                print("Failed to update monthly spend")
            print(f"Successfully processed file: {file_path}")
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            traceback.print_exc()
        return

if __name__ == "__main__":
    main() 