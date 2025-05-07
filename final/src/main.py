import os
from transform_aop import transform_aop_data
from update_account_details import validate_and_update_accounts
from update_aop_budget import update_aop_budget_monthly

def get_user_choice():
    """Get user's choice for file type"""
    while True:
        print("\nPlease select the type of file to process:")
        print("1. AOP (Annual Operating Plan)")
        print("2. Spend")
        print("3. Exit")
        
        choice = input("\nEnter your choice (1-3): ").strip()
        
        if choice in ['1', '2', '3']:
            return choice
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

def get_input_file():
    """Get input file path from user"""
    while True:
        file_path = input("\nEnter the path to your input file: ").strip()
        
        if os.path.isfile(file_path):
            return file_path
        else:
            print("File not found. Please enter a valid file path.")

def process_aop():
    """Process AOP file"""
    # Step 1: Transform AOP data
    input_file = get_input_file()
    transformed_file = transform_aop_data(input_file)
    
    if not transformed_file:
        print("AOP transformation failed. Please check the input file and try again.")
        return
    
    # Step 2: Validate and update account details
    if not validate_and_update_accounts(transformed_file):
        print("Account validation and update failed. Please check the database connection.")
        return
    
    # Step 3: Update AOP budget monthly table
    if not update_aop_budget_monthly(transformed_file):
        print("AOP budget monthly table update failed. Please check the database connection.")
        return
    
    print("\nAOP processing completed successfully!")

def main():
    """Main function"""
    print("Welcome to CloudRev Data Processing Tool")
    
    while True:
        choice = get_user_choice()
        
        if choice == '1':
            process_aop()
        elif choice == '2':
            print("\nSpend processing not implemented yet.")
        else:
            print("\nThank you for using CloudRev Data Processing Tool. Goodbye!")
            break

if __name__ == "__main__":
    main() 