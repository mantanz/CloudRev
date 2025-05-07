import os
import sys
from transform_aop import transform_aop_data
from db_operations import validate_and_update_accounts

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
        print("Invalid choice. Please enter 1, 2, or 3.")

def get_file_path():
    """Get the input file path from user"""
    while True:
        file_path = input("\nEnter the path to your input file: ").strip()
        if os.path.exists(file_path):
            return file_path
        print("File not found. Please enter a valid file path.")

def main():
    print("Welcome to CloudRev Data Processing Tool")
    
    while True:
        choice = get_user_choice()
        
        if choice == '3':
            print("\nThank you for using CloudRev Data Processing Tool. Goodbye!")
            sys.exit(0)
        
        if choice == '1':  # AOP processing
            file_path = get_file_path()
            try:
                # Transform AOP data
                transformed_file = transform_aop_data(file_path)
                
                # Validate and update accounts in database
                if transformed_file:
                    validate_and_update_accounts(transformed_file)
                
            except Exception as e:
                print(f"Error processing AOP file: {str(e)}")
        
        elif choice == '2':  # Spend processing
            print("\nSpend processing functionality will be implemented later.")
            continue

if __name__ == "__main__":
    main() 