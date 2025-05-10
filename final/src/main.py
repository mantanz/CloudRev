import os
import pandas as pd
from update_account_details import validate_and_update_accounts_from_file
from update_account_service_daily_spend import update_account_service_daily_spend
from update_account_service_monthly_spend import update_account_service_monthly_spend
from update_account_monthly_spend import update_account_monthly_spend

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
    """Main function to handle user interaction"""
    print("\nWelcome to CloudRev Data Processing Tool")
    
    while True:
        choice = get_user_choice()
        
        if choice == 3:  # Exit
            print("\nThank you for using CloudRev Data Processing Tool. Goodbye!")
            break
            
        elif choice == 1:  # AOP
            file_path = input("\nEnter file or directory path: ").strip()
            if os.path.exists(file_path):
                if os.path.isfile(file_path):
                    validate_and_update_accounts_from_file(file_path)
                else:
                    for file in os.listdir(file_path):
                        if file.endswith('.csv'):
                            full_path = os.path.join(file_path, file)
                            validate_and_update_accounts_from_file(full_path)
            else:
                print(f"Path does not exist: {file_path}")
                
        elif choice == 2:  # Spend
            entity = get_entity_choice()
            file_type = get_file_type_choice()
            file_path = input("\nEnter file or directory path: ").strip()
            
            if os.path.exists(file_path):
                if os.path.isfile(file_path):
                    if file_type == 1:
                        update_account_service_daily_spend(file_path, entity)
                    elif file_type == 2:
                        update_account_service_monthly_spend(file_path, entity)
                    else:
                        update_account_monthly_spend(file_path, entity)
                else:
                    for file in os.listdir(file_path):
                        if file.endswith('.csv'):
                            full_path = os.path.join(file_path, file)
                            if file_type == 1:
                                update_account_service_daily_spend(full_path, entity)
                            elif file_type == 2:
                                update_account_service_monthly_spend(full_path, entity)
                            else:
                                update_account_monthly_spend(full_path, entity)
            else:
                print(f"Path does not exist: {file_path}")

if __name__ == "__main__":
    main() 