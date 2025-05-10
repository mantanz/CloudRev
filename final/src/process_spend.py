import os
from update_account_service_daily_spend import update_account_service_daily_spend
from update_account_service_monthly_spend import update_account_service_monthly_spend
from update_account_monthly_spend import update_account_monthly_spend

def get_entity():
    """Get entity selection from user."""
    print("\nSelect entity:")
    print("1. OCL")
    print("2. PPSL")
    print("3. PIBPL")
    print("4. Nearbuy")
    print("5. PML")
    print("6. Creditmate")
    print("7. PaiPai")
    entity_option = input("Enter option (default: 1): ").strip() or "1"
    
    entity_mapping = {
        '1': 'OCL',
        '2': 'PPSL',
        '3': 'PIBPL',
        '4': 'Nearbuy',
        '5': 'PML',
        '6': 'Creditmate',
        '7': 'PaiPai'
    }
    
    entity = entity_mapping.get(entity_option)
    if not entity:
        print("Invalid entity option")
        return None
    return entity

def process_spend_data():
    """Main function to process spend data."""
    try:
        # Get entity
        entity = get_entity()
        if not entity:
            return
            
        # Get file type
        print("\nSelect file type:")
        print("1. Account Service Daily")
        print("2. Account Service Monthly")
        print("3. Account Monthly")
        file_type = input("Enter option (default: 1): ").strip() or "1"
        file_type = int(file_type)
        
        # Get file path
        path = input("\nEnter file or directory path: ").strip()
        
        if not os.path.exists(path):
            print("File or directory not found!")
            return
            
        # Process files
        if os.path.isdir(path):
            for file in os.listdir(path):
                if file.endswith('.csv'):
                    process_single_file(os.path.join(path, file), file_type, entity)
        else:
            process_single_file(path, file_type, entity)
            
    except Exception as e:
        print(f"Error processing spend data: {str(e)}")

def process_single_file(file_path, file_type, entity):
    """Process a single spend file."""
    try:
        if file_type == 1:  # Account Service Daily
            update_account_service_daily_spend(file_path, entity)
        elif file_type == 2:  # Account Service Monthly
            update_account_service_monthly_spend(file_path, entity)
        else:  # Account Monthly
            update_account_monthly_spend(file_path, entity)
            
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")

if __name__ == "__main__":
    process_spend_data() 