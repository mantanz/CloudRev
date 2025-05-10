import pandas as pd
import os
from pathlib import Path

# Read the reference CSV
reference_df = pd.read_csv('reference.csv')
print("Reference Data:")
print(reference_df.head())

# Process CSV files from a folder
folder_path = '/Users/charulmauni/Desktop/CloudRev/vishal_data'


# Process each CSV file in the folder
for file in Path(folder_path).glob('*.csv'):
    # Extract filename without extension and clean whitespace
    service_name = file.stem.strip()
    print(f"\nProcessing file: {service_name}")
    
    # Get the account_id from reference_df based on service_name
    try:
        account_id = reference_df.loc[reference_df['account_name'].str.strip() == service_name, 'account_id'].values[0]
        print(f"Found account_id: {account_id}")
        
        # Read the CSV file
        service_df = pd.read_csv(file)
        
        # Add account_id column to the service DataFrame
        service_df['account_id'] = account_id
        
        # Move account_id to the first position
        cols = list(service_df.columns)
        cols.insert(0, cols.pop(cols.index('account_id')))
        service_df = service_df[cols]
        
        # Save the updated DataFrame back to the file
        service_df.to_csv(f"/Users/charulmauni/Desktop/CloudRev/service_with_accountid/{service_name}.csv", index=False)
        print(f"\nUpdated file saved to: {service_name}.csv")
        
    except IndexError:
        print(f"Warning: No account_id found for service {service_name}")
    except Exception as e:
        print(f"Error processing {service_name}: {str(e)}")

