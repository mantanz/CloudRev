import pandas as pd

# Read both CSV files
hod_details = pd.read_csv('../data_files/hod_details_updated_processed.csv')
account_details = pd.read_csv('../data_files/final_account_details.csv')

# Create a dictionary mapping hod_name to hod_id
hod_mapping = dict(zip(hod_details['hod_name'], hod_details['hod_id']))

# Add hod_id column to account_details
account_details['hod_id'] = account_details['hod_name'].map(hod_mapping)

# Save the updated account details
account_details.to_csv('../data_files/final_account_details_updated.csv', index=False)

print("Successfully added hod_id column to final_account_details_updated.csv")