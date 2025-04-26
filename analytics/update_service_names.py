import pandas as pd

# Read the mapping file
mapping_df = pd.read_csv('../data_files/service_name_mapping.csv')
# Create a dictionary for mapping
service_mapping = dict(zip(mapping_df['service_name'], mapping_df['new_service_name']))

# Read the daily cost data
cost_df = pd.read_csv('../data_files/Daily AWS Service Wise Cost - April 1-22.csv')

# Update service names using the mapping
cost_df['service_name'] = cost_df['service_name'].map(service_mapping).fillna(cost_df['service_name'])

# Save the updated data to a new file
cost_df.to_csv('../data_files/Daily AWS Service Wise Cost - April 1-22_name_updated.csv', index=False)

print("Service names have been updated and saved to 'Daily AWS Service Wise Cost - April 1-22_name_updated.csv'") 