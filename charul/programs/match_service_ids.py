import pandas as pd

# Read the service details file
service_details = pd.read_csv('../data_files/service_details_new.csv')

# Read the daily cost file
daily_cost = pd.read_csv('../data_files/Daily AWS Service Wise Cost - April 1-22_name_updated.csv')

# Create a mapping dictionary from service name to service ID
service_id_map = dict(zip(service_details['service_name'], service_details['service_id']))

# Add service_id column to daily cost and convert to integer
daily_cost['service_id'] = daily_cost['service_name'].map(service_id_map).astype('Int64')

# Data Validation
print("\n=== Data Validation Results ===")
print(f"Total number of rows in daily cost file: {len(daily_cost)}")
print(f"Number of unique service names: {daily_cost['service_name'].nunique()}")
print(f"Number of matched services: {daily_cost['service_id'].notna().sum()}")
print(f"Number of unmatched services: {daily_cost['service_id'].isna().sum()}")

# Show unmatched services if any
unmatched_services = daily_cost[daily_cost['service_id'].isna()]['service_name'].unique()
if len(unmatched_services) > 0:
    print("\nUnmatched service names:")
    for service in unmatched_services:
        print(f"- {service}")

# Show service ID statistics
print("\nService ID Statistics:")
print(f"Min service ID: {daily_cost['service_id'].min()}")
print(f"Max service ID: {daily_cost['service_id'].max()}")
print(f"Number of unique service IDs: {daily_cost['service_id'].nunique()}")
daily_cost=daily_cost[['account_id','day','spend','service_id']]

# Save the updated file
daily_cost.to_csv('../data_files/Daily AWS Service Wise Cost - April 1-22_with_ids.csv', index=False)

print("\nService IDs have been matched and added to the daily cost file as integers.") 