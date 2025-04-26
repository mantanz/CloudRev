import pandas as pd

# Read both CSV files
hod_file=input("Enter the path to the hod_details file: ")
people_file=input("Enter the path to the people_details file: ")

hod_df = pd.read_csv(hod_file)
people_df = pd.read_csv(people_file)

print("\n=== Data Validation Report ===")
print(f"Total HOD records in hod_details_updated.csv: {len(hod_df)}")
print(f"Total records in people_details.csv: {len(people_df)}")

# Create a dictionary mapping hod_name to hod_id
hod_mapping = dict(zip(hod_df['hod_name'], hod_df['hod_id']))

# Add hod_id column to people_df if it doesn't exist
if 'hod_id' not in people_df.columns:
    people_df['hod_id'] = None

# Update hod_id based on matching hod_name
people_df['hod_id'] = people_df['hod_name'].map(hod_mapping)

# Fill entity column with 'OCL'
#people_df['entity'] = 'OCL'

# Calculate statistics
total_records = len(people_df)
matched_records = people_df['hod_id'].notna().sum()
unmatched_records = total_records - matched_records

print(f"\n=== Matching Results ===")
print(f"Total records processed: {total_records}")
print(f"Successfully matched HOD IDs: {matched_records}")
print(f"Unmatched HOD names: {unmatched_records}")

# Show unmatched HOD names
unmatched_hods = people_df[people_df['hod_id'].isna()]['hod_name'].unique()
if len(unmatched_hods) > 0:
    print("\n=== Unmatched HOD Names ===")
    for hod in unmatched_hods:
        print(f"- {hod}")

# Show sample of updated data
print("\n=== Sample of Updated Data (First 5 records) ===")
print(people_df[['emp_name', 'hod_name', 'hod_id', 'entity']].head().to_string())

people_df=people_df[['emp_id','emp_name','email_id','L1_mgr_id','L1_mgr_name','hod_id','tech_product','entity','business_id','percentage']]

# Save the updated people_details file
people_df.to_csv('../data_files/people_details_updated.csv', index=False)

print("\nHOD IDs have been added and entity column has been updated with 'OCL' in people_details_updated.csv successfully!")
