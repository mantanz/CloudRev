import pandas as pd

# Read the CSV files
hod_df = pd.read_csv('../data_files/hod_details.csv')
people_df = pd.read_csv('../data_files/people_details.csv')

# Data Validation
print("\n=== Data Validation ===")
print(f"Number of records in hod_details: {len(hod_df)}")
print(f"Number of records in people_details: {len(people_df)}")
print("\nColumns in hod_details:", hod_df.columns.tolist())
print("Columns in people_details:", people_df.columns.tolist())

# Convert column names to lowercase for case-insensitive matching
hod_df.columns = hod_df.columns.str.lower()
people_df.columns = people_df.columns.str.lower()

# Create a mapping dictionary from people_details
name_mapping = dict(zip(people_df['emp_name'], people_df['hod_name']))

# Show sample of name mappings
print("\n=== Sample Name Mappings ===")
sample_mappings = list(name_mapping.items())[:5]
for emp_name, hod_name in sample_mappings:
    print(f"Employee: {emp_name} -> HOD: {hod_name}")

# Update tech_leader in hod_details where names match
hod_df['tech_leader'] = hod_df['hod_name'].map(name_mapping)

# Fill tech_leader with hod_name where there's no match
hod_df['tech_leader'] = hod_df['tech_leader'].fillna(hod_df['hod_name'])

# Show matching results
print("\n=== Matching Results ===")
print(f"Number of records with matching HOD names: {hod_df['tech_leader'].notna().sum()}")
print(f"Number of records without matches: {hod_df['tech_leader'].isna().sum()}")

# Display sample of updated records
print("\n=== Sample Updated Records ===")
sample_updated = hod_df[['hod_name', 'tech_leader']].head(5)
print(sample_updated)

# Save the updated dataframe back to CSV
hod_df.to_csv('../data_files/hod_details_updated.csv', index=False)

print("\nProcessing completed. Updated file saved as hod_details_updated.csv")
