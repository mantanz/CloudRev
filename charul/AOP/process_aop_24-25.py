import pandas as pd
import numpy as np
from datetime import datetime

# Read the CSV file
df = pd.read_csv('AOP_24-25_final.csv')

# Calculate initial sum before any transformations
initial_sum = df.iloc[:, 2:].sum().sum()
print(f"\nInitial sum before transformation: {initial_sum:,.2f}")

# Get all month columns (columns 2 onwards)
month_columns = df.columns[2:]



# print("Data transformation completed successfully!") 
non_month_columns = ['account_id','account_name']
month_columns = [col for col in df.columns if col not in non_month_columns]

# Create a mapping of month abbreviations to month numbers
month_mapping = {
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
    'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
    'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
}

# Create a new dataframe to store the transformed data
transformed_data = []

# Process each row in the original data
for _, row in df.iterrows():
    account_id = row['account_id']
    account_name = row['account_name']
      

    # For each month column, create a new row
    for month in month_columns:
        amount = row[month]
        # Extract month and year from the column name (e.g., 'Apr-24')
        month_abbr, year = month.split('-')
        month_num = month_mapping[month_abbr]
        # Create date in YYYY-MM-DD format (using day=1)
        date_str = f"20{year}-{month_num}-01"
        
        transformed_data.append({
                'account_id': account_id,
                'account_name': account_name,
                'month': date_str,
                'aop_amount': amount
            })

# Convert the list of dictionaries to a DataFrame
transformed_df = pd.DataFrame(transformed_data)

# Convert account_id to string and handle padding for numerical IDs
def pad_account_id(account_id):
    if pd.isna(account_id):
        return account_id
    account_str = str(account_id)
    if account_str.isdigit():
        return account_str.zfill(12)  # Pad with zeros to length 12
    return account_str

# Apply padding only to numerical account_ids
transformed_df['account_id'] = transformed_df['account_id'].apply(pad_account_id)

# Show processing summary
print("\nProcessing Summary:")
print(f"Total rows in transformed data: {len(transformed_df)}")
print("\nSample of processed account_ids (showing first 20):")
print(transformed_df[['account_id', 'account_name']].head(20).to_string(index=False))

# Convert the month column to datetime format
transformed_df['month'] = pd.to_datetime(transformed_df['month'])
# Convert the 'aop_amount' column to float
transformed_df['aop_amount'] = transformed_df['aop_amount'].astype(float)

# Step 4: Display a sample of the transformed data
print("\nFirst 10 rows of transformed data:")
print(transformed_df.head(10))

# Calculate final sum after all transformations
final_sum = transformed_df['aop_amount'].sum()
print(f"\nFinal sum after all transformations: {final_sum:,.2f}")

# Validate if the sums match
if abs(initial_sum - final_sum) < 0.01:  # Using small epsilon for float comparison
    print("\nValidation successful: Sums match!")
else:
    print("\nValidation failed: Sums do not match!")
    print(f"Difference: {abs(initial_sum - final_sum):,.2f}")

# Step 5: Save the transformed data to a new CSV file
output = 'AOP_24-25_transformed.csv'
transformed_df.to_csv(output, index=False)

# Step 6: Download the transformed CSV file
# files.download(output_filename)

print(f"\nTransformation complete! Downloaded as {output}")
print(f"Total number of rows in transformed data: {len(transformed_df)}")
