import pandas as pd
import numpy as np
from datetime import datetime

# Read the CSV file
df = pd.read_csv('AOP_25-26.csv')

# # Calculate total sum before transformation
# original_sum = df[df.columns[1:]].sum().sum()
# print(f"\nTotal sum before transformation: {original_sum:,.2f}")

# Function to convert K values to actual numbers and handle nulls
def convert_k_to_number(value):
    if pd.isna(value) or value == '' or str(value).strip() == '-':
        return 0
    if isinstance(value, str) and 'K' in value:
        return float(value.replace('K', '').strip()) * 1000
    try:
        return float(str(value).strip())
    except ValueError:
        return 0


# Convert all month columns to numeric values
month_columns = df.columns[1:]  # All columns except account_id
for col in month_columns:
    df[col] = df[col].apply(convert_k_to_number)

# Calculate sum after K conversion
after_k_conversion_sum = df[month_columns].sum().sum()
print(f"Total sum after K conversion: {after_k_conversion_sum:,.2f}")

# print("Data transformation completed successfully!") 
non_month_columns = ['account_id']
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
                'month': date_str,
                'aop_amount': amount
            })

# Convert the list of dictionaries to a DataFrame
transformed_df = pd.DataFrame(transformed_data)

# Drop rows with null values in account_id
transformed_df = transformed_df.dropna(subset=['account_id'])

# Identify and drop non-numerical account_ids
numerical_accounts = transformed_df['account_id'].astype(str).str.match(r'^\d+$')
transformed_df = transformed_df[numerical_accounts]

# Find the maximum length of numerical account_ids
max_length = transformed_df['account_id'].astype(str).str.len().max()
print(f"\nMaximum numerical account_id length: {max_length}")

# Pad account_ids with leading zeros
transformed_df['account_id'] = transformed_df['account_id'].astype(str).str.zfill(max_length)

# Show processing summary
print("\nProcessing Summary:")
print(f"Total rows after dropping null and non-numerical account_ids: {len(transformed_df)}")
print("\nSample of processed account_ids (showing first 20):")
print(transformed_df[['account_id']].head(20).to_string(index=False))

# Convert the month column to datetime format
transformed_df['month'] = pd.to_datetime(transformed_df['month'])
# Convert the 'aop_amount' column to float and round to 1 decimal place
transformed_df['aop_amount'] = transformed_df['aop_amount'].astype(float).round(1)

# Calculate final sum after transformation
final_sum = transformed_df['aop_amount'].sum()
print(f"Total sum after transformation: {final_sum:,.2f}")

# Validate if the sums match (allowing for small rounding differences)
if abs(final_sum - after_k_conversion_sum) < 0.01:
    print("\nData validation successful: Sums match!")
else:
    print(f"\nWarning: Sum mismatch detected!")
    print(f"Difference: {abs(final_sum - after_k_conversion_sum):,.2f}")

# Handle duplicates by keeping the last occurrence
print("\nChecking for duplicates...")
duplicates = transformed_df.duplicated(subset=['account_id', 'month'], keep=False)
if duplicates.any():
    print(f"Found {duplicates.sum()} duplicate rows")
    print("\nComplete list of duplicate rows:")
    duplicate_df = transformed_df[duplicates].sort_values(['account_id', 'month'])
    print(duplicate_df.to_string(index=False))
    transformed_df = transformed_df.drop_duplicates(subset=['account_id', 'month'], keep='last')
    print(f"\nRemoved duplicates. Remaining rows: {len(transformed_df)}")

# Step 4: Display a sample of the transformed data
print("\nFirst 10 rows of transformed data:")
print(transformed_df.head(10))

# Step 5: Save the transformed data to a new CSV file
output = 'AOP_25-26_transformed_new.csv'
transformed_df.to_csv(output, index=False)

# Step 6: Download the transformed CSV file
# files.download(output_filename)

print(f"\nTransformation complete! Downloaded as {output}")
print(f"Total number of rows in transformed data: {len(transformed_df)}")
