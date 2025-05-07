# Budget Data Transformation
# This code transforms budget data from wide format (months as columns) to long format
# Output includes AccountId, AccountName, Owner, Month, Amount columns

import pandas as pd
import numpy as np
import io

# Get the uploaded filename
df = pd.read_csv("../data_files/monthly_spend_apr_transposed.csv")

# Step 3: Transform the data from wide to long format
# First, identify month columns and non-month columns
non_month_columns = ['account_id', 'account_name']
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
        # Convert amount to string first to handle any commas or special characters
        amount = str(row[month])
        # Remove commas and convert to numeric
        amount = amount.replace(',', '')
        try:
            amount = float(amount)
        except ValueError:
            amount = 0.0  # Set to 0 if conversion fails
            
        # Extract month and year from the column name (e.g., '2025-04-01')
        date_parts = month.split('-')
        if len(date_parts) == 3:  # Full date format
            year = date_parts[0]
            month_num = date_parts[1]
            date_str = month
        else:  # Handle other formats if needed
            date_str = month
            
        transformed_data.append({
            'account_id': account_id,
            'account_name': account_name,
            'month': date_str,
            'spend': amount 
        })

# Convert the list of dictionaries to a DataFrame
transformed_df = pd.DataFrame(transformed_data)

# Convert the month column to datetime format
transformed_df['month'] = pd.to_datetime(transformed_df['month'])

# Ensure spend column is numeric and round to 2 decimal places
transformed_df['spend'] = pd.to_numeric(transformed_df['spend'], errors='coerce').fillna(0).round(4)

# Step 4: Display a sample of the transformed data
print("First 10 rows of transformed data:")
print(transformed_df.head(10))

# Step 5: Save the transformed data to a new CSV file
output_filename = '../data_files/monthly_spend_apr_final.csv'
transformed_df.to_csv(output_filename, index=False)

# Step 6: Download the transformed CSV file
# files.download(output_filename)

print(f"\nTransformation complete! Saved as {output_filename}")
print(f"Total number of rows in transformed data: {len(transformed_df)}")