# Budget Data Transformation
# This code transforms budget data from wide format (months as columns) to long format
# Output includes AccountId, AccountName, Owner, Month, Amount columns

import pandas as pd
import numpy as np
import io

 # Get the uploaded filename
df = pd.read_csv("/Users/charulmauni/Downloads/AWS OCL Cost Apr'24 - Mar'25 - Account Wise.csv")

# Step 3: Transform the data from wide to long format
# First, identify month columns and non-month columns
non_month_columns = ['Account ID', 'Account Name']
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
    account_id = row['Account ID']
    account_name = row['Account Name']
    # owner = row['owner']

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
                # 'owner': owner,
                'month': date_str,
                'spend': amount
            })

# Convert the list of dictionaries to a DataFrame
transformed_df = pd.DataFrame(transformed_data)

# Convert the month column to datetime format
transformed_df['month'] = pd.to_datetime(transformed_df['month'])

# Step 4: Display a sample of the transformed data
print("First 10 rows of transformed data:")
print(transformed_df.head(10))

# Step 5: Save the transformed data to a new CSV file
output_filename = 'vishal_data.csv'
transformed_df.to_csv(output_filename, index=False)

# Step 6: Download the transformed CSV file
# files.download(output_filename)

print(f"\nTransformation complete! Downloaded as {output_filename}")
print(f"Total number of rows in transformed data: {len(transformed_df)}")