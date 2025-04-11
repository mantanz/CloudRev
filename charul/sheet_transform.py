# Budget Data Transformation
# This code transforms budget data from wide format (months as columns) to long format
# Output includes AccountId, AccountName, Owner, Month, Amount columns

import pandas as pd
import numpy as np
import io

 # Get the uploaded filename
df = pd.read_csv("/Users/charulmauni/Downloads/parakh_actual_spend.csv")

# Step 3: Transform the data from wide to long format
# First, identify month columns and non-month columns
non_month_columns = ['account_id', 'account_name', 'owner']
month_columns = [col for col in df.columns if col not in non_month_columns]

# Create a new dataframe to store the transformed data
transformed_data = []

# Process each row in the original data
for _, row in df.iterrows():
    account_id = row['account_id']
    account_name = row['account_name']
    # owner = row['owner']

    # For each month column, create a new row
    for month in month_columns:
        amount = row[month]
        #if pd.notna(amount):  # Only include rows where amount is not NaN
        transformed_data.append({
                'account_id': account_id,
                'account_name': account_name,
                # 'owner': owner,
                'month': month,
                'spend': amount
            })

# Convert the list of dictionaries to a DataFrame
transformed_df = pd.DataFrame(transformed_data)

# Step 4: Display a sample of the transformed data
print("First 10 rows of transformed data:")
print(transformed_df.head(10))

# Step 5: Save the transformed data to a new CSV file
output_filename = 'transformed_budget_data.csv'
transformed_df.to_csv(output_filename, index=False)

# Step 6: Download the transformed CSV file
# files.download(output_filename)

print(f"\nTransformation complete! Downloaded as {output_filename}")
print(f"Total number of rows in transformed data: {len(transformed_df)}")