# Budget Data Transformation
# This code transforms budget data from wide format (months as columns) to long format
# Output includes AccountId, AccountName, Owner, Month, Amount columns

import pandas as pd
import numpy as np
import io

 # Get the uploaded filename
df = pd.read_csv("../data_files/Service-Wise.csv")

# Step 3: Transform the data from wide to long formats
# First, identify month columns and non-month columns

non_month_columns = ['service_id', 'service_name']
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
    service_id = row['service_id']
    service_name = row['service_name']
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
                'service_id': service_id,
                'service_name': service_name,
                # 'owner': owner,
                'month': date_str,
                'spend': amount
            })

# Convert the list of dictionaries to a DataFrame
transformed_df = pd.DataFrame(transformed_data)

# Convert the month column to datetime format
transformed_df['month'] = pd.to_datetime(transformed_df['month'])
# Clean and convert the 'spend' column to integers
# Remove commas and convert to float first, then to integer
transformed_df['spend'] = transformed_df['spend'].str.replace(',', '').astype(float).astype(int)
# Step 4: Display a sample of the transformed data
print("First 10 rows of transformed data:")
print(transformed_df.head(10))

# Step 5: Save the transformed data to a new CSV file
output = '../data_files/final_servicewise_data.csv'
transformed_df.to_csv(output, index=False)

# Step 6: Download the transformed CSV file
# files.download(output_filename)

print(f"\nTransformation complete! Downloaded as {output}")
print(f"Total number of rows in transformed data: {len(transformed_df)}")


