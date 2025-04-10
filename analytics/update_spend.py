import pandas as pd
from datetime import datetime

# Read both CSV files
actual_df = pd.read_csv('sqlite/actual_spend.csv')
transformed_df = pd.read_csv('transformed_garima.csv')

# Convert the date format in transformed_df to match actual_df
transformed_df['month'] = pd.to_datetime(transformed_df['month'], format='%d-%b-%Y').dt.strftime('%Y-%m-%d')

# Create a dictionary from transformed_df for quick lookup
transformed_spend_dict = transformed_df.set_index(['account_id', 'month'])['spend'].to_dict()

# Update spend values in actual_df where matches are found
def update_spend(row):
    key = (row['account_id'], row['month'])
    return transformed_spend_dict.get(key, row['spend'])

actual_df['spend'] = actual_df.apply(update_spend, axis=1)

# Save the updated data back to actual_spend.csv
actual_df.to_csv('sqlite/actual_spend_1.csv', index=False)
print("Update complete. Values in actual_spend.csv have been updated with matching values from transformed_garima.csv")
