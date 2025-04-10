import pandas as pd

# Read the CSV file
df = pd.read_csv('sqlite/as_garima.csv')

# Select only required columns
df = df[['account_id', "Jan'25", "Feb'25"]]

# Melt the dataframe to transform it to long format
melted_df = df.melt(
    id_vars=['account_id'],
    var_name='month',
    value_name='spend'
)

# Clean up the spend column (remove commas)
melted_df['spend'] = melted_df['spend'].str.replace(',', '').astype(float)

# Transform month column to desired date format
month_mapping = {
    "Jan'25": '01-Jan-2025',
    "Feb'25": '01-Feb-2025'
}
melted_df['month'] = melted_df['month'].map(month_mapping)

# Save the transformed data
melted_df.to_csv('transformed_garima.csv', index=False)
print("Transformation complete. Output saved to 'transformed_garima.csv'")
