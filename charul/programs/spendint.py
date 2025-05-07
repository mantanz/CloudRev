import pandas as pd

# Read the CSV file
df = pd.read_csv('../data_files/nearbuy_monthly_spend.csv')



# Clean up the spend column (remove commas)
df['spend'] = df['spend'].str.replace(',', '').astype(float)



# Save the transformed data
df.to_csv('../data_files/nearbuy_monthly_spend.csv', index=False)
print("Transformation complete. Output saved to 'nearbuy_monthly_spend.csv'")
