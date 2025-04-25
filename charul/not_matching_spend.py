import pandas as pd
from datetime import datetime

def process_spend_data():
    # Read the CSV files
    df1 = pd.read_csv('sqlite/actual_spend_deepika.csv')
    df2 = pd.read_csv('sqlite/transformed_aws_accountwise.csv')
    
    # Get start time for statistics
    start_time = datetime.now()
    
    # Ensure both dataframes have the same column names for comparison
    df1 = df1.rename(columns={'spend': 'deepika_spend'})
    df2 = df2.rename(columns={'spend': 'parakh_spend'})
    
    # Merge the dataframes on account_id and month
    merged_df = pd.merge(df1, df2, on=['account_id', 'month'], how='inner')
    
    # Filter for records where spends don't match
    non_matching_df = merged_df[merged_df['deepika_spend'] != merged_df['parakh_spend']]
    # Drop the id and account_name columns
    non_matching_df = non_matching_df.drop(['id', 'account_name'], axis=1, errors='ignore')
    # Save the non-matching records to a new CSV
    output_file = 'non_matching_spend.csv'
    non_matching_df.to_csv(output_file, index=False)
    
    # Print summary statistics
    end_time = datetime.now()
    print(f"Processing completed in {end_time - start_time}")
    print(f"Total records processed: {len(merged_df)}")
    print(f"Non-matching records found: {len(non_matching_df)}")
    print(f"Results saved to: {output_file}")

if __name__ == "__main__":
    process_spend_data()