import pandas as pd
import os

def merge_records():
    # Define file paths
    transformed_path = 'sqlite/transformed_aws_accountwise.csv'
    actual_path = 'sqlite/actual_spend_deepika.csv'
    output_path = 'sqlite/merged_actual_spend.csv'
    
    # Read both files
    try:
        df_transformed = pd.read_csv(transformed_path)
        df_actual = pd.read_csv(actual_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return

    # Define the columns we want to keep (excluding id)
    relevant_columns = ['account_id', 'month', 'spend', 'resource_type']

    # Find records in transformed that are not in actual based on account_id and month
    df_to_add = df_transformed[~df_transformed.set_index(['account_id', 'month']).index.isin(
        df_actual.set_index(['account_id', 'month']).index
    )]

    # If there are records to add
    if not df_to_add.empty:
        # Add resource_type column if it's missing
        if 'resource_type' not in df_to_add.columns:
            df_to_add['resource_type'] = 'compute'  # Default value matching existing data
        
        # Keep only the relevant columns
        df_to_add = df_to_add[relevant_columns]
        
        # Append to actualspenddeepika
        df_merged = pd.concat([df_actual, df_to_add], ignore_index=True)
        
        # Save to new file
        df_merged.to_csv(output_path, index=False)
        print(f"Created new file: {output_path}")
        print(f"Added {len(df_to_add)} new records")
    else:
        print("No new records to add")

if __name__ == "__main__":
    merge_records()