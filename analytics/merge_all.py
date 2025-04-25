import pandas as pd
import os
from pathlib import Path

def clean_spend_value(value):
    # Handle NaN values
    if pd.isna(value):
        return 0
    if isinstance(value, str):
        # Remove commas and convert to float first to handle decimal points if any
        return int(float(value.replace(',', '')))
    return int(value)

def merge_csv_files():
    # Specify the directory containing the CSV files
    directory = Path("/Users/charulmauni/Desktop/CloudRev/transformed_service")
    
    # Get all CSV files in the directory
    csv_files = list(directory.glob("*.csv"))
    
    if not csv_files:
        print("No CSV files found in the directory!")
        return
    
    # Initialize an empty list to store DataFrames
    dataframes = []
    sum=0
    # Read each CSV file and add to the list
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            
            # Convert spend column to integer
            if 'spend' in df.columns:
                df['spend'] = df['spend'].apply(clean_spend_value)
                sum+=df['spend'].sum()
            dataframes.append(df)
            print(f"Successfully read and processed: {file.name}")
        except Exception as e:
            print(f"Error reading {file.name}: {str(e)}")
    
    if not dataframes:
        print("No valid CSV files found!")
        return
    
    try:
        # Merge all DataFrames
        merged_df = pd.concat(dataframes, ignore_index=True)
        
        # Save the merged DataFrame to a new CSV file
        output_file = "/Users/charulmauni/Desktop/CloudRev/data_files/merged_service_data.csv"
        merged_df.to_csv(output_file, index=False)
        print(f"Successfully merged {len(csv_files)} files into {output_file}")
    except Exception as e:
        print(f"Error merging files: {str(e)}")
    print(f"Total spend from all files: {sum}")
    print(f"Total spend from merged dataframe: {merged_df['spend'].sum()}")
if __name__ == "__main__":
    merge_csv_files()