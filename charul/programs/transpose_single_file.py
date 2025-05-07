import os
import pandas as pd
from pathlib import Path

def transpose_files(file_path):
  
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Get the first column name (should be 'Service')
            first_col = df.columns[0]
            # Remove row where first column contains 'Service total' (case insensitive)
            df = df[~df[first_col].str.contains('Service total', case=False, na=False)]
            # Transpose the dataframe
            transposed_df = df.transpose()
          
            # Create output filename (keeping original name)
            output_path = "../data_files/nearbuy_daily_spend_transpose.csv"
            
            # Save the transposed dataframe
            transposed_df.to_csv(output_path, header=False)
            print(f"Successfully transposed {output_path}")
            

if __name__ == "__main__":
    # Example usage
    file_path = "../data_files/nearbuy_daily_spend.csv"  # Folder containing original CSV files
    
    transpose_files(file_path)

