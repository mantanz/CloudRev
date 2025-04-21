import os
import pandas as pd
from pathlib import Path

def transpose_files(source_folder, destination_folder):
    """
    Transpose all CSV files from source folder and save them in destination folder.
    
    Args:
        source_folder (str): Path to the folder containing source files
        destination_folder (str): Path to the folder where transposed files will be saved
    """
    # Create destination folder if it doesn't exist
    os.makedirs(destination_folder, exist_ok=True)
    
    # Get all CSV files from source folder
    source_path = Path(source_folder)
    csv_files = list(source_path.glob('*.csv'))
    
    for file_path in csv_files:
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Get the first column name (should be 'Service')
            first_col = df.columns[0]
            # Remove row where first column contains 'Service total' (case insensitive)
            df = df[~df[first_col].str.contains('Service total', case=False, na=False)]
            # Transpose the dataframe
            transposed_df = df.transpose()
          
            # Create output filename (keeping original name)
            output_path = os.path.join(destination_folder, file_path.name)
            
            # Save the transposed dataframe
            transposed_df.to_csv(output_path, header=False)
            print(f"Successfully transposed {file_path.name}")
            
        except Exception as e:
            print(f"Error processing {file_path.name}: {str(e)}")

if __name__ == "__main__":
    # Example usage
    source_folder = "../data_files/monthly_spend"  # Folder containing original CSV files
    destination_folder = "../data_files/monthly_transposed_data"  # Folder for transposed files
    
    transpose_files(source_folder, destination_folder)

