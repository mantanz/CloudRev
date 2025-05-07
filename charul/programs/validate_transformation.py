import pandas as pd
import os
from pathlib import Path

def compare_csv_files(original_file, transformed_file):
    # Read both CSV files
    df_original = pd.read_csv(original_file)
    df_transformed = pd.read_csv(transformed_file)
    
 
    
    return {
        'file_name': os.path.basename(original_file),
        'original_rows': len(df_original),
        'transformed_rows': len(df_transformed),
        'original_columns': list(df_original.columns),
        'transformed_columns': list(df_transformed.columns)
    }

def main():
    original_dir = Path('service_with_accountid')
    transformed_dir = Path('transformed_service')
    
    results = []
    
    # Get all CSV files from original directory
    for original_file in original_dir.glob('*.csv'):
        transformed_file = transformed_dir / original_file.name
        
        if transformed_file.exists():
            result = compare_csv_files(original_file, transformed_file)
            results.append(result)
    
    # Generate summary report
    with open('transformation_validation_summary.txt', 'w') as f:
        f.write("Data Transformation Validation Summary\n")
        f.write("=====================================\n\n")
        
        for result in results:
            f.write(f"File: {result['file_name']}\n")
            f.write("-" * 50 + "\n")
         
            f.write(f"Original Rows: {result['original_rows']}\n")
            f.write(f"Transformed Rows: {result['transformed_rows']}\n")
            
           
            f.write("\nColumn Differences:\n")
            f.write(f"Original Columns: {result['original_columns']}\n")
            f.write(f"Transformed Columns: {result['transformed_columns']}\n")
            
            f.write("\n" + "=" * 50 + "\n\n")
        
        # Overall summary

        f.write("\nOverall Summary\n")
        f.write("--------------\n")
       
        f.write(f"Total files compared: {len(results)}\n")
        f.write(f"Total rows including all files: {sum(r['transformed_rows'] for r in results)}\n")
if __name__ == "__main__":
    main() 