import pandas as pd
import os

def find_difference(file1, file2, output_file, column_name):
    """
    Find records that are in file1 but not in file2 based on specified column.
    
    Args:
        file1 (str): Path to the first file (A)
        file2 (str): Path to the second file (B)
        output_file (str): Path to save the result
        column_name (str): Column name to use for comparison
    """
    try:
        # Read the CSV files
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)
        
        # Print initial statistics
        print("\n=== Input File Statistics ===")
        print(f"Records in first file: {len(df1)}")
        print(f"Records in second file: {len(df2)}")
        
        # Ensure specified column exists in both files
        if column_name not in df1.columns or column_name not in df2.columns:
            raise ValueError(f"{column_name} column not found in one or both files")
        
        # Find records in A that are not in B
        result = df1[~df1[column_name].isin(df2[column_name])]
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Save the result
        result.to_csv(output_file, index=False)
        
        # Print results
        print("\n=== Results ===")
        print(f"Difference saved to: {output_file}")
        print(f"Number of records in A-B: {len(result)}")
        
        # Show sample of the result
        if len(result) > 0:
            print("\n=== Sample of Result (First 5 records) ===")
            print(result.head().to_string())
        else:
            print("\nNo differences found between the files based on the specified column.")
        
    except Exception as e:
        print(f"\nError: {str(e)}")

if __name__ == "__main__":
    # Get input from user
    file1 = input("Enter the path to the first file: ")
    file2 = input("Enter the path to the second file: ")
    column_name = input("Enter the column name to compare: ")
    
    # Create output file path in data_files directory
    base_name = os.path.basename(file1)
    output_file = os.path.join("../data_files", f"{os.path.splitext(base_name)[0]}_difference.csv")
    
    find_difference(file1, file2, output_file, column_name)
