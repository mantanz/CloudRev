import pandas as pd

def find_difference(file1, file2, output_file):
    """
    Find records that are in file1 but not in file2 based on account_id.
    
    Args:
        file1 (str): Path to the first file (A)
        file2 (str): Path to the second file (B)
        output_file (str): Path to save the result
    """
    try:
       
        
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)
        
        # Ensure account_id column exists in both files
        if 'account_id' not in df1.columns or 'account_id' not in df2.columns:
            raise ValueError("account_id column not found in one or both files")
        
        # Find records in A that are not in B
        result = df1[~df1['account_id'].isin(df2['account_id'])]
        
        # Save the result
        result.to_csv(output_file, index=False)
        print(f"Difference saved to {output_file}")
        print(f"Number of records in A-B: {len(result)}")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    # Example usage
     # Read the CSV files
    file1=input("Enter the path to the first file: ")
    file2=input("Enter the path to the second file: ")
    output_file = "../data_files/B-A_result.csv"  # Replace with your desired output file path
    
    find_difference(file1, file2, output_file)
