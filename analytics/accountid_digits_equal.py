import pandas as pd
import os

def process_account_ids(input_file, output_file):
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Find maximum length of account IDs
    max_length = 0
    for account_id in df['account_id']:
        # Convert to string first to get length
        str_id = str(account_id)
        if str_id.isdigit():  # Check if it's all digits
            length = len(str_id)
            max_length = max(max_length, length)
    
    # Process each account_id and collect summary information
    processed_ids = []
    padded_ids = []  # Store account_ids that were padded
    for account_id in df['account_id']:
        str_id = str(account_id)
        if str_id.isdigit():  # Check if it's all digits
            original_length = len(str_id)
            if original_length < max_length:
                padded_ids.append({
                    'original_id': str_id,
                    'original_length': original_length,
                    'new_length': max_length
                })
            # Convert back to string with leading zeros if needed
            processed_id = str_id.zfill(max_length)
        else:
            processed_id = account_id
        processed_ids.append(processed_id)
    
    # Update the DataFrame
    df['account_id'] = processed_ids
    
    # Save to new CSV file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)
    
    # Print summary of account IDs
    print("\nSummary of Account IDs:")
    print("-" * 50)
    print(f"Maximum length: {max_length}")
    print(f"Total padded account IDs: {len(padded_ids)}")
    
    if padded_ids:
        print("\nDetails of padded account IDs:")
        for i, padded_id in enumerate(padded_ids, 1):
            print(f"\n{i}. Account ID: {padded_id['original_id']}")
            print(f"   Original Length: {padded_id['original_length']}")
            print(f"   New Length: {padded_id['new_length']}")
            print(f"   Padded ID: {padded_id['original_id'].zfill(max_length)}")
    
    return max_length

if __name__ == "__main__":
    input_file = input("Enter the input file path: ")
    # Get the base name of the input file
    base_name = os.path.basename(input_file)
    # Create output file path in data_files directory
    output_file = os.path.join("../data_files", f"{os.path.splitext(base_name)[0]}_processed.csv")
    
    max_length = process_account_ids(input_file, output_file)
    print(f"\nMaximum length of account IDs: {max_length}")
    print(f"Processed data saved to: {output_file}")