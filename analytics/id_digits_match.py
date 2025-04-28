import pandas as pd
import os

def process_id_columns(input_file, output_file):
    # Read the CSV file
    df = pd.read_csv(input_file, dtype=str)
    
    # Check which columns exist
    target_columns = ['hod_id', 'account_id']
    existing_columns = [col for col in target_columns if col in df.columns]
    
    if not existing_columns:
        raise ValueError("Neither 'hod_id' nor 'account_id' columns found in the input file")
    
    # Find maximum length for each column independently
    max_lengths = {}  # Store max length for each column
    for column in existing_columns:
        max_lengths[column] = 0
        for id_value in df[column]:
            str_id = str(id_value)
            if str_id.isdigit():  # Check if it's all digits
                length = len(str_id)
                max_lengths[column] = max(max_lengths[column], length)
    
    all_padded_ids = {}  # Store padded IDs for each column
    
    # Process each column with its own max length
    for column in existing_columns:
        processed_ids = []
        padded_ids = []  # Store account_ids that were padded
        column_max_length = max_lengths[column]
        
        for id_value in df[column]:
            str_id = str(id_value)
            if str_id.isdigit():  # Check if it's all digits
                original_length = len(str_id)
                if original_length < column_max_length:
                    padded_ids.append({
                        'original_id': str_id,
                        'original_length': original_length,
                        'new_length': column_max_length
                    })
                # Convert back to string with leading zeros if needed
                processed_id = str_id.zfill(column_max_length)
            else:
                processed_id = id_value
            processed_ids.append(processed_id)
        
        # Update the DataFrame
        df[column] = processed_ids
        all_padded_ids[column] = padded_ids
    
    # Save to new CSV file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)
    
    # Print summary for all processed columns
    print("\nProcessing Summary:")
    print("-" * 50)
    
    for column in existing_columns:
        padded_ids = all_padded_ids[column]
        print(f"\nSummary of {column}:")
        print(f"Maximum length for {column}: {max_lengths[column]}")
        print(f"Total padded IDs: {len(padded_ids)}")
        
        if padded_ids:
            print(f"\nDetails of padded {column}:")
            for i, padded_id in enumerate(padded_ids, 1):
                print(f"\n{i}. {column}: {padded_id['original_id']}")
                print(f"   Original Length: {padded_id['original_length']}")
                print(f"   New Length: {padded_id['new_length']}")
                print(f"   Padded ID: {padded_id['original_id'].zfill(max_lengths[column])}")
    
    return max_lengths, existing_columns

if __name__ == "__main__":
    input_file = input("Enter the input file path: ")
    # Get the base name of the input file
    base_name = os.path.basename(input_file)
    # Create output file path in data_files directory
    output_file = os.path.join("../data_files", f"{os.path.splitext(base_name)[0]}_processed.csv")
    
    try:
        max_lengths, processed_columns = process_id_columns(input_file, output_file)
        print(f"\nProcessed columns: {', '.join(processed_columns)}")
        for column in processed_columns:
            print(f"Maximum length for {column}: {max_lengths[column]}")
        print(f"Processed data saved to: {output_file}")
    except ValueError as e:
        print(f"Error: {e}")