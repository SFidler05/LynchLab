import os

folder_path = '/Users/samfidler/Desktop/Lab Project/Results/'

def search_string_in_files_with_context(folder_path, search_string, lines_of_interest):
    """
    Search for a given string in all files within a folder and return specific lines of interest.

    Args:
        folder_path (str): Path to the folder to search in.
        search_string (str): The string to search for.
        lines_of_interest (list): List of integers indicating relative line positions to include.

    Returns:
        dict: A dictionary where keys are file paths and values are lists of matched lines with context.
    """
    results = {}

    # Walk through all files in the folder
    for root, _, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                # Open and read the file line by line
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    for i, line in enumerate(lines):
                        if search_string in line:
                            # Capture only the specified lines of interest
                            context = []
                            for offset in lines_of_interest:
                                target_index = i + offset
                                if 0 <= target_index < len(lines):  # Ensure the index is within bounds
                                    context.append(lines[target_index].strip())
                            if file_path not in results:
                                results[file_path] = []
                            results[file_path].append(context)
            except (UnicodeDecodeError, PermissionError):
                # Skip files that cannot be read
                pass

    return results

if __name__ == "__main__":
    search_string = input("Enter the animal ID: ")
    # Example: Specify lines of interest (e.g., -2 for 2 lines before, 0 for the matched line, 3 for 3 lines after)
    lines_of_interest = [-2, 3]

    result = search_string_in_files_with_context(folder_path, search_string, lines_of_interest)

    if result:
        print("The ID was found with the associated data:")
        for file, contexts in result.items():
            print(f"\nRoom: {str(file)[45:49]}")
            for context in contexts:
                print("-----------------")
                for line in context:
                    print(line)
                print("-----------------")
    else:
        print("The ID was not found in any files.")
        
        
        