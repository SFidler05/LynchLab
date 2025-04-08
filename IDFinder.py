import os
from openpyxl import Workbook  # Import openpyxl for Excel file creation

# Dynamically determine the folder path for "Data"
script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the script
folder_path = os.path.join(script_dir, "Data")  # Path to the "Data" folder

# Input file path containing the list of animal IDs
id_file_path = input("Enter the file path containing the list of IDs: ").strip().strip("'").strip('"')

# Debugging: Print the file path
print(f"Debug: Checking file path '{id_file_path}'")

# Read the IDs from the file
try:
    with open(id_file_path, 'r', encoding='utf-8') as id_file:
        ID_list = [line.strip() for line in id_file.readlines()]  # Read and strip each line
except FileNotFoundError:
    print("Error: The specified file was not found.")
    ID_list = []
except PermissionError:
    print("Error: Permission denied for the specified file.")
    ID_list = []

def search_string_in_files_with_context(folder_path, search_string):
    """
    Search for a given string in all files within a folder and copy lines from two lines above the match
    until three consecutive empty lines are encountered.

    Args:
        folder_path (str): Path to the folder to search in.
        search_string (str): The string to search for.

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
                            # Start copying from two lines above the match
                            start_index = max(0, i - 2)
                            context = []
                            empty_line_count = 0

                            for j in range(start_index, len(lines)):
                                current_line = lines[j].strip()

                                # Exclude lines that contain "File:" (case-insensitive)
                                if not current_line.lower().startswith("file:"):
                                    context.append(current_line)

                                # Check for consecutive empty lines
                                if current_line == "":
                                    empty_line_count += 1
                                else:
                                    empty_line_count = 0

                                if empty_line_count == 3:  # Stop after three consecutive empty lines
                                    break

                            if file_path not in results:
                                results[file_path] = []
                            results[file_path].append(context)
            except (UnicodeDecodeError, PermissionError):
                # Skip files that cannot be read
                print(f"Error reading file: {file_path}")
                pass

    return results


if __name__ == "__main__":
    if not ID_list:
        print("No IDs found in the specified file.")
    else:
        # Create a new Excel workbook
        workbook = Workbook()
        workbook.remove(workbook.active)  # Remove the default sheet

        for search_string in ID_list:
            print(f"\nSearching for ID: {search_string}")
            result = search_string_in_files_with_context(folder_path, search_string)

            if result:
                # Create a new sheet for the current ID
                sheet = workbook.create_sheet(title=search_string[:31])  # Excel sheet names are limited to 31 characters

                for file, contexts in result.items():
                    for context in contexts:
                        for line in context:
                            # Split the line by spaces
                            columns = line.split()  # Split the line into columns by spaces

                            # Convert numeric strings to numbers
                            for i in range(len(columns)):
                                if columns[i].isdigit():  # Check if the value is numeric
                                    columns[i] = int(columns[i])  # Convert to an integer
                                else:
                                    try:
                                        columns[i] = float(columns[i])  # Attempt to convert to a float
                                    except ValueError:
                                        pass  # Leave as a string if conversion fails

                            # Append the processed row to the sheet
                            sheet.append(columns)
            else:
                print(f"The ID {search_string} was not found in any files.")

        # Save the workbook to a file
        results_file_path = os.path.join(script_dir, "Results.xlsx")
        workbook.save(results_file_path)
        print(f"Results saved to {results_file_path}")
        
# Need to Add:
# Flexibility to add more lines of interest
# Check for B boxes, (if list contains check for boxes, check if file contained is B or A boxes)
# Some other sort of output, possible CSV or Excel File? worst case just a text file
# Need to add the ability to input a list of IDs and scan through each one
# Need to change box / date ouput to 
    # Box should only have one output unless it is changed, in which case it should list all changes
    # Date should be the first and last date assosciated with the ID