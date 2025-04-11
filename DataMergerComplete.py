from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, FilePipePrinterAccessMask
from smbprotocol.file_info import FileAttributes
from smbprotocol.file_info import FileInformationClass
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
import getpass
import shutil
import os
import uuid
import re  # Import for regex to match folder names

# Define server and credentials
server = "topaz.storage.virginia.edu"
share = "psychmed_shared$"

# Prompt for username and password once before the loop
username = input("Enter your username here: ")
password = getpass.getpass("Enter your password here: ")

# Establish SMB connection with retry logic
while True:
    try:
        # Generate a UUID
        client_guid = uuid.uuid4()

        # Attempt to establish the SMB connection
        conn = Connection(guid=client_guid, server_name=server, port=445)
        conn.connect()
        session = Session(conn, username, password)
        session.connect()
        print("Connection to server successful!")
        break  # Exit the loop if the connection is successful
    except Exception:
        print("Error: Incorrect username or password. Please try again.")
        # Prompt for username and password again if the connection fails
        username = input("Enter your username here: ")
        password = getpass.getpass("Enter your password here: ")

# Access shared folder
try:
    tree = TreeConnect(session, f"\\\\{server}\\{share}")
    tree.connect()
    print("Shared folder accessed successfully!")
except Exception as e:
    print("Error: Unable to access the shared folder. Exiting.")
    exit()  # Stop execution]

# Automatically determine the number of threads based on CPU cores
num_threads = os.cpu_count()

# Input for chunk size in KB
chunk_size = 32 * 1024  # Convert KB to bytes

# Define the date range
start_date = datetime(2014, 1, 1)  # Start date
end_date = datetime.now().date()  # End date

# Ensure the directory exists
script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the script

# Dynamically name the results directory based on the current date
current_date_str = datetime.now().strftime("%Y-%m-%d")  # Format: YYYY-MM-DD
results_dir = os.path.join(script_dir, f"Data_{current_date_str}")

# Delete all folders matching the pattern "Data_{any date}"
for folder_name in os.listdir(script_dir):
    if re.match(r"Data_\d{4}-\d{2}-\d{2}", folder_name):  # Match folders like "Data_YYYY-MM-DD"
        folder_path = os.path.join(script_dir, folder_name)
        if os.path.isdir(folder_path):
            shutil.rmtree(folder_path)  # Delete the folder
            print(f"Deleted old folder: {folder_path}")

# Create the new results directory
os.makedirs(results_dir, exist_ok=True)
print(f"Output folder '{results_dir}' created.")

# Variables for which files in the Data Backup folder to be combined
room = ['G126', 'G138', 'G140'] #add more rooms as needed
box = ['1-16', '1B-16B', '1-16', '1B-16B', '1-16', '1B-14B'] #if more boxes are added, add "1-16, 1B-16B"

# Loop through each date in the range
current_date = start_date.date()

# Lock for thread-safe index management
index_lock = Lock()

# Shared indices
roomIndex = 0
boxIndex = 0

def get_smb_folder_size(tree, folder_path):
    """
    Calculate the total size of files in a folder on an SMB server.
    """
    total_size = 0
    try:
        # Open the folder on the SMB share
        folder = Open(tree, folder_path)
        folder.create(
            desired_access=FilePipePrinterAccessMask.GENERIC_READ,  # Use GENERIC_READ for directory access
            impersonation_level=0,
            file_attributes=0,
            share_access=1,
            create_disposition=1,
            create_options=0
        )

        # List the contents of the folder
        entries = folder.query_directory(
            pattern= "*",  # Wildcard pattern to match all files and directories
            file_information_class=FileInformationClass.FILE_DIRECTORY_INFORMATION
        )

        for entry in entries:
            # Decode the filename properly
            filename = entry['file_name'].get_value().decode('utf-16-le').strip()
            if filename not in [".", ".."]:  # Skip current and parent directory entries
                full_path = f"{folder_path}/{filename}"
                if entry['file_attributes'].get_value() & FileAttributes.FILE_ATTRIBUTE_DIRECTORY:
                    # Recursively calculate size for subdirectories
                    total_size += get_smb_folder_size(tree, full_path)
                else:
                    # Add the size of the file
                    file_size = entry['end_of_file'].get_value()
                    total_size += file_size

        folder.close()
    except Exception as e:
        print(f"Error accessing folder {folder_path}: {e}")
    return total_size

folder_path = "WLynch_Labs/Data Backup"  # Path to the folder on the SMB server
folder_size_bytes = get_smb_folder_size(tree, folder_path)  # Pass the tree object and folder path
folder_size_mb = folder_size_bytes / (1024 * 1024)  # Convert bytes to MB

# Input for the target folder size in MB
target_size_mb = folder_size_mb
target_size_bytes = target_size_mb * 1024 * 1024  # Convert MB to bytes

# Initialize a variable to track the cumulative size of processed files
cumulative_size = 0

# Create a progress bar based on the target size
progress_bar = tqdm(total=target_size_bytes, desc="Processing Files", unit="B", unit_scale=True, position=0, leave=True)

# Lock for thread-safe progress bar updates
progress_lock = Lock()

def process_box(start_date, end_date, results_dir, tree):
    """
    Function to process a single box for a given room.
    Dynamically updates roomIndex and boxIndex.
    """
    global roomIndex, boxIndex, cumulative_size

    while True:
        # Safely get the current indices and update them
        with index_lock:
            if roomIndex >= len(room):
                return  # Exit if all rooms are processed

            # Get the current room and box indices
            current_room = roomIndex
            current_box = boxIndex

            # Increment boxIndex and manage roomIndex
            boxIndex += 1
            if boxIndex % 2 == 0:  # After every two boxes, move to the next room
                roomIndex += 1

        # Reset the current date to the start date for each box
        current_date = start_date.date()

        # Define the output file path for the combined data
        combined_file_path = os.path.join(results_dir, f"{room[current_room]}_{box[current_box]}.txt")

        # Open the combined file in write-binary mode
        with open(combined_file_path, "wb") as combined_file:
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")  # Format: YYYY-MM-DD

                # Dynamically construct the file path based on the year
                year = current_date.year  # Extract the year from the current date
                file_path = f"WLynch_Labs/Data Backup/{room[current_room]}/{box[current_box]}/{year}/!{date_str}"

                try:
                    # Open the file on the SMB share
                    file = Open(tree, file_path)
                    file.create(
                        desired_access=FilePipePrinterAccessMask.GENERIC_READ,
                        impersonation_level=0,
                        file_attributes=0,
                        share_access=1,
                        create_disposition=1,
                        create_options=0
                    )

                    # Read file content in chunks
                    data = b""
                    offset = 0
                    while True:
                        try:
                            chunk = file.read(offset, chunk_size)
                            if not chunk:
                                break  # End of file reached
                            data += chunk
                            offset += len(chunk)
                        except Exception:
                            break  # Exit the loop if an error occurs during reading

                    file.close()

                    # Check if the file is empty
                    if not data:
                        current_date += timedelta(days=1)
                        continue

                    # Append the content to the combined file
                    combined_file.write(data)

                    # Update the cumulative size and progress bar
                    with progress_lock:
                        cumulative_size += len(data)
                        progress_bar.update(len(data))

                except Exception:
                    current_date += timedelta(days=1)
                    continue  # Ignore errors and continue

                # Move to the next date
                current_date += timedelta(days=1)

with ThreadPoolExecutor(max_workers=num_threads) as executor:  # Adjust max_workers based on num_threads
    futures = [executor.submit(process_box, start_date, end_date, results_dir, tree) for _ in range(num_threads)]

    # Wait for all threads to complete
    for future in as_completed(futures):
        future.result()

# Articially complete progress bar to 100%
progress_bar.n = progress_bar.total
progress_bar.last_print_n = progress_bar.total
progress_bar.refresh()  # Refresh the progress bar to show completion

# Close the progress bar
progress_bar.close()

# Final message
print("Processing complete.")

# Play an alert sound
os.system('afplay /System/Library/Sounds/Glass.aiff')  # Replace with your desired sound file

# Close the SMB session in the correct order
try:
    if tree:
        try:
            tree.disconnect()  # Disconnect the TreeConnect object
        except Exception as e:
            print(f"Error disconnecting TreeConnect: {e}")
    if session:
        try:
            session.disconnect()  # Disconnect the Session object
        except Exception as e:
            print(f"Error disconnecting Session: {e}")
    if conn:
        try:
            conn.disconnect()  # Disconnect the Connection object
        except Exception as e:
            print(f"Error disconnecting Connection: {e}")
    print("Successfully exited server.")
except Exception as e:
    print(f"Error during server cleanup: {e}")