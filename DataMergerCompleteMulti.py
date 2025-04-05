from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, FilePipePrinterAccessMask
from smbprotocol.create_contexts import CreateContextName
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
        print("Connection successful!")
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

# Multithreaded processing
num_threads = int(input("Enter the number of threads to use (default is 4): ") or 4)

# Input for chunk size in KB
chunk_size = int(input("Enter the chunk size in KB (default is 16: ") or 16) * 1024  # Convert KB to bytes
#Ensure chunk size is at least 1 KB
if chunk_size < 1024:
    print("Chunk size must be at least 1 KB. Setting to 1 KB.")
    chunk_size = 1024  # Set to 1 KB
# Ensure chunk size is not more than 1 MB
if chunk_size > 1024 * 1024:
    print("Chunk size must not exceed 16 MB. Setting to 16 MB.")
    chunk_size = 16 * 1024 * 1024  # Set to 1 MB

# Define the date range
start_date = datetime(2014, 1, 1)  # Start date
end_date = datetime.now().date()  # End date

# Ensure the directory exists
results_dir = "/Users/samfidler/Desktop/Lab Project/LynchLab/Results"

# Clear the directory by deleting it and recreating it
if os.path.exists(results_dir):
    shutil.rmtree(results_dir)  # Delete the directory and all its contents
os.makedirs(results_dir, exist_ok=True)  # Recreate the directory

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

def get_local_folder_size(folder_path):
    total_size = 0
    for dirpath, _, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += os.path.getsize(file_path)
    return total_size

folder_size_bytes = 1610612736 
folder_size_mb = folder_size_bytes * (1024 * 1024)  # Convert bytes to MB
print(f"Folder size: {folder_size_mb:.2f} MB")

# Input for the target folder size in MB
target_size_mb = folder_size_mb
target_size_bytes = target_size_mb  # Convert MB to bytes

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