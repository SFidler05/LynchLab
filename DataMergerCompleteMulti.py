from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, FilePipePrinterAccessMask
from smbprotocol.create_contexts import CreateContextName
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
    exit()  # Stop execution

# Thread count choice, with default of 4
num_threads = int(input("Enter the number of threads to use (default is 4): ") or 4)

# Determine chunk size, default is 16KB, decrease if memory or wifi issues
chunk_size = int(input("Enter the chunk size in KB (default is 16, decrease if issues): ") or 16) * 1024  # Convert to bytes

# Define the date range
start_date = datetime(2014, 1, 1)  # Start date, earliest Lynch Lab file
end_date = datetime.now().date()  # End date, to current day to catch all files

# Set the directory path for new files !!!(need to make flexible before publish)!!!
# Make temporary folder in folder with other .exe?
results_dir = "/Users/samfidler/Desktop/Lab Project/LynchLab/Results"

# Clear the directory by deleting it and recreating it
if os.path.exists(results_dir):
    shutil.rmtree(results_dir)  # Delete the directory and all its contents
os.makedirs(results_dir, exist_ok=True)  # Recreate the directory

# Room and Box lists to catch all files
room = ['G126', 'G138', 'G140'] # add more rooms as needed if lab expands
box = ['1-16', '1B-16B', '1-16', '1B-16B', '1-16', '1B-14B'] # if more rooms are added, add "1-16, 1B-16B" to end of list

# Initialize current_date to the begininning
current_date = start_date.date()

# Lock for thread-safe index management (???)
index_lock = Lock()

# Define the total number of files to process for progress bar
total_files = len(room) * len(box) * ((end_date - start_date.date()).days + 1)

# Create a shared progress bar for all threads
progress_bar = tqdm(total=total_files, desc="Processing Files", position=0, leave=True)

# Lock for thread-safe progress bar updates (???)
progress_lock = Lock()

# Shared indices to find correct box/room
roomIndex = 0
boxIndex = 0

def process_box(start_date, end_date, results_dir, tree):
    """
    Function to process a single box for a given room.
    Dynamically updates roomIndex and boxIndex.
    """
    global roomIndex, boxIndex

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

        # Define the output file path for the combined data, named in "room_box" format
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

                    # Read file content in chunks of 1 KB
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
                            break  # Exit the loop if an error occurs during reading, typically end of file error

                    file.close()

                    # Check if the file is empty
                    if not data:
                        current_date += timedelta(days=1)
                        continue

                    # Append the content to the combined file
                    combined_file.write(data)

                except Exception:
                    # Handle exceptions (e.g., file not found)
                    current_date += timedelta(days=1)
                    continue  # Skip to the next iteration

                finally:
                    # Update the progress bar
                    with progress_lock:
                        progress_bar.update(1)


# no idea how this works, if it ever breaks the script won't function
with ThreadPoolExecutor(max_workers=num_threads) as executor:  # Adjust max_workers based on num_threads
    futures = [executor.submit(process_box, start_date, end_date, results_dir, tree) for _ in range(num_threads)]

    # Wait for all threads to complete
    for future in as_completed(futures):
        future.result()

# Final message
print("Processing complete.")

# Play an alert sound (don't think this is working)
os.system('afplay /System/Library/Sounds/Glass.aiff')

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