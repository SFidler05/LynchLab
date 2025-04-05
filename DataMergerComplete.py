from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, FilePipePrinterAccessMask
from smbprotocol.create_contexts import CreateContextName
from datetime import datetime, timedelta
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
        password = input("Enter your password here: ")

# Access shared folder
try:
    tree = TreeConnect(session, f"\\\\{server}\\{share}")
    tree.connect()
    print("Shared folder accessed successfully!")
except Exception as e:
    print("Error: Unable to access the shared folder. Exiting.")
    exit()  # Stop execution

# Define the date range
start_date = datetime(2014, 1, 1)  # Start date
end_date = datetime.now().date()  # End date

# Ensure the directory exists
results_dir = "/Users/samfidler/Desktop/Lab Project/Results"

# Clear the directory by deleting it and recreating it
if os.path.exists(results_dir):
    shutil.rmtree(results_dir)  # Delete the directory and all its contents
os.makedirs(results_dir, exist_ok=True)  # Recreate the directory

# Variables for which files in the Data Backup folder to be combined
room = ['G126', 'G138', 'G140'] #add more rooms as needed
box = ['1-16', '1B-16B', '1-16', '1B-16B', '1-16', '1B-14B'] #if more boxes are added, add "1-16, 1B-16B"

# Loop through each date in the range
current_date = start_date.date()

# Initialize indices for room and box
roomIndex = 0
boxIndex = 0

# Loop through each room
while roomIndex < len(room):
    # Loop through each box
    for i in range(2):
        # Reset the current date to the start date for each box
        current_date = start_date.date()

        # Define the output file path for the combined data
        combined_file_path = os.path.join(results_dir, f"{room[roomIndex]}_{box[boxIndex]}.txt")

        # Open the combined file in write-binary mode
        with open(combined_file_path, "wb") as combined_file:
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")  # Format: YYYY-MM-DD

                # Dynamically construct the file path based on the year
                year = current_date.year  # Extract the year from the current date
                file_path = f"WLynch_Labs/Data Backup/{room[roomIndex]}/{box[boxIndex]}/{year}/!{date_str}"

                try:
                    print(f"Processing file: {room[roomIndex]} {box[boxIndex]} !{date_str}.")

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
                    chunk_size = 1028  # 1 KB
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
                    print("File Found! Adding data.")
                    combined_file.write(data)

                except Exception:
                    current_date += timedelta(days=1)
                    continue  # Ignore errors and continue

                # Move to the next date
                current_date += timedelta(days=1)

        print(f"End of box {box[boxIndex]}")
        boxIndex += 1  # Move to the next box
        if boxIndex < len(box) and boxIndex % 2 != 0:
            print(f"Starting box {box[boxIndex]}")

    # Move to the next room
    print(f"End of room {room[roomIndex]}")
    roomIndex += 1
    if (roomIndex < len(room)):
        print(f"Starting room {room[roomIndex]}")
        print(f"Starting box {box[boxIndex]}")

# Final message
print("Processing complete.")

# Play an alert sound
os.system('say "Processing complete"')

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