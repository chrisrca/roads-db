import os

# Define input and output directories
input_dir = "E:/sub-regions/unparsed/"
commands_file_path = "E:/sub-regions/osmium_commands2.txt"

# Open the commands file for writing
with open(commands_file_path, "w") as commands_file:
    # Loop through each .osm file in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith(".osm"):
            input_path = os.path.join(input_dir, filename)

            # Construct the osmium fileinfo command to get metadata (including bounds)
            osmium_command = f"osmium fileinfo {input_path}"

            # Write the command to the file
            commands_file.write(osmium_command + "\n")

print(f"All fileinfo commands have been written to {commands_file_path}")
