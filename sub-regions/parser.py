import os

# Define input and output directories
input_dir = "E:/sub-regions/unparsed/"
output_dir = "E:/sub-regions/parsed/"
commands_file_path = "E:/sub-regions/osmium_commands.txt"

# Open the commands file for writing
with open(commands_file_path, "w") as commands_file:
    # Loop through each .osm file in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith(".osm"):
            input_path = os.path.join(input_dir, filename)
            output_path = os.path.join(output_dir, filename)  # same name as input file

            # Construct the osmium command
            osmium_command = (
                f"osmium tags-filter {input_path} "
                "w/highway=motorway,motorway_link,trunk,trunk_link,primary,primary_link,"
                "secondary,secondary_link,tertiary,tertiary_link,unclassified,unclassified_link,"
                "residential,residential_link,service,service_link,living_street,road "
                f"-o {output_path}"
            )

            # Write the command to the file
            commands_file.write(osmium_command + "\n")

print(f"All commands have been written to {commands_file_path}")
