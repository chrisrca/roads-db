import os

# Define input directory and output command file path
input_dir = r"E:\sub-regions\parsed"
commands_file_path = r"E:\sub-regions\osm2pgsql_commands.txt"

# PostgreSQL connection details
pg_user = "postgres"
pg_host = "localhost"
pg_password = "0120"
osm2pgsql_path = r"E:\osm2pgsql-bin\osm2pgsql"
style_file = r"E:\osm2pgsql-bin\default.style"

# Open the commands file for writing
with open(commands_file_path, "w") as commands_file:
    # Loop through each .osm file in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith(".osm"):
            input_path = os.path.join(input_dir, filename)

            # Derive the database name by replacing hyphens with underscores and removing the file extension
            db_name = filename.replace("-latest.osm", "").replace("-", "_")

            # Construct the osm2pgsql command for each file with correct backslashes
            osm2pgsql_command = (
                f"{osm2pgsql_path} -d {db_name} -U {pg_user} -H {pg_host} -W "
                f"-S {style_file} {input_path}"
            )

            # Write the command to the file
            commands_file.write(osm2pgsql_command + "\n")

print(f"All osm2pgsql commands have been written to {commands_file_path}")
