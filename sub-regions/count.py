import os
import csv

# Directory containing the CSV files
directory = r"E:\sub-regions\output"

# Dictionary to store filename and line count
file_line_counts = {}

# Iterate through all files in the directory
for filename in os.listdir(directory):
    if filename.endswith(".csv"):
        filepath = os.path.join(directory, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as file:
                line_count = sum(1 for line in file)
                file_line_counts[filename] = line_count
        except Exception as e:
            file_line_counts[filename] = f"Error: {e}"

# Display the results
print(file_line_counts)
