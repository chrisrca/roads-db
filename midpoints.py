import os
import csv
import sqlite3
from lxml import etree as ET
from tqdm import tqdm
import logging
import traceback
import gc

# Paths for directories
input_dir = "E:/sub-regions/parsed/"
output_dir = "E:/sub-regions/output/"
combined_csv_path = os.path.join(output_dir, "combined_midpoints.csv")
combined_osm_path = os.path.join(output_dir, "combined_midpoints.osm")
db_path = "node_coordinates.db"

# Set up logging
logging.basicConfig(filename="midpoints.log", level=logging.INFO, format="%(asctime)s %(message)s")

# Create output directory if it doesn’t exist
os.makedirs(output_dir, exist_ok=True)

# Prepare combined CSV and OSM files
with open(combined_csv_path, mode="w", newline="") as combined_csv, open(combined_osm_path, "w", encoding="utf-8") as combined_osm:
    # Set up combined CSV writer and write header
    combined_writer = csv.writer(combined_csv)
    combined_writer.writerow(["way_id", "mid_lat", "mid_lon"])

    # Write opening tag for combined OSM file
    combined_osm.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    combined_osm.write('<osm version="0.6" generator="midpoint-script">\n')

    # Loop through each .osm file in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith(".osm"):
            osm_file_path = os.path.join(input_dir, filename)
            state_name = filename.replace("-latest.osm", "")

            # Individual output paths for each state
            state_csv_path = os.path.join(output_dir, f"{state_name}_midpoints.csv")
            state_osm_path = os.path.join(output_dir, f"{state_name}_midpoints.osm")

            # Set up SQLite database for node storage (reset for each state)
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            conn.execute("PRAGMA journal_mode=WAL;")
            c.execute("CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY, lat REAL, lon REAL)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_node_id ON nodes (id)")
            conn.commit()

            # Open CSV and OSM output files for writing for the current state
            with open(state_csv_path, mode="w", newline="") as state_csv, open(state_osm_path, "w", encoding="utf-8") as state_osm:
                # Set up CSV writer for the state file
                state_writer = csv.writer(state_csv)
                state_writer.writerow(["way_id", "mid_lat", "mid_lon"])

                # Write opening tag for the state OSM file
                state_osm.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                state_osm.write('<osm version="0.6" generator="midpoint-script">\n')

                try:
                    # First pass: Collect nodes
                    logging.info(f"Starting node collection phase for {state_name}...")
                    for event, elem in tqdm(ET.iterparse(osm_file_path, events=("end",), tag="node")):
                        node_id = elem.attrib["id"]
                        lat = float(elem.attrib["lat"])
                        lon = float(elem.attrib["lon"])
                        c.execute("INSERT OR IGNORE INTO nodes (id, lat, lon) VALUES (?, ?, ?)", (node_id, lat, lon))

                        # Commit every 10,000 nodes
                        if int(node_id) % 10000 == 0:
                            conn.commit()

                        # Clear the element from memory
                        elem.clear()
                        while elem.getprevious() is not None:
                            del elem.getparent()[0]
                        gc.collect()

                    conn.commit()
                    logging.info(f"Node collection complete for {state_name}.")

                    # Second pass: Process ways
                    logging.info(f"Starting way processing phase for {state_name}...")
                    for event, elem in tqdm(ET.iterparse(osm_file_path, events=("end",), tag="way")):
                        way_id = elem.attrib["id"]
                        nd_refs = [nd.attrib["ref"] for nd in elem.findall("nd")[:2]]

                        if len(nd_refs) == 2:
                            node1, node2 = nd_refs
                            # Retrieve node coordinates from the database
                            c.execute("SELECT lat, lon FROM nodes WHERE id = ?", (node1,))
                            result1 = c.fetchone()
                            c.execute("SELECT lat, lon FROM nodes WHERE id = ?", (node2,))
                            result2 = c.fetchone()
                            if result1 and result2:
                                lat1, lon1 = result1
                                lat2, lon2 = result2

                                # Calculate midpoint
                                mid_lat = (lat1 + lat2) / 2
                                mid_lon = (lon1 + lon2) / 2

                                # Write to state CSV and OSM file
                                state_writer.writerow([way_id, mid_lat, mid_lon])
                                state_csv.flush()
                                midpoint_node = f'<node id="-{way_id}" lat="{mid_lat}" lon="{mid_lon}" version="1" />\n'
                                state_osm.write(midpoint_node)
                                state_osm.flush()

                                # Write to combined files
                                combined_writer.writerow([way_id, mid_lat, mid_lon])
                                combined_csv.flush()
                                combined_osm.write(midpoint_node)
                                combined_osm.flush()

                        # Clear the element from memory
                        elem.clear()
                        while elem.getprevious() is not None:
                            del elem.getparent()[0]
                        gc.collect()

                    state_osm.write('</osm>\n')
                    logging.info(f"Way processing complete for {state_name}.")

                except Exception as e:
                    logging.error(f"An error occurred processing {state_name}: {e}")
                    logging.error(traceback.format_exc())
                    raise e

                finally:
                    conn.close()
                    logging.info(f"Database connection closed for {state_name}.")

            print(f"Completed processing for {state_name}: {state_csv_path} and {state_osm_path}")

    # Write the closing tag for the combined OSM file
    combined_osm.write('</osm>\n')
    logging.info("Combined processing complete.")
print(f"All state files processed. Combined results saved to {combined_csv_path} and {combined_osm_path}")
