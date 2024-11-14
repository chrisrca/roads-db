import xml.etree.ElementTree as ET
import csv
import sqlite3
from tqdm import tqdm
import logging
import traceback

# Set to True to skip the node collection phase if the .db already exists
skip_node_collection = False

# Paths for the input .osm file, output CSV, a
# nd new midpoint .osm file
osm_file_path = "E:/sub-regions/parsed/wyoming-latest.osm"
output_csv_path = "E:/sub-regions/output/wyoming_midpoints.csv"
output_osm_path = "E:/sub-regions/output/wyoming_midpoints.osm"
db_path = "node_coordinates.db"

# Set up logging to capture errors
logging.basicConfig(filename="midpoints.log", level=logging.INFO, format="%(asctime)s %(message)s")

# Set up SQLite database for node storage
conn = sqlite3.connect(db_path)
c = conn.cursor()

# Check if we need to create the table and index (only if not skipping node collection)
if not skip_node_collection:
    c.execute("CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY, lat REAL, lon REAL)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_node_id ON nodes (id)")
    conn.commit()

# Open CSV and OSM output files for writing
with open(output_csv_path, mode="w", newline="") as csv_file, open(output_osm_path, "w", encoding="utf-8") as osm_file:
    # Set up CSV writer
    writer = csv.writer(csv_file)
    writer.writerow(["way_id", "mid_lat", "mid_lon"])  # CSV Header

    # Write the opening tag for the .osm file
    osm_file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    osm_file.write('<osm version="0.6" generator="midpoint-script">\n')

    # Only run node collection if skip_node_collection is False
    if not skip_node_collection:
        try:
            # First pass: Extract node coordinates and save to database
            logging.info("Starting node collection phase...")
            for event, elem in tqdm(ET.iterparse(osm_file_path, events=("start", "end"))):
                if event == "end" and elem.tag == "node":
                    node_id = elem.attrib["id"]
                    lat = float(elem.attrib["lat"])
                    lon = float(elem.attrib["lon"])
                    c.execute("INSERT OR IGNORE INTO nodes (id, lat, lon) VALUES (?, ?, ?)", (node_id, lat, lon))

                    # Commit every 100,000 nodes to avoid memory overload
                    if int(node_id) % 100000 == 0:
                        conn.commit()
                        logging.info(f"Committed nodes up to ID {node_id}")

                    elem.clear()  # Clear element to save memory

                if event == "end" and elem.tag == "way":
                    break  # Stop parsing after collecting all nodes

            conn.commit()  # Final commit for any remaining nodes
            logging.info("Node collection complete.")
        
        except Exception as e:
            logging.error(f"An error occurred during node collection: {e}")
            conn.close()
            raise e  # Stop execution if there's an error in node collection

    else:
        logging.info("Skipping node collection phase and using existing database.")

    # Second pass: Process ways to calculate midpoints
    try:
        logging.info("Starting way processing phase...")
        for event, elem in tqdm(ET.iterparse(osm_file_path, events=("start", "end"))):
            if event == "end" and elem.tag == "way":
                way_id = elem.attrib["id"]
                nd_refs = [nd.attrib["ref"] for nd in elem.findall("nd")[:2]]
                
                # Ensure there are at least two nodes in the way
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

                        # Write to CSV and OSM file immediately
                        writer.writerow([way_id, mid_lat, mid_lon])
                        csv_file.flush()  # Ensure data is written immediately

                        midpoint_node = f'<node id="-{way_id}" lat="{mid_lat}" lon="{mid_lon}" version="1" />\n'
                        osm_file.write(midpoint_node)
                        osm_file.flush()  # Ensure data is written immediately

                elem.clear()  # Clear element to save memory

        # Write the closing tag for the .osm file
        osm_file.write('</osm>\n')
        logging.info("Way processing complete.")

    except Exception as e:
        # Log full traceback
        logging.error(f"An error occurred during way processing: {e}")
        logging.error(traceback.format_exc())
        raise e  # Stop execution if there's an error in way processing

    finally:
        # Close database connection
        conn.close()
        logging.info("Database connection closed.")

    logging.info(f"Midpoints saved to {output_csv_path} and {output_osm_path}")