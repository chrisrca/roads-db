import psycopg2
import time

def fetch_speed_limit_from_db(way_id):
    # Connect to the centralized speed limits database
    conn = psycopg2.connect(
        dbname="speed_limits",
        user="postgres",
        password="0120",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    # Query to find the speed limit for the given way_id
    query = "SELECT speed_limit FROM way_speed_limits WHERE way_id = %s;"
    cursor.execute(query, (way_id,))
    result = cursor.fetchone()

    # Close the connection
    cursor.close()
    conn.close()

    # Return the speed limit or "Unknown" if not found
    return result[0] if result else "Unknown"

def find_nearby_states(lat, lon):
    # Connect to the central bounding box database
    conn = psycopg2.connect(
        dbname="state_bounding_boxes",
        user="postgres",
        password="0120",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    
    # Query to find up to 5 states whose bounding boxes are near the given coordinates
    query = """
    SELECT state FROM bounding_boxes
    WHERE %s BETWEEN min_lat - 0.5 AND max_lat + 0.5
      AND %s BETWEEN min_lon - 0.5 AND max_lon + 0.5
    ORDER BY (min_lat - %s)^2 + (min_lon - %s)^2
    LIMIT 5;
    """
    cursor.execute(query, (lat, lon, lat, lon))
    results = cursor.fetchall()
    
    # Close the cursor and connection
    cursor.close()
    conn.close()
    
    # Extract state names from the query results
    return [result[0] for result in results]

def find_nearest_way_in_state(state, lat, lon):
    # Connect to the state-specific PostgreSQL database
    conn = psycopg2.connect(
        dbname=state,
        user="postgres",
        password="0120",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    # SQL query to find the nearest way
    query = """
    SELECT osm_id, name, highway, 
           ST_Distance(ST_Transform(way, 4326), ST_SetSRID(ST_MakePoint(%s, %s), 4326)) AS distance
    FROM planet_osm_line
    WHERE highway IS NOT NULL
    ORDER BY ST_Transform(way, 4326) <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
    LIMIT 1;
    """

    cursor.execute(query, (lon, lat, lon, lat))
    result = cursor.fetchone()
    
    # Close the cursor and connection
    cursor.close()
    conn.close()
    
    return result

def find_nearest_way(lat, lon):
    # Measure the total search time
    start_time = time.time()
    
    # Step 1: Find up to 5 nearby states
    states = find_nearby_states(lat, lon)
    
    if not states:
        print("Coordinates do not match any state bounding box.")
        return
    
    nearest_way = None
    min_distance = float('inf')

    for state in states:
        print(f"Searching in the {state} database...")
        
        # Step 2: Search in each state and keep the closest result
        result = find_nearest_way_in_state(state, lat, lon)
        
        if result and result[3] < min_distance:
            nearest_way = result
            min_distance = result[3]
    
    # Calculate the total duration
    end_time = time.time()
    duration = end_time - start_time

    # Display the nearest result found
    if nearest_way:
        way_id = nearest_way[0]  # Extract way_id from the nearest way
        speed_limit = fetch_speed_limit_from_db(way_id)  # Fetch the speed limit
        print(f"Nearest Way ID: {nearest_way[0]}")
        print(f"Name: {nearest_way[1]}")
        print(f"Highway Type: {nearest_way[2]}")
        print(f"Speed Limit: {speed_limit}")
        print(f"Distance: {nearest_way[3]} meters")
    else:
        print("No way found near the given coordinates.")

    print(f"Total query time: {duration:.4f} seconds.")

# Example usage
latitude = 42.77197744734784
longitude = -71.46470629821955
find_nearest_way(latitude, longitude)
