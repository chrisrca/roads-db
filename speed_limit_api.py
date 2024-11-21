from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2
import time

app = FastAPI()

class Coordinates(BaseModel):
    latitude: float
    longitude: float

def fetch_speed_limit_from_db(way_id):
    conn = psycopg2.connect(
        dbname="speed_limits",
        user="postgres",
        password="0120",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    query = "SELECT speed_limit FROM way_speed_limits WHERE way_id = %s;"
    cursor.execute(query, (way_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else "Unknown"

def find_nearby_states(lat, lon):
    conn = psycopg2.connect(
        dbname="state_bounding_boxes",
        user="postgres",
        password="0120",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    query = """
    SELECT state FROM bounding_boxes
    WHERE %s BETWEEN min_lat - 0.5 AND max_lat + 0.5
      AND %s BETWEEN min_lon - 0.5 AND max_lon + 0.5
    ORDER BY (min_lat - %s)^2 + (min_lon - %s)^2
    LIMIT 5;
    """
    cursor.execute(query, (lat, lon, lat, lon))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return [result[0] for result in results]

def find_nearest_way_in_state(state, lat, lon):
    conn = psycopg2.connect(
        dbname=state,
        user="postgres",
        password="0120",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
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
    cursor.close()
    conn.close()
    return result

@app.post("/find_nearest_way/")
def find_nearest_way(coords: Coordinates):
    lat, lon = coords.latitude, coords.longitude
    start_time = time.time()
    states = find_nearby_states(lat, lon)

    if not states:
        return {"message": "Coordinates do not match any state bounding box."}

    nearest_way = None
    min_distance = float('inf')

    for state in states:
        result = find_nearest_way_in_state(state, lat, lon)
        if result and result[3] < min_distance:
            nearest_way = result
            min_distance = result[3]

    end_time = time.time()
    duration = end_time - start_time

    if nearest_way:
        way_id = nearest_way[0]
        speed_limit = fetch_speed_limit_from_db(way_id)
        return {
            "Nearest Way ID": nearest_way[0],
            "Name": nearest_way[1],
            "Highway Type": nearest_way[2],
            "Speed Limit": speed_limit,
        }
    else:
        return {"message": "No way found near the given coordinates."}
