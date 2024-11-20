import psycopg2

def count_roads_with_speed_limits(states, region_name="Region"):
    total_ways_all_states = 0
    total_matched_ways_all_states = 0

    print(f"--- {region_name} Results ---")
    
    # Loop through each state
    for state in states:
        # Connect to the state-specific database
        state_conn = psycopg2.connect(
            dbname=state,
            user="postgres",
            password="0120",
            host="localhost",
            port="5432"
        )
        state_cursor = state_conn.cursor()

        # Connect to the speed limits database
        speed_limits_conn = psycopg2.connect(
            dbname="speed_limits",
            user="postgres",
            password="0120",
            host="localhost",
            port="5432"
        )
        speed_limits_cursor = speed_limits_conn.cursor()

        # Query to get all way_ids from the state's database
        state_cursor.execute("SELECT osm_id FROM planet_osm_line WHERE highway IS NOT NULL;")
        state_ways = state_cursor.fetchall()

        # Prepare to count matches for this state
        total_ways = len(state_ways)
        matched_ways = 0

        # Check each way_id against the speed_limits database
        for way in state_ways:
            way_id = way[0]
            speed_limits_cursor.execute("SELECT speed_limit FROM way_speed_limits WHERE way_id = %s;", (way_id,))
            result = speed_limits_cursor.fetchone()
            if result and result[0] != "0 mph":  # Only count if the speed limit is NOT "0 mph"
                matched_ways += 1

        # Calculate percentage for the current state
        percentage_with_speed_limits = (matched_ways / total_ways) * 100 if total_ways > 0 else 0

        # Print results for the current state
        print(f"{state.capitalize()} - Total roads: {total_ways}, Roads with valid speed limits: {matched_ways} "
              f"({percentage_with_speed_limits:.2f}%)")
        
        # Add to the overall totals
        total_ways_all_states += total_ways
        total_matched_ways_all_states += matched_ways

        # Close connections for the current state
        state_cursor.close()
        state_conn.close()
        speed_limits_cursor.close()
        speed_limits_conn.close()

    # Calculate overall percentages
    percentage_all_states = (total_matched_ways_all_states / total_ways_all_states) * 100 if total_ways_all_states > 0 else 0

    # Print overall totals
    print(f"\n--- {region_name} Total ---")
    print(f"Total roads in {region_name}: {total_ways_all_states}")
    print(f"Total roads with valid speed limits: {total_matched_ways_all_states} ({percentage_all_states:.2f}%)")

# New England states
new_england_states = ["connecticut", "maine", "massachusetts", "new_hampshire", "rhode_island", "vermont"]

# All U.S. states (add all 50 states to this list)
all_us_states = [
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware", "florida",
    "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine", 
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi", "missouri", "montana", "nebraska", 
    "nevada", "new_hampshire", "new_jersey", "new_mexico", "new_york", "north_carolina", "north_dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode_island", "south_carolina", "south_dakota", "tennessee", "texas",
    "utah", "vermont", "virginia", "washington", "west_virginia", "wisconsin", "wyoming"
]

# Run the function for New England
count_roads_with_speed_limits(new_england_states, region_name="New England")

# Run the function for all U.S. states
count_roads_with_speed_limits(all_us_states, region_name="United States")
