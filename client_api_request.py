import requests
import time

def query_nearest_way(api_url, latitude, longitude):
    # Define the JSON payload
    payload = {
        "latitude": latitude,
        "longitude": longitude
    }

    # Measure the start time
    start_time = time.time()

    try:
        # Make the POST request
        response = requests.post(api_url, json=payload)
        
        # Measure the end time
        end_time = time.time()
        query_time = end_time - start_time  # Round-trip time in seconds

        # Check for successful response
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            print("Response received:")
            print(f"Way ID: {data.get('Nearest Way ID', 'N/A')}")
            print(f"Name: {data.get('Name', 'N/A')}")
            print(f"Highway Type: {data.get('Highway Type', 'N/A')}")
            print(f"Speed Limit: {data.get('Speed Limit', 'N/A')}")
            print(f"Query Time: {query_time:.4f} seconds")
        else:
            print(f"Error: Received status code {response.status_code}")
            print(f"Message: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

# Example usage
if __name__ == "__main__":
    API_URL = "http://73.218.203.249:8000/find_nearest_way/"
    LATITUDE = 42.2726854
    LONGITUDE = -71.8098057
    query_nearest_way(API_URL, LATITUDE, LONGITUDE)
