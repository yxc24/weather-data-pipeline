import json
import time
from kafka import KafkaProducer
import requests

# Replace with your actual API key from OpenWeatherMap
API_KEY = "your api"

def fetch_weather_data(city, country):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": f"{city},{country}",
        "appid": API_KEY,
        "units": "metric"
    }
    response = requests.get(base_url, params=params)
    data = response.json()
    # Add timestamp to the response
    data["timestamp"] = int(time.time())
    return data

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=json_serializer
)

if __name__ == "__main__":
    cities = [
        ("Sydney", "au"),
        ("Melbourne", "au"),
        ("Perth", "au"),
        ("Brisbane", "au"),
        ("Adelaide", "au"),
        ("Hobart", "au"),
        ("Darwin", "au")
    ]

    start_time = time.time()
    while time.time() - start_time < 3600 * 12: # Run for 12 hours from 6am to 6pm
        
        for city, country in cities:
            try:
                data = fetch_weather_data(city, country)
                producer.send("new_weather_data", data)
                print(f"Sent: {data}")
            except Exception as e:
                print(f"Error fetching data for {city}: {str(e)}")
        time.sleep(300)  # Wait for 300 seconds before the next round of data
 
        
