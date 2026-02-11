import requests
import json
import time
from kafka import KafkaProducer

# 1. Setup the Kafka Producer
# We use 'localhost:9092' because that's where we mapped Kafka in Docker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2. Function to fetch data from the National Grid API
def fetch_energy_data():
    # This API gives us the current carbon intensity of the UK grid
    url = "https://api.carbonintensity.org.uk/intensity"
    try:
        response = requests.get(url)
        # We extract the specific data part
        return response.json()['data'][0]
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

print("🚀 Energy Producer started! Sending data every 10 seconds...")

# 3. The main loop to stream data
try:
    while True:
        energy_data = fetch_energy_data()
        if energy_data:
            # We send the data to a 'topic' called 'uk-energy-data'
            producer.send('uk-energy-data', energy_data)
            print(f"✅ Data Sent to Kafka: {energy_data}")
        
        # Wait for 10 seconds before the next check
        time.sleep(10)
except KeyboardInterrupt:
    print("\n🛑 Producer stopped.")