from kafka import KafkaConsumer
import json

# Initialize the Consumer
# 'auto_offset_reset' tells it where to start if it's the first time
# 'earliest' means "give me everything currently in the tank"
consumer = KafkaConsumer(
    'uk-energy-data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🕵️ Consumer started! Waiting for messages from the tank...")

try:
    for message in consumer:
        # 'message.value' is the actual data we sent earlier
        data = message.value
        print(f"📥 Received from Kafka: {data}")
        
        # Simple Logic: Check if intensity is 'high'
        intensity_index = data['intensity']['index']
        if intensity_index == 'high':
            print("⚠️ ALERT: Carbon intensity is high. Suggesting shift to renewables.")
            
except KeyboardInterrupt:
    print("\n🛑 Consumer stopped.")