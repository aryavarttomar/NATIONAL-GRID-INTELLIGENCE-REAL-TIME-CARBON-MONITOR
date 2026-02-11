import json
import boto3
from kafka import KafkaConsumer
from datetime import datetime
from io import BytesIO

# 1. Setup MinIO (S3) Client
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000', # MinIO API port
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

BUCKET_NAME = 'greenpulse-bronze'

# 2. Setup Kafka Consumer
consumer = KafkaConsumer(
    'uk-energy-data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"📥 Smart Consumer started! Watching for energy data to save in {BUCKET_NAME}...")

try:
    for message in consumer:
        data = message.value
        
        # 3. Create a unique filename using the timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"energy_data_{timestamp}.json"
        
        # 4. Upload to MinIO
        json_data = json.dumps(data).encode('utf-8')
        s3_client.upload_fileobj(
            BytesIO(json_data),
            BUCKET_NAME,
            filename
        )
        
        print(f"✅ Saved to Bronze Vault: {filename}")

except KeyboardInterrupt:
    print("\n🛑 Smart Consumer stopped.")