import boto3
import snowflake.connector
import os

# 1. Connect to local MinIO
s3 = boto3.resource('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)
bucket = s3.Bucket('greenpulse-bronze')

# 2. Connect to Snowflake 
# Use your Snowflake login credentials here
ctx = snowflake.connector.connect(
    user='vart1556', 
    password='1XubileSat&1556',
    account='AI35326.eu-west-1'
)
cs = ctx.cursor()

try:
    # Set the right context
    cs.execute("USE ROLE ACCOUNTADMIN")
    cs.execute("USE ROLE ACCOUNTADMIN")
    cs.execute("CREATE DATABASE IF NOT EXISTS ENERGY_PROJECT")
    cs.execute("CREATE SCHEMA IF NOT EXISTS ENERGY_PROJECT.BRONZE")
    cs.execute("USE SCHEMA ENERGY_PROJECT.BRONZE")
    
    # Create a temporary 'Stage' inside Snowflake
    cs.execute("CREATE OR REPLACE STAGE energy_stage")

    print("📤 Migrating files from local MinIO to Snowflake Cloud...")

    # Loop through every file in your MinIO bucket
    for obj in bucket.objects.all():
        file_path = obj.key
        # Download from MinIO to your laptop temporarily
        bucket.download_file(obj.key, file_path)
        
        # Upload from your laptop to the Snowflake Stage
        cs.execute(f"PUT file://{os.path.abspath(file_path)} @energy_stage")
        
        # Remove the temporary local file
        os.remove(file_path)
        print(f"✅ Staged: {file_path}")

    # 3. Final Step: Copy the JSON from the stage into the actual table
    cs.execute("COPY INTO RAW_INTENSITY FROM @energy_stage FILE_FORMAT = (TYPE = JSON)")
    print("🚀 SUCCESS! All data is now live in Snowflake.")

finally:
    cs.close()
    ctx.close()