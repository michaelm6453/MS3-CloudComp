from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ## to install
import glob                             # for searching for json file
import json
import os
import csv
import time

# Search the current directory for the JSON file (including the service account key)
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id and topic name
project_id = "" # removed for security reasons
topic_name = "sensor-data-raw"

# Create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}.")

# Helper functions for safe type casting
def to_int(value):
    try:
        # CSV stores time in scientific notation
        # so we convert to float first, then to int
        return int(float(value))
    except (TypeError, ValueError):
        return None

def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

# Read the CSV file and iterate over the records
with open('Labels.csv', 'r') as csvfile:
    csv_reader = csv.DictReader(csvfile)

    for row in csv_reader:
        try:
            # Convert each record (row from the CSV file) into a dictionary
            record = {
                'time': to_int(row.get('time')),
                'profile_name': row.get('profileName'),
                'temperature': to_float(row.get('temperature')),
                'humidity': to_float(row.get('humidity')),
                'pressure': to_float(row.get('pressure'))
            }

            # Serialize the dictionary into a message
            record_value = json.dumps(record).encode('utf-8')

            # Publish the message to the topic
            future = publisher.publish(topic_path, record_value)

            # Ensure that the publishing has been completed successfully
            future.result()
            print(f"Published: {record['profile_name']} at {record['time']}")
            
            # Small delay between messages for streaming
            time.sleep(0.1)

        except Exception as e:
            print(f"Failed to publish the message: {e}")
