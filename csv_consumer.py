from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import json
import os 

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set the project_id, topic name, and subscription id
project_id="";
topic_name = "sensor-data-processed";
subscription_id = "sensor-data-processed-sub";

# create a subscriber for the project using the subscription_id
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
topic_path = 'projects/{}/topics/{}'.format(project_id,topic_name);

print(f"Listening for messages on {subscription_path}..\n")

# A callback function for handling received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # Deserialize the message into a dictionary
    message_data = json.loads(message.data.decode('utf-8'));
    
    # Print the values of the dictionary
    print("Processed record:")
    print("  Time: {}".format(message_data.get('time')))
    print("  Location: {}".format(message_data.get('profile_name')))
    print("  Temperature: {} degrees F".format(message_data.get('temperature')))
    print("  Humidity: {}%".format(message_data.get('humidity')))
    print("  Pressure: {} psi".format(message_data.get('pressure')))
    print("-" * 50)
   
   # Report to Google Pub/Sub that the message has been successfully processed
    message.ack()
    
with subscriber:
    # The callback function will be called for each message received from the topic 
    # through the subscription.
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
