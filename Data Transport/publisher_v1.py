from google.cloud import pubsub_v1
import os
import json


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/jbanerje/labservicekey.json"


project_id = "utopian-pact-456118-b3"
topic_id = "my-topic"


publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


with open("bcsample.json", "r") as f:
    breadcrumbs = json.load(f)

print(f" Loaded {len(breadcrumbs)} records from bcsample.json")


msg_count = 0


for record in breadcrumbs:
    message_data = json.dumps(record).encode("utf-8")
    future = publisher.publish(topic_path, data=message_data)
    print(f" Published Vehicle {record.get('VehicleID')} → Message ID: {future.result()}")
    msg_count += 1


print(f"\n All records have been published to {topic_path} and message count is: {msg_count}")
