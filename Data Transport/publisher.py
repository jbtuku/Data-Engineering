from google.cloud import pubsub_v1
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/jbanerje/labservicekey.json"


project_id = "utopian-pact-456118-b3"
topic_id = "datatransporttopic"


publisher = pubsub_v1.PublisherClient()


topic_path = publisher.topic_path(project_id, topic_id)


for n in range(1, 10):
    data_str = f"Message number {n}"
    data = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    print(f"Published: {data_str} → Message ID: {future.result()}")

print(f"\n Finished publishing to topic: {topic_path}")
