from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/jbanerje/labservicekey.json"


project_id = "utopian-pact-456118-b3"
subscription_id = "my-sub"
timeout = 10.0  

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f" Received: {message.data.decode('utf-8')}")
    message.ack()  # Acknowledge so it won’t be redelivered

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f" Listening for messages on {subscription_path}...\n")

with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
