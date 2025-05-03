from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import os


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/jbanerje/labservicekey.json'


project_id = "utopian-pact-456118-b3"
subscription_id = "my-sub"


timeout = 40.0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

msg_count = 0


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global msg_count
    print(f" Received message: {message.data.decode('utf-8')}")
    message.ack()  
    msg_count += 1


streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f" Listening for messages on {subscription_path}...\n")


with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()

print(f"\n Total number of messages received: {msg_count}")
