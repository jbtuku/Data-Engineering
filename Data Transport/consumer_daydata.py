from google.cloud import pubsub_v1
import os
import time
import threading

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/jbanerje/labservicekey.json'

project_id = "utopian-pact-456118-b3"
subscription_id = "my-sub"
subscription_path = pubsub_v1.SubscriberClient().subscription_path(project_id, subscription_id)

timeout = 600.0                
idle_timeout = 30.0             
msg_count = 0

subscriber = pubsub_v1.SubscriberClient()
last_message_time = time.time()
shutdown_event = threading.Event()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global msg_count, last_message_time
    print(f"Received: {message.data.decode('utf-8')}")
    message.ack()
    msg_count += 1
    last_message_time = time.time()

def monitor_inactivity():
    while not shutdown_event.is_set():
        if time.time() - last_message_time > idle_timeout:
            print(f"\n🛑 No new messages in the last {idle_timeout} seconds. Stopping early.")
            shutdown_event.set()
            streaming_pull_future.cancel()
            break
        time.sleep(5)

start = time.time()
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
monitor_thread = threading.Thread(target=monitor_inactivity)
monitor_thread.start()

print(f"🔄 Listening for messages on {subscription_path}... (Max {timeout}s)")

with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except Exception as e:
        pass  

shutdown_event.set()
monitor_thread.join()
end = time.time()

print(f"\n✅ Total messages received: {msg_count} in {end - start:.2f} seconds")
