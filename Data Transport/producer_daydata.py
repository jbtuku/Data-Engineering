from google.cloud import pubsub_v1
import os
import json
import time


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/jbanerje/labservicekey.json"
project_id = "utopian-pact-456118-b3"
topic_id = "my-topic"


batch_settings = pubsub_v1.types.BatchSettings(
    max_bytes=1024 * 1024,    
    max_latency=0.1,          
    max_messages=100          
)

publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
topic_path = publisher.topic_path(project_id, topic_id)

with open("daydata.json", "r") as f:
    data = json.load(f)

start_time = time.time()
msg_count = 0
futures = []


def flush_futures(pending_futures, batch_number):
    for j, future in enumerate(pending_futures, 1):
        try:
            future.result(timeout=60)
        except Exception as e:
            print(f"❌ Error in future {j} of batch {batch_number}: {e}")


for i, record in enumerate(data, 1):
    try:
        message_data = json.dumps(record).encode("utf-8")
        future = publisher.publish(topic_path, data=message_data)
        futures.append(future)
        msg_count += 1

        if i % 5000 == 0:
            print(f"✅ Published {i} messages so far... flushing batch", flush=True)
            flush_futures(futures, i // 5000)
            futures.clear()

    except Exception as e:
        print(f"❌ Failed to publish message {i}: {e}")


if futures:
    print(f"🧹 Flushing remaining {len(futures)} messages...", flush=True)
    flush_futures(futures, "final")

end_time = time.time()
print(f"\n🚀 Finished publishing {msg_count} messages in {end_time - start_time:.2f} seconds to {topic_path}")
