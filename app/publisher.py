import json
from google.cloud import pubsub_v1

from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())

project_id = "graphql-gcp"
topic_id = "app-messaging"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

for n in range(22, 23):
    message = {
            "content": "content 2",
            "sender_id": "1",
            "recipient_id": "2"
        }
   
    data_str = json.dumps(message)
    data = data_str.encode("utf-8")

    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data)
    print(future.result())

print(f"Published messages to {topic_path}.")