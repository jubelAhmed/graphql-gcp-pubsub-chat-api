from concurrent.futures import TimeoutError
import json
from tokenize import String
from google.cloud import pubsub_v1

from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())

project_id = "graphql-gcp"
subscription_id = "app-messaging-sub"
# Number of seconds the subscriber should listen for messages
timeout = 5.0

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message):
    print(f"Received {message}.")
    message.ack()
    print(message.data.decode('ascii'))
    # yield message.data.decode('ascii')

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
# while True:

 # Block until the shutdown is complete.

def main():
    streaming_pull_future.result()
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            print("new")
            streaming_pull_future.result()
           
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result() 
            
print(main())