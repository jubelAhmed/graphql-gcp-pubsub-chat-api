import asyncio
from google.api_core import retry
from google.cloud import pubsub_v1


from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())

project_id = "graphql-gcp"
subscription_id = "app-messaging-sub"



NUM_MESSAGES = 3

# Wrap the subscriber in a 'with' block to automatically call close() to
# close the underlying gRPC channel when done.

while True:
    print("checking")
    with  pubsub_v1.SubscriberClient() as subscriber:

        subscription_path = subscriber.subscription_path(project_id, subscription_id)
        # The subscriber pulls a specific number of messages. The actual
        # number of messages pulled may be smaller than max_messages.
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": NUM_MESSAGES},
            retry=retry.Retry(deadline=300),
        )

        # if len(response.received_messages) == 0:
        #     return

        ack_ids = []
        for received_message in response.received_messages:
            print(f"Received: {received_message.message.data}.")
            # yield received_message.message.data
            # await displayMessage(received_message.message.data)
            ack_ids.append(received_message.ack_id)

        # Acknowledges the received messages so they will not be sent again.
        if len(ack_ids) > 0:
            subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )

        print(
            f"Received and acknowledged {len(response.received_messages)} messages from {subscription_path}."
        )
            
        
# main()
