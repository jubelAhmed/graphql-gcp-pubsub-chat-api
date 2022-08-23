import asyncio
from logging import exception
import time
from tkinter import E
from ariadne import convert_kwargs_to_snake_case, SubscriptionType
from google.api_core import retry

from app.store import queues

subscription = SubscriptionType()

from concurrent.futures import TimeoutError
import json
from tokenize import String
from google.cloud import pubsub_v1

from dotenv import load_dotenv,find_dotenv
from app.store import queues
load_dotenv(find_dotenv())

project_id = "graphql-gcp"
subscription_id = "app-messaging-sub"
# Number of seconds the subscriber should listen for messages
timeout = 4

from multiprocessing import Process




def callback(message:pubsub_v1.subscriber.message.Message):
    # print(msg)
    # message = pubsub_v1.subscriber.message.Message
    print(f"New Received {message}.")
    message.ack()
    msg = message.data.decode('ascii')
    jd= json.loads(msg)
    async def test():
        print("calling")
        print(queues)
        print(len(queues))
        for queue in queues:
            await queue.put(jd)
    print(jd)
    asyncio.run(test())
    print("run")
    time.sleep(2)
    # yield jd
           
            
NUM_MESSAGES = 1

# @subscription.source("messages")
# @convert_kwargs_to_snake_case
def consumePayload(user_id):
    msg = {
        "content":"hello",
        "sender_id":"1",
        "recipient_id":"1"
    }
    yield msg
        
def loadAsync():
    print("loading")
    pass      
def consumePayloadAsync(queues):
    # print("loading a")
    # time.sleep(3)
    # print("loading b")
    subscriber = pubsub_v1.SubscriberClient()
           
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    # When `timeout` is not set, result() will block indefinitely,
    # unless an exception is encountered first.
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    
    with subscriber:
        try:
            print("dsafdsaf")
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result() 
        
      

            
# @subscription.source("messages")
# @convert_kwargs_to_snake_case        
# async def messages_source(obj, info, user_id):
#     # while True:
#     queue = asyncio.Queue()
#     queues.append(queue) 
#     p1 = Process(target=consumePayloadAsync(queues))
#     p1.start()
   
#     print(len(queues))
#     try:
#         while True:
#             print('listen')
#             print(queues)
#             message = await queue.get()
#             queue.task_done()
#             if message["recipient_id"] == user_id:
#                 yield message
#     except asyncio.CancelledError:
#         queues.remove(queue)
#         raise
    # msg = {
    #     "content":"hello",
    #     "sender_id":"1",
    #     "recipient_id":"1"
    # }
    # yield msg
    # print("sdfdf")
    # p2 = Process(target=loadAsync)
    # p2.start()
    # p1.join()
    # p2.join()

from asgiref.sync import sync_to_async

def blocking_function(seconds: int,subscriber,subscription_path):
    # time.sleep(seconds)
    try:
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": NUM_MESSAGES}, retry=retry.Retry(deadline=300),
        )
        return response
    except Exception as e:
        print(e)
    return f"Finished in {seconds} seconds"

@subscription.source("messages")
@convert_kwargs_to_snake_case        
async def messages_source(obj, info, user_id):
    
    
    while True:
      
        print("looping")
   
        # subscriber = pubsub_v1.SubscriberClient() 
        
        # The subscriber pulls a specific number of messages. The actual
        # number of messages pulled may be smaller than max_messages.
        with  pubsub_v1.SubscriberClient() as subscriber:
            subscription_path = subscriber.subscription_path(project_id, subscription_id)
            response = {}
            # try:
            #     response = subscriber.pull(
            #         request={"subscription": subscription_path, "max_messages": NUM_MESSAGES}, retry=retry.Retry(deadline=300),
            #     )
            # except Exception as e:
            #     print(e)
            response = await sync_to_async(blocking_function)(2,subscriber,subscription_path)
            
            if not "received_messages" in response or len(response.received_messages) == 0:
                time.sleep(3)
                continue
            print("response")
            print(response)
            # if len(response.received_messages) == 0:
            #     return

            ack_ids = []
            messages = []
            for received_message in response.received_messages:
                message = received_message.message.data
                msg = message.decode('ascii')
                jd= json.loads(msg)
                print(jd)
                messages.append(jd)
                print(f"Received: {message}.")
                ack_ids.append(received_message.ack_id)
                # yield jd

            # Acknowledges the received messages so they will not be sent again.
            if len(ack_ids) > 0:
                subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
                )
            
            print(
                f"Received and acknowledged {len(response.received_messages)} messages from {subscription_path}."
            )
            
        
            for message in messages:
                if message["recipient_id"] == user_id:
                    yield message
                    await asyncio.sleep(3)
        print("sad")
        await asyncio.sleep(3)
    
@subscription.field("messages")
@convert_kwargs_to_snake_case
async def messages_resolver(message, info, user_id):
    return message