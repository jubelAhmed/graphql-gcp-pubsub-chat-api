import json
from ariadne import ObjectType, convert_kwargs_to_snake_case
from google.cloud import pubsub_v1
from concurrent import futures

from app.store import users, messages, queues


from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())

mutation = ObjectType("Mutation")

project_id = "graphql-gcp"
topic_id = "app-messaging"


    
@mutation.field("createUser")
@convert_kwargs_to_snake_case
async def resolve_create_user(obj, info, username):
    try:
        if not users.get(username):
            user = {
                "user_id": len(users) + 1,
                "username": username
            }
            users[username] = user
            return {
                "success": True,
                "user": user
            }
        return {
            "success": False,
            "errors": ["Username is taken"]
        }

    except Exception as error:
        return {
            "success": False,
            "errors": [str(error)]
        }

def get_callback(future, message):
    def callback(future):
        try:
            print("Published message %s.", future.result(timeout=1))
        except futures.TimeoutError as exc:
            print("Publishing %s timeout: %r", message, exc)
    return callback

@mutation.field("createMessage")
@convert_kwargs_to_snake_case
async def resolve_create_message(obj, info, content, sender_id, recipient_id):
    print("call create message")
    try:
        message = {
            "content": content,
            "sender_id": sender_id,
            "recipient_id": recipient_id
        }
        messages.append(message)
        data_str = json.dumps(message)
        data = data_str.encode("utf-8")
        
        publisher = pubsub_v1.PublisherClient()
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/topics/{topic_id}`
        topic_path = publisher.topic_path(project_id, topic_id)
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, data)
        # future.add_done_callback(get_callback(future, data))

        # print("future")
        print(future.result())
        return {
            "success": True,
            "message": message
        }
    except Exception as error:
        return {
            "success": False,
            "errors": [str(error)]
        }
