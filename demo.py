import boto3
from dotenv import load_dotenv
load_dotenv()                     # reads .env
sqs = boto3.client("sqs")
url = sqs.create_queue(QueueName="demo")["QueueUrl"]
sqs.send_message(QueueUrl=url, MessageBody="puzzle",
    DelaySeconds=20, MessageAttributes={
        "order_no": {"DataType": "String", "StringValue": "1"},
        "word":     {"DataType": "String", "StringValue": "The"}})

resp = sqs.receive_message(QueueUrl=url)   # too early

# To prevent errors, use resp.get("Messages", [])
# Never use resp["Messages"] outright
resp.get("Messages", [])

# ... 20 s later
import time
time.sleep(20)
#resp = sqs.receive_message(QueueUrl=url)   # This will not return anything useful, 
# make sure that you're returning all message attributes with the argument below
# no delete: in flight now, visible again after 30 s. Remember to delete your messages
resp = sqs.receive_message(QueueUrl=url,
                           MessageAttributeNames=["All"])
msg = resp["Messages"][0]
print(msg["MessageAttributes"])
# Always delete with a receipt handle
sqs.delete_message(QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"])
