import os
import time
import boto3
import json


sns = boto3.client("sns")
sqs = boto3.client('sqs')
p3_queue_url = os.environ.get('SQS_QUEUE_URL_P3')

topic_arn = os.environ.get('TOPIC_ARN')

#-------------------------------------------------------------------------
def retrieve_messages_from_queue():
    response = sqs.receive_message(
        QueueUrl=p3_queue_url,
        MessageAttributeNames=['All'],
        MaxNumberOfMessages=10,
        VisibilityTimeout=30,
        WaitTimeSeconds=5
    )

    return response.get("Messages", [])
#-------------------------------------------------------------------------


#-------------------------------------------------------------------------
def send_ticket_to_sns(messages):
    for message in messages:
        id = message['MessageAttributes']['TicketID']['StringValue']
        title = message['MessageAttributes']['Title']['StringValue']
        description = message['Body']
        priority = message['MessageAttributes']['Priority']['StringValue']
        createdAt = message['MessageAttributes']['CreatedAt']['StringValue']

        email_body = "----------------------------------------------------------------------------------------------------------------"
        email_body += f"\nTitle: {title}"
        email_body += f"\nPriority {priority}"
        email_body += f"\nCreated At {createdAt}"
        email_body += f"\nDescription {description}"
        email_body += f"\n----------------------------------------------------------------------------------------------------------------"


        response = sns.publish(
            TopicArn=topic_arn,
            Message=email_body.strip(),
            Subject=f"{id}: {title}"  
        )

        sqs.delete_message(
            QueueUrl=p3_queue_url,
            ReceiptHandle=message['ReceiptHandle']
        )
#-------------------------------------------------------------------------


#-------------------------------------------------------------------------
def process_message():
    while True:
        messages = retrieve_messages_from_queue()
        print(f"Retrieved {len(messages)} messages from SQS")
        if messages:
            send_ticket_to_sns(messages)
        else:
            time.sleep(5)
#-------------------------------------------------------------------------

if __name__ == "__main__":
    process_message()
