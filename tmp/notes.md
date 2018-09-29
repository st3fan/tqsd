GetQueues

GET /queues

[
        {
                "Name": "ResizeJobs",
                "Created": "2018-01-20T12:23:45.123Z",
                "VisibilityTimeout": 180,
                "MessageRetentionPeriod": 10800,
                "DelaySeconds": 300
        },
        ...
]



GetQueue

GET /queues/<QueueName>

{
        "Name": "ResizeJobs",
        "Created": "2018-01-20T12:23:45.123Z",
        "VisibilityTimeout": 180,
        "MessageRetentionPeriod": 10800,
        "DelaySeconds": 300
}



CreateQueue

POST /queues

Request = {
    "Name": "ResizeJobs",
    "VisibilityTimeout": 180,
    "MessageRetentionPeriod": 10800,
    "DelaySeconds": 300,
}



DeleteQueue

DELETE /queues/<QueueName>

{
    "Name": "ResizeJobs"
}



PurgeQueue

DELETE /queues/<QueueName>/messages



SendMessages

POST /queues/<QueueName>/messages

Request = {
    Messages = [
        {
            "Body": "Some content here",
            "Attributes": {
                "SomeString": "Foo",
                "SomeNumber": 42
            }
        },
        ... another message ...
    ]
}

Response = [
    {
        ID
    }
]


ReceiveMessages

GET /queues/<QueueName>/messages
  MaxNumberOfMessages=25
  VisibilityTimeout=60
  WaitTimeSeconds=60

Result = [
    {
        "Message": {
            "Body": "text/plain",
            "Attributes": {
                "SomeString": "Foo",
                "SomeNumber": 42
            }
        },
        "Lease": {
            "ID": "1234567890",
            "Expiration": "2018-01-20T12:23:45.123Z",
        }
    },
    {
        ...
    }
]


DeleteMessage

DELETE /queues/<QueueName>/leases/<LeaseID>
