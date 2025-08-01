CONFIG = {
    "nombre": "Task Updater Service",
    "server": {
        "port": 8002
    },
    "sqs": {
        "endpoint_url": "http://localhost:4566",  # LocalStack default endpoint
        "region_name": "us-east-1",
        "queue_url": "task_consumer_queue",
        "wait_time_seconds": 20,  # Long polling wait time
        "max_number_of_messages": 10  # Max messages to receive in one batch
    }
}
