CONFIG = {
    "nombre": "Task Consumer Service",
    "server": {
        "port": 8001
    },
    "sqs": {
        "endpoint_url": "http://localhost:4566",
        "region_name": "us-east-1",
        "queue_name": "task_consumer_queue",
        "queue_url":"http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/task_consumer_queue"
    }
}
