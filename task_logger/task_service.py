import json
import logging
import threading
import time
import boto3
from datetime import datetime
from fastapi import APIRouter
from botocore.config import Config
from resources.config import CONFIG

__all__ = ['router', 'start_consumer']

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("task_service")

executed_commands = []

# Create router instead of FastAPI app
router = APIRouter()

@router.get("/")
async def root():
    return {"message": "Task Updater API is working!"}

@router.get("/getCreatedTasks")
async def get_created_tasks():
    created_tasks = calculate_created_tasks()
    return {"status": "success", "count": len(created_tasks), "tasks": created_tasks}

def calculate_created_tasks():
    return [cmd for cmd in executed_commands if cmd.startswith("Create")]

def calculate_deleted_tasks():
    return [cmd for cmd in executed_commands if cmd.startswith("Delete")]


@router.get("/getDeletedTasks")
async def get_deleted_tasks():
    deleted_tasks = calculate_deleted_tasks()
    return {"status": "success", "count": len(deleted_tasks), "tasks": deleted_tasks}

@router.get("/getAllTasks")
async def get_all_tasks():
    created_tasks = calculate_created_tasks()
    deleted_tasks = calculate_deleted_tasks()
    return {
        "status": "success",
        "stats": {
            "total_created": len(created_tasks),
            "total_deleted": len(deleted_tasks)
        },
        "created_tasks": created_tasks,
        "deleted_tasks": deleted_tasks,
        "all_operations": executed_commands
    }

def process_message(message):
    try:
        # SQS messages are in the 'body' field and need to be parsed from JSON string
        message_body = json.loads(message['Body'])
        task_id = message_body.get('id', 'unknown')
        action = message_body.get('action', 'unknown')

        logger.info(f"Processing message - Task ID: {task_id}, Action: {action}")

        if action == 'create':
            logger.info(f"Creating/Updating task - ID: {task_id}")
            logger.info(f"Task Name: {message_body.get('task_name', 'N/A')}")
            logger.info(f"Description: {message_body.get('content', 'N/A')}")
            logger.info(f"Assignee: {message_body.get('asignee', 'N/A')}")
            logger.info(f"Status: {message_body.get('status', 'N/A')}")
            executed_commands.append(f"Create task {task_id}")
        elif action == 'delete':
            logger.info(f"Deleting task - ID: {task_id}")
            executed_commands.append(f"Delete task {task_id}")
        else:
            logger.warning(f"Unknown action received: {action} for task ID: {task_id}")

        logger.info(f"Successfully processed message for task ID: {task_id}")
        return True

    except json.JSONDecodeError as je:
        logger.error(f"Failed to decode message: {str(je)}")
        return False
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}", exc_info=True)
        return False

def get_sqs_client():
    """Create and return an SQS client with the configured settings."""
    return boto3.client(
        'sqs',
        endpoint_url=CONFIG['sqs']['endpoint_url'],
        region_name=CONFIG['sqs']['region_name'],
        aws_access_key_id="test",  # <<< AÑADIR ESTO
        aws_secret_access_key="test",  # <<< AÑADIR ESTO
        config=Config(
            retries={
                'max_attempts': 3,
                'mode': 'standard'
            }
        )
    )

def get_queue_url(sqs_client):
    """Get or create the SQS queue and return its URL."""
    try:
        response = sqs_client.get_queue_url(QueueName=CONFIG['sqs']['queue_url'])
        return response['QueueUrl']
    except sqs_client.exceptions.QueueDoesNotExist:
        logger.info(f"Queue {CONFIG['sqs']['queue_url']} does not exist. Creating it...")
        response = sqs_client.create_queue(QueueName=CONFIG['sqs']['queue_url'])
        return response['QueueUrl']

def start_consumer():
    max_retries = 3
    retry_count = 0
    sqs_client = None
    queue_url = None

    while retry_count < max_retries:
        try:
            logger.info(f"Attempting to connect to SQS (Attempt {retry_count + 1}/{max_retries})")
            
            # Initialize SQS client
            sqs_client = get_sqs_client()
            
            # Get or create queue URL
            queue_url = get_queue_url(sqs_client)
            
            logger.info(f"Successfully connected to SQS")
            logger.info(f"Listening for messages on queue: {queue_url}")
            logger.info("Press CTRL+C to exit")
            
            # Main message polling loop
            while True:
                try:
                    # Receive messages from SQS
                    response = sqs_client.receive_message(
                        QueueUrl=queue_url,
                        AttributeNames=['All'],
                        MaxNumberOfMessages=CONFIG['sqs']['max_number_of_messages'],
                        WaitTimeSeconds=CONFIG['sqs']['wait_time_seconds']
                    )
                    
                    if 'Messages' in response:
                        for message in response['Messages']:
                            # Process the message
                            success = process_message(message)
                            
                            # Delete the processed message from the queue
                            if success:
                                sqs_client.delete_message(
                                    QueueUrl=queue_url,
                                    ReceiptHandle=message['ReceiptHandle']
                                )
                                logger.debug(f"Deleted message: {message['MessageId']}")
                            else:
                                logger.warning(f"Message processing failed, keeping in queue: {message['MessageId']}")
                    
                except Exception as e:
                    logger.error(f"Error processing messages: {str(e)}", exc_info=True)
                    time.sleep(5)  # Wait before retrying
                    continue

        except Exception as e:
            logger.error(f"Error in SQS consumer: {str(e)}", exc_info=True)
            retry_count += 1
            if retry_count >= max_retries:
                logger.error(f"Max retries reached. Giving up.")
                break
            logger.warning(f"Connection attempt {retry_count} failed. Retrying in 5 seconds...")
            time.sleep(5)
            
        finally:
            if sqs_client:
                try:
                    # SQS client doesn't need explicit cleanup, but we can log the disconnection
                    logger.info("Disconnected from SQS")
                except Exception as e:
                    logger.error(f"Error disconnecting from SQS: {str(e)}")
                    logger.error(f"Error closing connection: {str(e)}")

