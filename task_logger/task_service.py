import pika
import json
import logging
import threading
from datetime import datetime
from fastapi import APIRouter
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

def process_message(channel, method, properties, body):
    try:
        message = json.loads(body)
        task_id = message.get('id', 'unknown')
        action = message.get('action', 'unknown')

        logger.info(f"Processing message - Task ID: {task_id}, Action: {action}")

        if action == 'create':
            logger.info(f"Creating/Updating task - ID: {task_id}")
            logger.info(f"Task Name: {message.get('task_name', 'N/A')}")
            logger.info(f"Description: {message.get('content', 'N/A')}")
            logger.info(f"Assignee: {message.get('asignee', 'N/A')}")
            executed_commands.append(
                f"Create::: task_id: {task_id}, task_name: {message.get('task_name', 'N/A')}, content: {message.get('content', 'N/A')}, asignee: {message.get('asignee', 'N/A')}"
            )
        elif action == 'delete':
            logger.info(f"Deleting task - ID: {task_id}")
            executed_commands.append(f"Delete::: task_id: {task_id}")
        else:
            logger.warning(f"Unknown action received: {action} for task ID: {task_id}")

        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"Successfully processed message for task ID: {task_id}")

    except json.JSONDecodeError as je:
        logger.error(f"Failed to decode message: {str(je)}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}", exc_info=True)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def start_consumer():
    connection = None
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            logger.info(f"Attempting to connect to RabbitMQ (Attempt {retry_count + 1}/{max_retries})")

            connection_params = pika.ConnectionParameters(
                host=CONFIG['rabbitmq']['host'],
                port=CONFIG['rabbitmq']['port'],
                heartbeat=60,
                connection_attempts=3,
                retry_delay=5
            )

            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()

            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(
                queue=CONFIG['rabbitmq']['queue'],
                on_message_callback=process_message,
                consumer_tag='task_updater_consumer'
            )

            logger.info(f"Successfully connected to RabbitMQ")
            logger.info(f"Listening for messages on queue: {CONFIG['rabbitmq']['queue']}")
            logger.info("Press CTRL+C to exit")

            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            retry_count += 1
            if retry_count >= max_retries:
                logger.error(f"Failed to connect to RabbitMQ after {max_retries} attempts: {str(e)}")
                break
            logger.warning(f"Connection attempt {retry_count} failed. Retrying in 5 seconds...")
            import time
            time.sleep(5)

        except pika.exceptions.AMQPChannelError as e:
            logger.error(f"Channel error: {str(e)}")
            break

        except pika.exceptions.AMQPError as e:
            logger.error(f"AMQP error: {str(e)}")
            retry_count += 1
            if retry_count >= max_retries:
                logger.error(f"Max retries reached. Giving up.")
                break

        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}", exc_info=True)
            break

        finally:
            if connection and connection.is_open:
                try:
                    connection.close()
                    logger.info("Closed RabbitMQ connection")
                except Exception as e:
                    logger.error(f"Error closing connection: {str(e)}")

