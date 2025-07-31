import pika
import json
import logging
import threading
from datetime import datetime
from resources.config import CONFIG

# Set up logger
logger = logging.getLogger(__name__)

executed_commands = []

def process_message(channel, method, properties, body):
    try:
        message = json.loads(body)
        task_id = message.get('id', 'unknown')
        action = message.get('action', 'unknown')
        
        logger.info(f"Processing message - Task ID: {task_id}, Action: {action}")
        logger.debug(f"Message details: {message}")
        
        # Process different actions
        if action == 'create':
            logger.info(f"Creating/Updating task - ID: {task_id}")
            logger.info(f"Task Name: {message.get('task_name', 'N/A')}")
            logger.info(f"Description: {message.get('content', 'N/A')}")
            logger.info(f"Assignee: {message.get('asignee', 'N/A')}")
            executed_commands.append(f"Create::: task_id: {task_id},task_name: {message.get('task_name', 'N/A')}, content: {message.get('content', 'N/A')}, asignee: {message.get('asignee', 'N/A')}")
            # Here you would normally update your database
            
        elif action == 'delete':
            logger.info(f"Deleting task - ID: {task_id}")
            executed_commands.append(f"Delete::: task_id:{task_id}")
            # Here you would normally delete from your database
            
        else:
            logger.warning(f"Unknown action received: {action} for task ID: {task_id}")
        
        # Acknowledge the message (this will remove it from the queue)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"Successfully processed message for task ID: {task_id}")
        
    except json.JSONDecodeError as je:
        error_msg = f"Failed to decode message: {str(je)}"
        logger.error(error_msg)
        # Reject the message without requeuing if it's not valid JSON
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        error_msg = f"Error processing message: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # NACK the message to keep it in the queue for retry
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def start_consumer():
    connection = None
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            logger.info(f"Attempting to connect to RabbitMQ (Attempt {retry_count + 1}/{max_retries})")

            # Create RabbitMQ connection with heartbeat and connection timeout
            connection_params = pika.ConnectionParameters(
                host=CONFIG['rabbitmq']['host'],
                port=CONFIG['rabbitmq']['port'],
                heartbeat=60,  # 60 second heartbeat
                connection_attempts=3,
                retry_delay=5
            )

            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()

            # Set QoS prefetch count to control how many messages are consumed at once
            channel.basic_qos(prefetch_count=1)

            # Set up the consumer with the message processing function
            channel.basic_consume(
                queue=CONFIG['rabbitmq']['queue'],
                on_message_callback=process_message,
                consumer_tag='task_updater_consumer'
            )

            logger.info(f"Successfully connected to RabbitMQ")
            logger.info(f"Listening for messages on queue: {CONFIG['rabbitmq']['queue']}")
            logger.info("Press CTRL+C to exit")

            # Start consuming messages
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
            break  # Channel errors are usually not recoverable
            
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

# Start the consumer in a separate thread
logger.info("Starting RabbitMQ consumer thread...")
consumer_thread = threading.Thread(target=start_consumer, daemon=True, name="RabbitMQ-Consumer")
consumer_thread.start()
logger.info("RabbitMQ consumer thread started")
