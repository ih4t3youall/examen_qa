import pika
from resources.config import CONFIG
import threading
import json

def process_message(channel, method, properties, body):
    try:
        message = json.loads(body)
        print(f"\nMensaje recibido: {message}")
        # Acknowledge the message (this will remove it from the queue)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error procesando el mensaje: {str(e)}")
        # NACK the message to keep it in the queue
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def start_consumer():
    connection = None
    try:
        # Create RabbitMQ connection
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=CONFIG['rabbitmq']['host'],
                port=CONFIG['rabbitmq']['port']
            )
        )
        channel = connection.channel()
        
        # Declare the queue (it will be created if it doesn't exist)
        channel.queue_declare(queue=CONFIG['rabbitmq']['queue'])
        
        # Set up the consumer
        channel.basic_consume(
            queue=CONFIG['rabbitmq']['queue'],
            on_message_callback=process_message
        )
        
        print(f"\n[*] Esperando mensajes en la cola {CONFIG['rabbitmq']['queue']}. Para salir presione CTRL+C")
        channel.start_consuming()
    except Exception as e:
        print(f"Error en el consumidor: {str(e)}")
        if connection:
            connection.close()

# Start the consumer in a separate thread
consumer_thread = threading.Thread(target=start_consumer, daemon=True)
consumer_thread.start()
