import pika
from resources.config import CONFIG

def get_rabbitmq_connection():
    """Establece una conexión con RabbitMQ"""
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=CONFIG['rabbitmq']['host'],
                port=CONFIG['rabbitmq']['port']
            )
        )
        return connection
    except Exception as e:
        print(f"Error al conectar con RabbitMQ: {str(e)}")
        raise

def send_message_to_queue(message: dict):
    """Envía un mensaje a la cola de RabbitMQ"""
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        
        # Declaramos la cola
        channel.queue_declare(queue=CONFIG['rabbitmq']['queue'])
        
        # Convertimos el mensaje a JSON
        import json
        message_str = json.dumps(message)
        
        # Enviamos el mensaje
        channel.basic_publish(
            exchange='',
            routing_key=CONFIG['rabbitmq']['queue'],
            body=message_str,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Hace que el mensaje sea persistente
            )
        )
        
        print(f"Mensaje enviado a la cola {CONFIG['rabbitmq']['queue']}")
        connection.close()
    except Exception as e:
        print(f"Error al enviar mensaje a RabbitMQ: {str(e)}")
        raise
