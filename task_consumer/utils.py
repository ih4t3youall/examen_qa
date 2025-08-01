import boto3
import json
from botocore.config import Config
from resources.config import CONFIG

def get_sqs_client():
    """Crea y retorna un cliente de SQS"""
    try:
        # Configuración del cliente SQS para usar localstack
        sqs = boto3.client(
            'sqs',
            endpoint_url=CONFIG['sqs']['endpoint_url'],
            region_name=CONFIG['sqs']['region_name'],
            aws_access_key_id='test',
            aws_secret_access_key='test',
            config=Config(
                retries={
                    'max_attempts': 3,
                    'mode': 'standard'
                }
            )
        )
        return sqs
    except Exception as e:
        print(f"Error al crear el cliente de SQS: {str(e)}")
        raise

def send_message_to_queue(message: dict):
    """Envía un mensaje a la cola de SQS"""
    try:
        sqs = get_sqs_client()
        
        # Convertimos el mensaje a JSON
        message_str = json.dumps(message)
        
        # Enviamos el mensaje a la cola
        response = sqs.send_message(
            QueueUrl=CONFIG['sqs']['queue_url'],
            MessageBody=message_str,
            MessageAttributes={
                'ContentType': {
                    'DataType': 'String',
                    'StringValue': 'application/json'
                }
            }
        )
        
        print(f"Mensaje enviado a la cola {CONFIG['sqs']['queue_name']}. MessageId: {response['MessageId']}")
        return response
    except Exception as e:
        print(f"Error al enviar mensaje a SQS: {str(e)}")
        raise
