import logging
import os
import sys
from datetime import datetime
from fastapi import FastAPI
import uvicorn
from resources.config import CONFIG

# Configure logging
log_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_file = os.path.join(log_dir, 'task_updater.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Importar el servicio de tareas para que se ejecute en segundo plano
import task_service

# Crear aplicación FastAPI
app = FastAPI(title="Task Updater Service")

@app.get("/")
async def root():
    return {"message": "Task Updater Service is running"}

def main():
    logger.info('=' * 80)
    logger.info(f'Starting Task Updater Service at {datetime.now()}')
    logger.info(f'Service Name: {CONFIG["nombre"]}')
    logger.info(f'Server running on http://localhost:{CONFIG["server"]["port"]}')
    logger.info('RabbitMQ consumer is running in the background')
    logger.info('=' * 80)
    
    try:
        uvicorn.run(app, host="0.0.0.0", port=CONFIG["server"]["port"])
    except Exception as e:
        logger.error(f"Error starting server: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
