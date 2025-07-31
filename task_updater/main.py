import logging
import os
import sys
from datetime import datetime
from fastapi import FastAPI
import uvicorn
import threading

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

from task_service import router as task_router, start_consumer

# Crear aplicación FastAPI
app = FastAPI(title="Task Updater Service")

# Incluir las rutas del task_service
app.include_router(task_router, prefix="/api")

@app.get("/")
async def root():
    return {
        "message": "Task Updater Service is running",
        "endpoints": [
            "/api/getCreatedTasks - GET: Get all created tasks",
            "/api/getDeletedTasks - GET: Get all deleted tasks",
            "/api/getAllTasks - GET: Get all tasks and operations"
        ]
    }

def main():
    # Ensure we're using port 8002
    port = 8002
    logger.info('=' * 80)
    logger.info(f'Starting Task Updater Service at {datetime.now()}')
    logger.info(f'Service Name: {CONFIG["nombre"]}')
    logger.info(f'Server running on http://localhost:{port}')
    
    # Start RabbitMQ consumer in a separate thread
    consumer_thread = threading.Thread(target=start_consumer, daemon=True)
    consumer_thread.start()
    logger.info('RabbitMQ consumer started in background thread')
    
    logger.info('=' * 80)
    
    try:
        uvicorn.run(app, host="0.0.0.0", port=port)
    except Exception as e:
        logger.error(f"Error starting server: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
