import logging
import os
import sys
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel
from typing import Optional, List, Dict
import uuid
from utils import send_message_to_queue

# Configure logging
log_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
log_file = os.path.join(log_dir, 'task_consumer.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
logger.info('=' * 80)
logger.info(f'Starting Task Consumer at {datetime.now()}')
logger.info('=' * 80)

app = FastAPI()

# Set up templates
templates_dir = os.path.join(os.path.dirname(__file__), "templates")
os.makedirs(templates_dir, exist_ok=True)
templates = Jinja2Templates(directory=templates_dir)

# Set up static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# In-memory storage for tasks (for demo purposes)
tasks_db: Dict[str, Dict] = {}

class Task(BaseModel):
    task_name: str
    content: str
    asignee: str

class TaskUpdate(BaseModel):
    task_name: Optional[str] = None
    content: Optional[str] = None
    asignee: Optional[str] = None

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "tasks": list(tasks_db.values())
    })

@app.post("/tasks")
async def create_task(
    request: Request,
    task_name: str = Form(...),
    content: str = Form(...),
    asignee: str = Form(...)
):
    task_id = str(uuid.uuid4())
    
    # Log task creation
    logger.info(f"Creating new task - ID: {task_id}")
    logger.info(f"Task Name: {task_name}")
    logger.info(f"Content: {content}")
    logger.info(f"Assignee: {asignee}")
    
    # Guardar en memoria
    task = {
        "id": task_id,
        "task_name": task_name,
        "content": content,
        "asignee": asignee
    }
    tasks_db[task_id] = task
    
    # Enviar a la cola de mensajes
    try:
        logger.info(f"Sending task {task_id} to message queue")
        send_message_to_queue({
            **task,
            "action": "create"
        })
        logger.info(f"Message for task {task_id} sent to queue successfully")
    except Exception as e:
        error_msg = f"Error sending task {task_id} to queue: {str(e)}"
        logger.error(error_msg)
    
    logger.info(f"Task {task_id} created successfully")
    return RedirectResponse(url="/?message=Tarea%20creada%20correctamente&message_type=success", status_code=303)

@app.post("/tasks/{task_id}/delete")
async def delete_task(task_id: str):
    if task_id in tasks_db:
        task = tasks_db[task_id]
        logger.info(f"Deleting task - ID: {task_id}")
        logger.info(f"Task Name: {task['task_name']}")
        logger.info(f"Assignee: {task['asignee']}")
        
        # Enviar a la cola de mensajes
        try:
            logger.info(f"Sending delete message for task {task_id} to message queue")
            send_message_to_queue({
                "id": task_id,
                "action": "delete"
            })
            logger.info(f"Delete message for task {task_id} sent to queue successfully")
        except Exception as e:
            error_msg = f"Error sending delete message for task {task_id} to queue: {str(e)}"
            logger.error(error_msg)
        
        # Eliminar de la base de datos en memoria
        del tasks_db[task_id]
        logger.info(f"Task {task_id} deleted from memory")
        
        return RedirectResponse(url="/?message=Tarea%20eliminada%20correctamente&message_type=success", status_code=303)
    else:
        error_msg = f"Task not found - ID: {task_id}"
        logger.error(error_msg)
        raise HTTPException(status_code=404, detail=error_msg)
