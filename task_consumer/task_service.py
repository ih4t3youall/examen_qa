from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from utils import send_message_to_queue

app = FastAPI()

class Task(BaseModel):
    task_name: str
    content: str
    asignee: str

class TaskUpdate(BaseModel):
    task_name: Optional[str] = None
    content: Optional[str] = None
    asignee: Optional[str] = None

@app.post("/tasks")
async def create_task(task: Task):
    print(f"Creando tarea:")
    print(f"Nombre: {task.task_name}")
    print(f"Contenido: {task.content}")
    print(f"Asignado a: {task.asignee}")
    
    # Preparar el mensaje para RabbitMQ
    message = {
        "task_name": task.task_name,
        "content": task.content,
        "asignee": task.asignee,
        "timestamp": "2025-07-29T19:40:02-03:00"
    }
    
    try:
        send_message_to_queue(message)
        return {"message": "Tarea creada y enviada a la cola"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/tasks/{task_id}")
async def update_task(task_id: int, task_update: TaskUpdate):
    print(f"Actualizando tarea con ID {task_id}")
    print(f"Nuevos datos:")
    print(f"Nombre: {task_update.task_name}")
    print(f"Contenido: {task_update.content}")
    print(f"Asignado a: {task_update.asignee}")
    return {"message": "Tarea actualizada"}

@app.delete("/tasks/{task_id}")
async def delete_task(task_id: int):
    print(f"Eliminando tarea con ID {task_id}")
    return {"message": "Tarea eliminada"}
