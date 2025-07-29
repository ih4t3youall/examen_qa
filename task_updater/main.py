from fastapi import FastAPI
import uvicorn
from resources.config import CONFIG

# Importar el servicio de tareas para que se ejecute en segundo plano
import task_service

# Crear aplicación FastAPI
app = FastAPI(title="Task Updater Service")

@app.get("/")
async def root():
    return {"message": "Task Updater Service is running"}

def main():
    print("\n=== Task Updater Service ===")
    print(f"Nombre del servicio: {CONFIG['nombre']}")
    print(f"Servidor FastAPI iniciado en http://localhost:{CONFIG['server']['port']}")
    print("El consumidor de RabbitMQ está ejecutándose en segundo plano")
    print("Presiona CTRL+C para detener el servicio\n")
    
    uvicorn.run(app, host="0.0.0.0", port=CONFIG["server"]["port"])

if __name__ == "__main__":
    main()
