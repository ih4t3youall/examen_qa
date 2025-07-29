from resources.config import CONFIG
import uvicorn
from task_service import app

def main():
    print("Task Consumer iniciado")
    print(f"Nombre del servicio: {CONFIG['nombre']}")
    print("Iniciando servidor FastAPI...")
    uvicorn.run(app, host="0.0.0.0", port=CONFIG["server"]["port"])

if __name__ == "__main__":
    main()
