# worker_service/main.py
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from services.message_consumer import MessageConsumerService

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP
    logger.info("🚀 Starting Worker (LOG ONLY MODE)")
    
    try:
        consumer = MessageConsumerService.get_instance()
        consumer.start()
        app.state.consumer = consumer
        logger.info("✅ Ready!")
    except Exception as e:
        logger.error(f"❌ Startup error: {e}")
    
    yield
    
    # SHUTDOWN
    logger.info("🛑 Shutting down...")
    try:
        if hasattr(app.state, 'consumer'):
            app.state.consumer.stop()
    except Exception as e:
        logger.error(f"❌ Shutdown error: {e}")

app = FastAPI(
    title="Simple Worker Service",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {
        "service": "worker-service",
        "mode": "log-only",
        "status": "running"
    }

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/consumer/status")
def status():
    try:
        consumer = MessageConsumerService.get_instance()
        return {
            "status": "running" if consumer.is_running else "stopped",
            "stats": consumer.get_stats()
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.post("/consumer/start")
def start():
    try:
        consumer = MessageConsumerService.get_instance()
        if not consumer.is_running:
            consumer.start()
            return {"message": "Started"}
        return {"message": "Already running"}
    except Exception as e:
        return {"error": str(e)}

@app.post("/consumer/stop")
def stop():
    try:
        consumer = MessageConsumerService.get_instance()
        if consumer.is_running:
            consumer.stop()
            return {"message": "Stopped"}
        return {"message": "Not running"}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    import os
    
    HOST = os.getenv("WORKER_HOST", "0.0.0.0")
    PORT = int(os.getenv("WORKER_PORT", 8001))
    
    uvicorn.run("main:app", host=HOST, port=PORT, reload=True)