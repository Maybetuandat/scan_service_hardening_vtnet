
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from services.message_consumer import MessageConsumerService




# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager - quản lý startup và shutdown
    """
    # ============ STARTUP ============
    logger.info("🚀 Starting Worker Service...")
    
    try:
        # Create database tables
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Database tables initialized")
        
        # Start Message Consumer Service
        consumer = MessageConsumerService.get_instance()
        consumer.start()
        logger.info("✅ Message Consumer Service started")
        
        # Store consumer in app state
        app.state.consumer = consumer
        
    except Exception as e:
        logger.error(f"❌ Error during startup: {e}")
        import traceback
        traceback.print_exc()
    
    yield
    
    # ============ SHUTDOWN ============
    logger.info("🛑 Shutting down Worker Service...")
    
    try:
        if hasattr(app.state, 'consumer'):
            app.state.consumer.stop()
            logger.info("✅ Message Consumer stopped")
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")


# Create FastAPI app
app = FastAPI(
    title="Security Scan Worker Service",
    version="1.0.0",
    description="Worker service for processing scan and fix jobs from Redis Pub/Sub",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============ HEALTH CHECK ROUTES ============

@app.get("/")
def root():
    """Root endpoint"""
    return {
        "service": "worker-service",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "worker-service",
        "version": "1.0.0"
    }


@app.get("/consumer/status")
def get_consumer_status():
    """Get consumer service status"""
    try:
        consumer = MessageConsumerService.get_instance()
        stats = consumer.get_stats()
        
        return {
            "status": "running" if consumer.is_running else "stopped",
            "stats": stats
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


@app.post("/consumer/start")
def start_consumer():
    """Manually start consumer"""
    try:
        consumer = MessageConsumerService.get_instance()
        if not consumer.is_running:
            consumer.start()
            return {"message": "Consumer started successfully"}
        return {"message": "Consumer is already running"}
    except Exception as e:
        return {"error": str(e)}


@app.post("/consumer/stop")
def stop_consumer():
    """Manually stop consumer"""
    try:
        consumer = MessageConsumerService.get_instance()
        if consumer.is_running:
            consumer.stop()
            return {"message": "Consumer stopped successfully"}
        return {"message": "Consumer is not running"}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn
    import os
    
    HOST = os.getenv("WORKER_HOST", "0.0.0.0")
    PORT = int(os.getenv("WORKER_PORT", 8001))
    
    uvicorn.run(
        "worker_service.main:app",
        host=HOST,
        port=PORT,
        reload=True
    )