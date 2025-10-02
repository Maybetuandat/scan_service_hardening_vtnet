# worker_service/services/message_consumer.py
"""
Simple Message Consumer - CHỈ nhận và log messages
"""
import logging
import threading
from typing import Dict, Any, Optional
from datetime import datetime
from config.redis_pubsub import RedisPubSubManager

logger = logging.getLogger(__name__)

class MessageConsumerService:
    _instance: Optional['MessageConsumerService'] = None
    _lock = threading.Lock()
    
    def __init__(self):
        if MessageConsumerService._instance is not None:
            raise RuntimeError("Use get_instance() instead")
        
        self.pubsub_manager = RedisPubSubManager()
        self.is_running = False
        self.consumer_thread = None
        self.stop_event = threading.Event()
        
        self.stats = {
            "total_messages": 0,
            "scan_requests": 0,
            "fix_requests": 0,
            "errors": 0,
            "started_at": None,
            "last_message_at": None
        }
        
        logger.info("✅ Consumer initialized (LOG ONLY MODE)")
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = MessageConsumerService()
        return cls._instance
    
    def start(self):
        if self.is_running:
            logger.warning("⚠️ Already running")
            return
        
        logger.info("🚀 Starting consumer...")
        
        # Subscribe channels
        self.pubsub_manager.subscribe_scan_requests()
        self.pubsub_manager.subscribe_fix_requests()
        
        self.is_running = True
        self.stop_event.clear()
        self.stats["started_at"] = datetime.now().isoformat()
        
        self.consumer_thread = threading.Thread(
            target=self._consume_messages,
            daemon=True
        )
        self.consumer_thread.start()
        
        logger.info("✅ Consumer started!")
    
    def stop(self, timeout=5):
        if not self.is_running:
            return
        
        logger.info("🛑 Stopping...")
        self.stop_event.set()
        self.is_running = False
        
        if self.consumer_thread:
            self.consumer_thread.join(timeout=timeout)
        
        self.pubsub_manager.close()
        logger.info("✅ Stopped")
    
    def _consume_messages(self):
        logger.info("👂 Listening...")
        
        try:
            for message_data in self.pubsub_manager.listen_for_messages(
                callback=self._handle_message
            ):
                if self.stop_event.is_set():
                    break
                
                self.stats["total_messages"] += 1
                self.stats["last_message_at"] = datetime.now().isoformat()
                
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            self.stats["errors"] += 1
    
    def _handle_message(self, channel: str, message: Dict[str, Any]):
        try:
            msg_type = message.get("type")
            data = message.get("data", {})
            
            if msg_type == "scan_request":
                self._log_scan(data)
                self.stats["scan_requests"] += 1
            elif msg_type == "fix_request":
                self._log_fix(data)
                self.stats["fix_requests"] += 1
            else:
                logger.warning(f"⚠️ Unknown type: {msg_type}")
                
        except Exception as e:
            logger.error(f"❌ Handle error: {e}")
            self.stats["errors"] += 1
    
    def _log_scan(self, data: Dict):
        import json
        logger.info("\n" + "="*80)
        logger.info("📨 SCAN REQUEST")
        logger.info("="*80)
        logger.info(f"Job ID:       {data.get('job_id')}")
        logger.info(f"User:         {data.get('username')} (ID: {data.get('user_id')})")
        logger.info(f"Instances:    {data.get('instance_ids') or 'ALL'}")
        logger.info(f"Batch:        {data.get('batch_size')}")
        logger.info(f"Time:         {data.get('timestamp')}")
        logger.info("-"*80)
        logger.info(json.dumps(data, indent=2))
        logger.info("="*80 + "\n")
    
    def _log_fix(self, data: Dict):
        import json
        logger.info("\n" + "="*80)
        logger.info("🔧 FIX REQUEST")
        logger.info("="*80)
        logger.info(f"Job ID:       {data.get('job_id')}")
        logger.info(f"User:         {data.get('username')} (ID: {data.get('user_id')})")
        logger.info(f"Instances:    {data.get('instance_ids')}")
        logger.info(f"Rules:        {data.get('rule_ids')}")
        logger.info(f"Time:         {data.get('timestamp')}")
        logger.info("-"*80)
        logger.info(json.dumps(data, indent=2))
        logger.info("="*80 + "\n")
    
    def get_stats(self):
        return {**self.stats, "is_running": self.is_running}