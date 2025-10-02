# worker_service/services/message_consumer.py
"""
Message Consumer Service - Subscribe và xử lý messages từ Redis Pub/Sub
"""
import logging
import time
import threading
from typing import Dict, Any, Optional
from datetime import datetime
from sqlalchemy.orm import Session

from config.redis_pubsub import RedisPubSubManager
from config.config_database import get_db
from services.scan_service import ScanService
from services.fix_service import FixService
from schemas.compliance_result import ComplianceScanRequest
from schemas.fix_execution import ServerFixRequest
from dao.user_dao import UserDAO

logger = logging.getLogger(__name__)


class MessageConsumerService:
    """
    Singleton service để consume messages từ Redis Pub/Sub
    """
    
    _instance: Optional['MessageConsumerService'] = None
    _lock = threading.Lock()
    
    def __init__(self):
        """Initialize Message Consumer Service"""
        if MessageConsumerService._instance is not None:
            raise RuntimeError("Use get_instance() instead")
        
        self.pubsub_manager = RedisPubSubManager()
        self.is_running = False
        self.consumer_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        
        # Statistics
        self.stats = {
            "total_messages_received": 0,
            "scan_requests_processed": 0,
            "fix_requests_processed": 0,
            "errors": 0,
            "started_at": None,
            "last_message_at": None
        }
        
        logger.info("✅ MessageConsumerService initialized")
    
    @classmethod
    def get_instance(cls) -> 'MessageConsumerService':
        """Get singleton instance"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = MessageConsumerService()
        return cls._instance
    
    def start(self):
        """Start consumer thread"""
        if self.is_running:
            logger.warning("⚠️ Consumer is already running")
            return
        
        logger.info("🚀 Starting Message Consumer...")
        
        # Subscribe to channels
        self.pubsub_manager.subscribe_scan_requests()
        self.pubsub_manager.subscribe_fix_requests()
        
        # Start consumer thread
        self.is_running = True
        self.stop_event.clear()
        self.stats["started_at"] = datetime.now().isoformat()
        
        self.consumer_thread = threading.Thread(
            target=self._consume_messages,
            daemon=True,
            name="MessageConsumerThread"
        )
        self.consumer_thread.start()
        
        logger.info("✅ Message Consumer started successfully")
    
    def stop(self, timeout: int = 5):
        """Stop consumer thread"""
        if not self.is_running:
            logger.warning("⚠️ Consumer is not running")
            return
        
        logger.info("🛑 Stopping Message Consumer...")
        
        self.stop_event.set()
        self.is_running = False
        
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=timeout)
        
        self.pubsub_manager.close()
        
        logger.info("✅ Message Consumer stopped")
    
    def _consume_messages(self):
        """Main consumer loop - runs in background thread"""
        logger.info("👂 Listening for messages...")
        
        try:
            for message_data in self.pubsub_manager.listen_for_messages(
                callback=self._handle_message
            ):
                # Check stop signal
                if self.stop_event.is_set():
                    logger.info("⏹️ Stop signal received")
                    break
                
                # Update stats
                self.stats["total_messages_received"] += 1
                self.stats["last_message_at"] = datetime.now().isoformat()
                
        except Exception as e:
            logger.error(f"❌ Error in consumer loop: {e}")
            import traceback
            traceback.print_exc()
            self.stats["errors"] += 1
    
    def _handle_message(self, channel: str, message: Dict[str, Any]):
        """
        Handle received message based on channel
        
        Args:
            channel: Redis channel name
            message: Parsed message data
        """
        try:
            logger.info(f"📨 Processing message from {channel}")
            logger.debug(f"Message: {message}")
            
            message_type = message.get("type")
            message_data = message.get("data", {})
            
            # Route to appropriate handler
            if message_type == "scan_request":
                self._handle_scan_request(message_data)
            elif message_type == "fix_request":
                self._handle_fix_request(message_data)
            else:
                logger.warning(f"⚠️ Unknown message type: {message_type}")
            
        except Exception as e:
            logger.error(f"❌ Error handling message: {e}")
            import traceback
            traceback.print_exc()
            self.stats["errors"] += 1
    
    def _handle_scan_request(self, data: Dict[str, Any]):
        """
        Handle SCAN request
        
        Args:
            data: Scan request data
                - job_id: str
                - instance_ids: Optional[List[int]]
                - batch_size: int
                - user_id: int
                - username: str
        """
        try:
            job_id = data.get("job_id")
            instance_ids = data.get("instance_ids")
            batch_size = data.get("batch_size", 10)
            user_id = data.get("user_id")
            username = data.get("username")
            
            logger.info(f"🔍 Processing SCAN request: {job_id}")
            logger.info(f"   - Instances: {instance_ids or 'ALL'}")
            logger.info(f"   - Batch size: {batch_size}")
            logger.info(f"   - User: {username} (ID: {user_id})")
            
            # Create DB session
            db = next(get_db())
            
            try:
                # Get user from database
                user_dao = UserDAO(db)
                user = user_dao.get_by_id(user_id)
                
                if not user:
                    raise ValueError(f"User {user_id} not found")
                
                # Create scan service and execute
                scan_service = ScanService(db)
                scan_request = ComplianceScanRequest(
                    server_ids=instance_ids,
                    batch_size=batch_size
                )
                
                # Execute scan
                start_time = time.time()
                result = scan_service.start_compliance_scan(scan_request, user)
                execution_time = time.time() - start_time
                
                logger.info(f"✅ SCAN completed in {execution_time:.2f}s")
                logger.info(f"   - Total scanned: {result.total_instances}")
                logger.info(f"   - Started scans: {result.started_scans_count}")
                
                # Publish response
                response_data = {
                    "job_id": job_id,
                    "status": "completed",
                    "total_instances": result.total_instances,
                    "started_scans": result.started_scans_count,
                    "execution_time": execution_time,
                    "timestamp": datetime.now().isoformat()
                }
                
                self.pubsub_manager.publish_scan_response(response_data)
                
                # Update stats
                self.stats["scan_requests_processed"] += 1
                
            finally:
                db.close()
            
        except Exception as e:
            logger.error(f"❌ Error processing SCAN request: {e}")
            import traceback
            traceback.print_exc()
            
            # Publish error response
            try:
                error_response = {
                    "job_id": data.get("job_id"),
                    "status": "failed",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
                self.pubsub_manager.publish_scan_response(error_response)
            except:
                pass
            
            self.stats["errors"] += 1
    
    def _handle_fix_request(self, data: Dict[str, Any]):
        """
        Handle FIX request
        
        Args:
            data: Fix request data
                - job_id: str
                - instance_id: int
                - rule_result_ids: List[int]
                - user_id: int
                - username: str
        """
        try:
            job_id = data.get("job_id")
            instance_id = data.get("instance_id")
            rule_result_ids = data.get("rule_result_ids", [])
            user_id = data.get("user_id")
            username = data.get("username")
            
            logger.info(f"🔧 Processing FIX request: {job_id}")
            logger.info(f"   - Instance ID: {instance_id}")
            logger.info(f"   - Rule results: {len(rule_result_ids)} items")
            logger.info(f"   - User: {username} (ID: {user_id})")
            
            # Create DB session
            db = next(get_db())
            
            try:
                # Get user from database
                user_dao = UserDAO(db)
                user = user_dao.get_by_id(user_id)
                
                if not user:
                    raise ValueError(f"User {user_id} not found")
                
                # Create fix service and execute
                fix_service = FixService(
                    db,
                    user_id=user_id,
                    username=username,
                    ip_address="worker-service",
                    user_agent="MessageConsumer"
                )
                
                fix_request = ServerFixRequest(
                    instance_id=instance_id,
                    rule_result_ids=rule_result_ids
                )
                
                # Execute fix
                start_time = time.time()
                result = fix_service.execute_server_fixes(fix_request, user)
                execution_time = time.time() - start_time
                
                logger.info(f"✅ FIX completed in {execution_time:.2f}s")
                logger.info(f"   - Total fixes: {result.total_fixes}")
                logger.info(f"   - Successful: {result.successful_fixes}")
                logger.info(f"   - Failed: {result.failed_fixes}")
                
                # Publish response
                response_data = {
                    "job_id": job_id,
                    "status": "completed",
                    "instance_id": instance_id,
                    "total_fixes": result.total_fixes,
                    "successful_fixes": result.successful_fixes,
                    "failed_fixes": result.failed_fixes,
                    "skipped_fixes": result.skipped_fixes,
                    "execution_time": execution_time,
                    "timestamp": datetime.now().isoformat()
                }
                
                self.pubsub_manager.publish_fix_response(response_data)
                
                # Update stats
                self.stats["fix_requests_processed"] += 1
                
            finally:
                db.close()
            
        except Exception as e:
            logger.error(f"❌ Error processing FIX request: {e}")
            import traceback
            traceback.print_exc()
            
            # Publish error response
            try:
                error_response = {
                    "job_id": data.get("job_id"),
                    "status": "failed",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
                self.pubsub_manager.publish_fix_response(error_response)
            except:
                pass
            
            self.stats["errors"] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics"""
        uptime = 0
        if self.stats["started_at"]:
            started = datetime.fromisoformat(self.stats["started_at"])
            uptime = (datetime.now() - started).total_seconds()
        
        return {
            **self.stats,
            "is_running": self.is_running,
            "uptime_seconds": uptime
        }