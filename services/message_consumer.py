import logging
import threading
from typing import Dict, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from config.redis_pubsub import get_pubsub_manager, RedisPubSubManager

from schemas.scan_message import ScanInstanceMessage, ScanResponseMessage
from services.scan_service import ScanService # Import ScanService

logger = logging.getLogger(__name__)

class MessageConsumerService:
    _instance: Optional['MessageConsumerService'] = None
    _lock = threading.Lock()
    
    def __init__(self):
        if MessageConsumerService._instance is not None:
            raise RuntimeError("Use get_instance() instead")
        
        self.pubsub_manager = get_pubsub_manager() # Lấy singleton PubSubManager
        self.is_running = False
        self.consumer_thread = None
        self.stop_event = threading.Event()
        
        # Tạo ThreadPoolExecutor dùng chung cho toàn bộ ứng dụng, giới hạn 10 luồng
        self.max_workers = 10
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        self._warm_up_threads() # Khởi động các luồng
        
        # Khởi tạo ScanService với thread_pool chung và pubsub_manager
        # ScanService KHÔNG còn cần db_engine
        self.scan_service = ScanService(self.thread_pool, self.pubsub_manager) 
        
        self.stats = {
            "total_messages_received": 0,
            "scan_requests_processed": 0,
            "scan_responses_received": 0, # Thêm thống kê response
            "fix_requests_processed": 0,
            "fix_responses_received": 0, # Thêm thống kê response
            "errors": 0,
            "started_at": None,
            "last_message_at": None,
            "active_scan_tasks": 0, 
            "pending_scan_tasks": 0 
        }
        
        logger.info("✅ Consumer initialized (with integrated ScanService and ThreadPool, DB-less for worker)")
    
    def _warm_up_threads(self):
        def dummy_task():
            import time
            time.sleep(0.01)  
            return "warmed"
        futures = [self.thread_pool.submit(dummy_task) for _ in range(self.max_workers)]
        for future in futures:
            future.result()
        logger.info(f"Thread pool warmed up with {self.max_workers} threads")

    @classmethod
    def get_instance(cls): 
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = MessageConsumerService()
        return cls._instance
    
    def start(self):
        if self.is_running:
            logger.warning("⚠️ Consumer already running.")
            return
        
        logger.info("🚀 Starting consumer...")
        
        # Đăng ký lắng nghe cả request và response
        self.pubsub_manager.subscribe_scan_requests()
        self.pubsub_manager.subscribe_scan_responses() # Mới
        self.pubsub_manager.subscribe_fix_requests()
        self.pubsub_manager.subscribe_fix_responses() # Mới
        
        self.is_running = True
        self.stop_event.clear()
        self.stats["started_at"] = datetime.now().isoformat()
        
        self.consumer_thread = threading.Thread(
            target=self._consume_messages,
            daemon=True
        )
        self.consumer_thread.start()
        
        logger.info("✅ Consumer started and listening for messages!")
    
    def stop(self, timeout=5):
        if not self.is_running:
            logger.info("🛑 Consumer not running.")
            return
        
        logger.info("🛑 Stopping consumer...")
        self.stop_event.set()
        self.is_running = False
        
        if self.consumer_thread:
            self.consumer_thread.join(timeout=timeout)
        
        # Đóng thread pool một cách an toàn
        self.thread_pool.shutdown(wait=True) 
        
        self.pubsub_manager.close()
        logger.info("✅ Consumer stopped.")
    
    def _consume_messages(self):
        logger.info("👂 Listening for messages from Redis Pub/Sub...")
        
        try:
            # pubsub_manager.listen_for_messages giờ yield dict {"channel": ..., "message": ...}
            for message_envelope in self.pubsub_manager.listen_for_messages():
                if self.stop_event.is_set():
                    logger.info("Stopping message consumption as stop event is set.")
                    break
                
                channel = message_envelope["channel"]
                message_payload = message_envelope["message"]
                
                self._handle_message_from_broker(channel, message_payload)
                
                self.stats["total_messages_received"] += 1
                self.stats["last_message_at"] = datetime.now().isoformat()
                
        except Exception as e:
            logger.critical(f"❌ CRITICAL Error in message consumption loop: {e}", exc_info=True)
            self.stats["errors"] += 1
    
    def _handle_message_from_broker(self, channel: str, message_payload: Dict[str, Any]):
        """
        Callback để xử lý message nhận được từ Redis Pub/Sub.
        Phân tích message và gửi tới ScanService hoặc log response.
        """
        try:
            message_type = message_payload.get("type")
            data = message_payload.get("data", {}) # Data nằm trong key 'data'
            
            if channel == self.pubsub_manager.settings.REDIS_CHANNEL_SCAN_REQUEST:
                self.stats["scan_requests_processed"] += 1
                # Chuyển đổi payload thành ScanInstanceMessage Pydantic model
                scan_message = ScanInstanceMessage(**data) # data là nội dung của ScanInstanceMessage
                self._log_scan_request(scan_message)
                
                # Gửi tác vụ quét tới ScanService
                self.scan_service.submit_scan_task(scan_message)
                # Note: stats["pending_scan_tasks"] sẽ được cập nhật bởi thread_pool nội bộ
                
            elif channel == self.pubsub_manager.settings.REDIS_CHANNEL_SCAN_RESPONSE:
                self.stats["scan_responses_received"] += 1
                scan_response = ScanResponseMessage(**data) # data là nội dung của ScanResponseMessage
                self._log_scan_response(scan_response)
                
            elif channel == self.pubsub_manager.settings.REDIS_CHANNEL_FIX_REQUEST:
                self.stats["fix_requests_processed"] += 1
                self._log_fix_request(data) # Giả định data là dict cho fix request
                
            elif channel == self.pubsub_manager.settings.REDIS_CHANNEL_FIX_RESPONSE:
                self.stats["fix_responses_received"] += 1
                self._log_fix_response(data) # Giả định data là dict cho fix response
                
            else:
                logger.warning(f"⚠️ Consumer received message from unknown channel: {channel}. Message type: {message_type}")
                
        except Exception as e:
            logger.error(f"❌ Error handling message from channel {channel}: {e}", exc_info=True)
            self.stats["errors"] += 1

    def _log_scan_request(self, scan_message: ScanInstanceMessage):
        logger.info("\n" + "="*80)
        logger.info("📨 RECEIVED SCAN REQUEST FROM BROKER")
        logger.info("="*80)
        logger.info(f"Scan Request ID: {scan_message.scan_request_id}")
        logger.info(f"Instance ID:  {scan_message.instance_id}")
        logger.info(f"Instance Name:{scan_message.instance_name}")
        logger.info(f"User ID:      {scan_message.user_id}")
        logger.info(f"Workload ID:  {scan_message.workload_id}")
        logger.info(f"Rules Count:  {len(scan_message.rules)}")
        logger.info(f"Timestamp:    {scan_message.timestamp.isoformat()}")
        logger.info("="*80 + "\n")
    
    def _log_scan_response(self, scan_response: ScanResponseMessage):
        logger.info("\n" + "#"*80)
        logger.info("✅ RECEIVED SCAN RESPONSE FROM BROKER")
        logger.info("#"*80)
        logger.info(f"Scan Request ID: {scan_response.scan_request_id}")
        logger.info(f"Instance ID:  {scan_response.instance_id}")
        logger.info(f"Instance Name:{scan_response.instance_name}")
        logger.info(f"Overall Status: {scan_response.status.upper()}")
        logger.info(f"Total Rules: {scan_response.total_rules}, Passed: {scan_response.rules_passed}, Failed: {scan_response.rules_failed}")
        if scan_response.detail_error:
            logger.error(f"Error Detail: {scan_response.detail_error}")
        logger.info("-" * 80)
        for rr in scan_response.rule_results:
            logger.info(f"  Rule {rr.rule_id} ({rr.status.upper()}): {rr.message}")
            if rr.details_error:
                logger.debug(f"    Error: {rr.details_error}")
            if rr.output:
                logger.debug(f"    Output: {rr.output}")
        logger.info("#"*80 + "\n")

    def _log_fix_request(self, data: Dict):
        import json
        logger.info("\n" + "="*80)
        logger.info("🔧 RECEIVED FIX REQUEST FROM BROKER")
        logger.info("="*80)
        logger.info(f"Job ID:       {data.get('job_id')}")
        logger.info(f"User:         {data.get('username')} (ID: {data.get('user_id')})")
        logger.info(f"Instances:    {data.get('instance_ids')}")
        logger.info(f"Rules:        {data.get('rule_ids')}")
        logger.info(f"Time:         {data.get('timestamp')}")
        logger.info("="*80 + "\n")

    def _log_fix_response(self, data: Dict):
        import json
        logger.info("\n" + "#"*80)
        logger.info("🔧 RECEIVED FIX RESPONSE FROM BROKER")
        logger.info("#"*80)
        logger.info(f"Job ID:       {data.get('job_id')}")
        logger.info(f"Status:       {data.get('status')}")
        logger.info(f"Time:         {data.get('timestamp')}")
        logger.info("#"*80 + "\n")
    
    def get_stats(self):
        # Cập nhật số lượng tác vụ đang chạy/chờ
        # Note: self.thread_pool._work_queue.qsize() và _pending_work_items 
        # là các thuộc tính nội bộ và có thể không phản ánh chính xác 100%
        # số tác vụ đang chạy/chờ trong mọi thời điểm, nhưng cung cấp một ước tính.
        self.stats["active_scan_tasks"] = self.thread_pool._work_queue.qsize() # Tác vụ đã được submit và đang chờ
        # Số luồng thực sự đang bận có thể lấy từ self.thread_pool._threads nếu muốn phức tạp hơn,
        # nhưng qsize() thường là đủ để biểu thị tải.
        
        return {**self.stats, "is_running": self.is_running}