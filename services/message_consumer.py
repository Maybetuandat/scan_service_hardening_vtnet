import logging
import threading
from typing import Dict, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from config.redis_pubsub import get_pubsub_manager, RedisPubSubManager

from schemas.scan_message import ScanInstanceMessage, ScanResponseMessage
from schemas.fix_message import FixMessage, FixResponseMessage
from services.scan_service import ScanService
from services.fix_service import FixService

logger = logging.getLogger(__name__)

class MessageConsumerService:
    _instance: Optional['MessageConsumerService'] = None
    _lock = threading.Lock()
    
    def __init__(self):
        if MessageConsumerService._instance is not None:
            raise RuntimeError("Use get_instance() instead")
        
        self.pubsub_manager = get_pubsub_manager()
        self.is_running = False
        self.consumer_thread = None
        self.stop_event = threading.Event()
        
        # Tạo ThreadPoolExecutor dùng chung cho toàn bộ ứng dụng
        self.max_workers = 10
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        self._warm_up_threads()
        
        # Khởi tạo ScanService và FixService với thread_pool chung
        self.scan_service = ScanService(self.thread_pool, self.pubsub_manager)
        self.fix_service = FixService(self.thread_pool, self.pubsub_manager)
        
        self.stats = {
            "total_messages_received": 0,
            "scan_requests_processed": 0,
            "scan_responses_received": 0,
            "fix_requests_processed": 0,
            "fix_responses_received": 0,
            "errors": 0,
            "started_at": None,
            "last_message_at": None,
            "active_scan_tasks": 0,
            "pending_scan_tasks": 0
        }
        
        logger.info("✅ Consumer initialized (with ScanService and FixService, DB-less for worker)")
    
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
        self.pubsub_manager.subscribe_scan_responses()
        self.pubsub_manager.subscribe_fix_requests()
        self.pubsub_manager.subscribe_fix_responses()
        
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
        
        self.thread_pool.shutdown(wait=True)
        
        self.pubsub_manager.close()
        logger.info("✅ Consumer stopped.")
    
    def _consume_messages(self):
        logger.info("👂 Listening for messages from Redis Pub/Sub...")
        
        try:
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
        """
        try:
            message_type = message_payload.get("type")
            data = message_payload.get("data", {})
            
            if channel == self.pubsub_manager.settings.REDIS_CHANNEL_SCAN_REQUEST:
                self.stats["scan_requests_processed"] += 1
                scan_message = ScanInstanceMessage(**data)
                self._log_scan_request(scan_message)
                self.scan_service.submit_scan_task(scan_message)
                
            elif channel == self.pubsub_manager.settings.REDIS_CHANNEL_SCAN_RESPONSE:
                self.stats["scan_responses_received"] += 1
                scan_response = ScanResponseMessage(**data)
                self._log_scan_response(scan_response)
                
            elif channel == self.pubsub_manager.settings.REDIS_CHANNEL_FIX_REQUEST:
                self.stats["fix_requests_processed"] += 1
                fix_message = FixMessage(**data)
                self._log_fix_request(fix_message)
                self.fix_service.submit_fix_task(fix_message)
                
            elif channel == self.pubsub_manager.settings.REDIS_CHANNEL_FIX_RESPONSE:
                self.stats["fix_responses_received"] += 1
                fix_response = FixResponseMessage(**data)
                self._log_fix_response(fix_response)
                
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

    def _log_fix_request(self, fix_message: FixMessage):
        logger.info("\n" + "="*80)
        logger.info("🔧 RECEIVED FIX REQUEST FROM BROKER")
        logger.info("="*80)
        logger.info(f"Fix Request ID: {fix_message.fix_request_id}")
        logger.info(f"Fix ID:       {fix_message.fix_id}")
        logger.info(f"IP Address:   {fix_message.ip_address}")
        logger.info(f"Port:         {fix_message.port}")
        logger.info(f"User:         {fix_message.ssh_username}")
        logger.info(f"Fix Type:    {fix_message.fix_type}")
        logger.info(f"Fix Commands: {len(fix_message.suggest_fix)}")
        for idx, cmd in enumerate(fix_message.suggest_fix, 1):
            logger.info(f"  {idx}. {cmd[:100]}{'...' if len(cmd) > 100 else ''}")
        logger.info(f"Timestamp:    {fix_message.timestamp.isoformat()}")
        logger.info("="*80 + "\n")

    def _log_fix_response(self, fix_response: FixResponseMessage):
        logger.info("\n" + "#"*80)
        logger.info("✅ RECEIVED FIX RESPONSE FROM BROKER")
        logger.info("#"*80)
        logger.info(f"Fix Request ID: {fix_response.fix_request_id}")
        logger.info(f"Fix ID:       {fix_response.fix_id}")
        logger.info(f"IP Address:   {fix_response.ip_address}")
        logger.info(f"Overall Status: {fix_response.status.upper()}")
        logger.info(f"Total Fixes: {fix_response.total_fixes}, Success: {fix_response.fixes_success}, Failed: {fix_response.fixes_failed}")
        if fix_response.detail_error:
            logger.error(f"Error Detail: {fix_response.detail_error}")
        logger.info("-" * 80)
        for fr in fix_response.fix_results:
            logger.info(f"  Fix Command: {fr.fix_command[:60]}... ({fr.status.upper()})")
            logger.info(f"    Message: {fr.message}")
            if fr.execution_time:
                logger.info(f"    Execution Time: {fr.execution_time}s")
            if fr.details_error:
                logger.debug(f"    Error: {fr.details_error}")
            if fr.output:
                logger.debug(f"    Output: {fr.output[:200]}{'...' if len(fr.output) > 200 else ''}")
        logger.info("#"*80 + "\n")
    
    def get_stats(self):
        self.stats["active_scan_tasks"] = self.thread_pool._work_queue.qsize()
        return {**self.stats, "is_running": self.is_running}