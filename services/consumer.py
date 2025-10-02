# scan_service/services/scan_consumer_service.py

import logging
import json
from typing import Dict, Any

from schemas.scan_message import ScanInstanceMessage
from utils.redis_manager import get_pubsub_manager

logger = logging.getLogger(__name__)


class ScanConsumerService:
    """Service để consume scan requests từ Redis queue"""
    
    def __init__(self):
        self.pubsub_manager = get_pubsub_manager()
    
    def start_listening(self):
        """Bắt đầu lắng nghe messages từ Redis queue"""
        logger.info("🎧 Starting scan consumer service...")
        logger.info("📡 Waiting for scan requests...")
        
        def message_callback(channel: str, data: Dict[str, Any]):
            """Callback khi nhận được message"""
            try:
                message_data = data.get("data", {})
                scan_message = ScanInstanceMessage(**message_data)
                
                # In ra màn hình
                self._print_scan_message(scan_message)
                
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")
                print(f"\n❌ ERROR: {e}\n")
        
        # Subscribe và listen
        self.pubsub_manager.subscribe_to_scan_requests(callback=message_callback)
        
        for message in self.pubsub_manager.listen_to_messages(callback=message_callback):
            pass
    
    def _print_scan_message(self, msg: ScanInstanceMessage):
        """In message ra màn hình"""
        print("\n" + "="*100)
        print(f"📨 RECEIVED SCAN REQUEST - {msg.scan_request_id}")
        print("="*100)
        
        print(f"\n🖥️  INSTANCE:")
        print(f"   ID: {msg.instance_id}")
        print(f"   Name (IP): {msg.instance_name}")
        print(f"   SSH Port: {msg.ssh_port}")
        print(f"   Role: {msg.instance_role or 'N/A'}")
        
        print(f"\n📦 WORKLOAD:")
        print(f"   ID: {msg.workload_id}")
        print(f"   Name: {msg.workload_name}")
        print(f"   Description: {msg.workload_description or 'N/A'}")
        
        print(f"\n💿 OS:")
        print(f"   Name: {msg.os_name}")
        print(f"   Type: {msg.os_type}")
        print(f"   Display: {msg.os_display}")
        
        print(f"\n👤 USER:")
        print(f"   User ID: {msg.user_id}")
        
        # IN RA CREDENTIALS
        print(f"\n🔑 SSH CREDENTIALS:")
        print(f"   Username: {msg.credentials.username}")
        print(f"   Password: {'*' * 8 if msg.credentials.password else 'N/A'}")  # Mask password
        print(f"   Private Key: {'[PROVIDED]' if msg.credentials.private_key else 'N/A'}")
        
        print(f"\n📋 RULES ({len(msg.rules)}):")
        for idx, rule in enumerate(msg.rules, 1):
            print(f"\n   Rule #{idx}: {rule.name}")
            print(f"   Command: {rule.command}")
            if rule.parameters:
                print(f"   Parameters: {json.dumps(rule.parameters, indent=6)}")
            if rule.suggested_fix:
                print(f"   Fix: {rule.suggested_fix}")
        
        print(f"\n⏰ Timestamp: {msg.timestamp}")
        print("="*100 + "\n")