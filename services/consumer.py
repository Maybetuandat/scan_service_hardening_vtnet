# scan_service/services/scan_consumer_service.py

import logging
import json
import time
import threading
import os
import tempfile
import yaml
import ansible_runner
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine

from dao.instance_dao import InstanceDAO
from models.rule import Rule
from models.rule_result import RuleResult
from models.compliance_result import ComplianceResult

from schemas.scan_message import ScanInstanceMessage
from utils.redis_manager import get_pubsub_manager
from services.compilance_result_service import ComplianceResultService
from services.rule_service import RuleService

logger = logging.getLogger(__name__)


class ScanConsumerService:
    """Service để consume scan requests từ Redis queue và xử lý scan với thread pool"""
    
    def __init__(self, db_engine):
        self.pubsub_manager = get_pubsub_manager()
        self.db_engine = db_engine
        self.session_maker = sessionmaker(bind=self.db_engine)
        
        # Thread pool configuration
        self.max_workers = 10
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # Queue để quản lý messages
        self.message_queue = Queue()
        
        # Ansible configuration
        self.ansible_timeout = 30
        
        # Warm up thread pool
        self._warm_up_threads()
    
    def _warm_up_threads(self):
        """Khởi động sẵn các threads"""
        def dummy_task():
            time.sleep(0.01)
            return "warmed"
        
        futures = [self.thread_pool.submit(dummy_task) for _ in range(self.max_workers)]
        for future in futures:
            future.result()
        
        logger.info(f"✅ Thread pool warmed up with {self.max_workers} threads")
    
    def start_listening(self):
        """Bắt đầu lắng nghe messages từ Redis queue"""
        logger.info("🎧 Starting scan consumer service...")
        logger.info(f"📡 Max workers: {self.max_workers}")
        logger.info("📡 Waiting for scan requests...\n")
        
        def message_callback(channel: str, data: Dict[str, Any]):
            """Callback khi nhận được message từ Redis"""
            try:
                message_data = data.get("data", {})
                scan_message = ScanInstanceMessage(**message_data)
                
                logger.info(f"📨 Received scan request for instance: {scan_message.instance_name}")
                
                # Submit job vào thread pool
                future = self.thread_pool.submit(
                    self._process_scan_message, 
                    scan_message
                )
                
                # Log khi submit thành công
                logger.info(f"✅ Submitted scan task for {scan_message.instance_name} to thread pool")
                
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")
        
        # Subscribe và listen
        try:
            self.pubsub_manager.subscribe_to_scan_requests(callback=message_callback)
            
            # Blocking call - listen forever
            for message in self.pubsub_manager.listen_to_messages(callback=message_callback):
                pass
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Shutting down consumer service...")
            self.thread_pool.shutdown(wait=True)
            logger.info("✅ All threads completed. Service stopped.")
    
    def _process_scan_message(self, scan_message: ScanInstanceMessage):
        """
        Xử lý scan message trong thread riêng biệt
        """
        thread_id = threading.current_thread().ident
        start_time = time.time()
        
        logger.info(f"🔹 Thread {thread_id} STARTED for instance {scan_message.instance_name}")
        
        # Tạo session riêng cho thread này
        thread_session = self.session_maker()
        compliance_result_id = None
        
        try:
            # Print scan message details
            self._print_scan_message(scan_message, thread_id)
            
            # Khởi tạo services với session riêng
            compliance_result_service = ComplianceResultService(thread_session)
            rule_service = RuleService(thread_session)
            
            # 1. Tạo compliance result với status pending
            logger.info(f"Thread {thread_id}: Creating compliance result...")
            compliance_result = compliance_result_service.create_pending_result(
                scan_message.instance_id,
                scan_message.workload_id
            )
            compliance_result_id = compliance_result.id
            thread_session.commit()
            
            # 2. Update status sang running
            logger.info(f"Thread {thread_id}: Updating status to running...")
            compliance_result_service.update_status(compliance_result_id, "running")
            thread_session.commit()
            
            # 3. Lấy rules từ message (đã có sẵn rules trong message)
            rules = scan_message.rules
            
            if not rules:
                logger.warning(f"Thread {thread_id}: No rules found in message")
                compliance_result_service.update_status(
                    compliance_result_id, 
                    "failed", 
                    detail_error="No active rules"
                )
                thread_session.commit()
                return
            
            logger.info(f"Thread {thread_id}: Found {len(rules)} rules to execute")
            
            # 4. Execute rules với Ansible
            rule_results, error_message = self._execute_rules_with_ansible(
                scan_message,
                rules,
                compliance_result_id,
                thread_id
            )
            
            # 5. Xử lý kết quả
            if error_message:
                logger.error(f"Thread {thread_id}: Ansible execution failed - {error_message}")
                compliance_result_service.update_status(
                    compliance_result_id,
                    "failed",
                    detail_error=error_message
                )
                thread_session.commit()
                
                # Publish failure message về Redis
                self._publish_scan_result(scan_message, "failed", error_message)
                return
            
            # 6. Complete result với rule_results
            logger.info(f"Thread {thread_id}: Completing scan result...")
            compliance_result_service.complete_result(
                compliance_result_id,
                rule_results,
                len(rules)
            )
            thread_session.commit()
            
            # 7. Publish success message về Redis
            self._publish_scan_result(scan_message, "completed", None)
            
            # Calculate execution time
            execution_time = time.time() - start_time
            logger.info(f"✅ Thread {thread_id}: Instance {scan_message.instance_name} scan completed in {execution_time:.2f}s")
            
        except Exception as e:
            thread_session.rollback()
            logger.error(f"❌ Thread {thread_id}: Error scanning instance {scan_message.instance_id}: {str(e)}")
            
            # Update error status
            if compliance_result_id:
                try:
                    temp_session = self.session_maker()
                    temp_service = ComplianceResultService(temp_session)
                    temp_service.update_status(
                        compliance_result_id,
                        "failed",
                        detail_error=str(e)
                    )
                    temp_session.commit()
                    temp_session.close()
                except Exception as update_e:
                    logger.error(f"Failed to update error status: {update_e}")
            
            # Publish failure message
            self._publish_scan_result(scan_message, "failed", str(e))
            
        finally:
            thread_session.close()
    
    def _execute_rules_with_ansible(
        self,
        scan_message: ScanInstanceMessage,
        rules: List[Any],  # RuleInfo objects
        compliance_result_id: int,
        thread_id: int
    ) -> tuple[List[RuleResult], Optional[str]]:
        """
        Thực thi rules sử dụng Ansible Runner
        """
        all_rule_results = []
        rules_to_run = {}
        playbook_tasks = []
        
        logger.info(f"Thread {thread_id}: Preparing {len(rules)} rules for execution")
        
        # Chuẩn bị playbook tasks
        for rule in rules:
            start_time = time.time()
            task_name = f"Execute rule ID {rule.id}: {rule.name}"
            rules_to_run[task_name] = {
                'rule': rule,
                'start_time': start_time
            }
            
            playbook_tasks.append({
                'name': task_name,
                'shell': rule.command,
                'ignore_errors': True
            })
        
        if not playbook_tasks:
            return all_rule_results, None
        
        # Tạo temporary directory cho Ansible
        with tempfile.TemporaryDirectory() as private_data_dir:
            # Tạo inventory
            inventory = {
                'all': {
                    'hosts': {
                        scan_message.instance_name: {
                            'ansible_user': scan_message.credentials.username,
                            'ansible_password': scan_message.credentials.password,
                            'ansible_port': scan_message.ssh_port,
                            'ansible_ssh_common_args': '-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'
                        }
                    }
                }
            }
            
            # Nếu có private key thì dùng private key thay vì password
            if scan_message.credentials.private_key:
                # Write private key to temp file
                key_path = os.path.join(private_data_dir, 'ssh_key')
                with open(key_path, 'w') as f:
                    f.write(scan_message.credentials.private_key)
                os.chmod(key_path, 0o600)
                
                inventory['all']['hosts'][scan_message.instance_name]['ansible_ssh_private_key_file'] = key_path
                del inventory['all']['hosts'][scan_message.instance_name]['ansible_password']
            
            inventory_path = os.path.join(private_data_dir, 'inventory.yml')
            with open(inventory_path, 'w') as f:
                yaml.dump(inventory, f)
            
            # Tạo playbook
            playbook = [{
                'hosts': 'all',
                'gather_facts': False,
                'tasks': playbook_tasks
            }]
            
            playbook_path = os.path.join(private_data_dir, 'scan_playbook.yml')
            with open(playbook_path, 'w') as f:
                yaml.dump(playbook, f)
            
            # Run ansible-runner
            logger.info(f"Thread {thread_id}: Running Ansible for {len(playbook_tasks)} rules on {scan_message.instance_name}")
            
            runner = ansible_runner.run(
                private_data_dir=private_data_dir,
                playbook=playbook_path,
                inventory=inventory_path,
                quiet=True,
                cmdline=f'--timeout {self.ansible_timeout}'
            )
            
            all_events = list(runner.events)
            
            # Kiểm tra connection failure
            if runner.status in ('failed', 'unreachable') or (runner.rc != 0 and not all_events):
                error_output = runner.stdout.read() if hasattr(runner, 'stdout') else "Unknown error"
                full_error = f"Ansible connection failed. Status: {runner.status}, RC: {runner.rc}"
                logger.error(f"Thread {thread_id}: {full_error}")
                return [], full_error
            
            # Xử lý kết quả từ events
            for event in all_events:
                if event['event'] in ('runner_on_ok', 'runner_on_failed'):
                    task_name_from_event = event['event_data'].get('task')
                    
                    if not task_name_from_event:
                        continue
                    
                    rule_info = rules_to_run.get(task_name_from_event)
                    if not rule_info:
                        logger.warning(f"Thread {thread_id}: Could not map task '{task_name_from_event}' to rule")
                        continue
                    
                    task_result = event['event_data']['res']
                    rule_obj = rule_info['rule']
                    execution_time = int(time.time() - rule_info['start_time'])
                    
                    output = task_result.get('stdout', '')
                    error = task_result.get('stderr', '')
                    
                    # Evaluate rule result
                    is_passed, parsed_output_dict = self._evaluate_rule_result(rule_obj, output)
                    
                    status = "passed"
                    message = "Rule execution successful"
                    
                    if task_result.get('rc', 1) != 0:
                        status = "failed"
                        message = "Rule execution failed"
                    
                    if not is_passed:
                        status = "failed"
                        message = "Parameter mismatch"
                    
                    details_error = None
                    if status == "failed" and error:
                        details_error = error[:500]
                    
                    all_rule_results.append(RuleResult(
                        compliance_result_id=compliance_result_id,
                        rule_id=rule_obj.id,
                        status=status,
                        message=message,
                        details_error=details_error,
                        output=parsed_output_dict
                    ))
        
        return all_rule_results, None
    
    def _evaluate_rule_result(self, rule: Any, command_output: str) -> tuple[bool, dict]:
        """Đánh giá kết quả rule"""
        if not rule.parameters or not isinstance(rule.parameters, dict):
            return True, {}
        
        try:
            parsed_output = self._parse_output_values(command_output)
            is_passed = self._compare_with_parameters(rule.parameters, parsed_output)
            return is_passed, parsed_output
        except Exception as e:
            logger.error(f"Error evaluating rule {rule.name}: {str(e)}")
            return False, {"error": str(e)}
    
    def _parse_output_values(self, output: str) -> Dict[str, Any]:
        """Parse command output"""
        parsed_data = {}
        clean_output = output.strip()
        
        if not clean_output:
            return parsed_data
        
        try:
            # 1. Key-Value pairs
            lines = [line.strip() for line in clean_output.splitlines() if line.strip()]
            if lines and '=' in clean_output:
                delimiter = '=' if all('=' in line for line in lines) else None
                if delimiter:
                    temp_dict = {
                        parts[0].strip(): parts[1].strip()
                        for line in lines
                        if len(parts := line.split(delimiter, 1)) == 2
                    }
                    if temp_dict:
                        parsed_data.update(temp_dict)
                        return parsed_data
            
            # 2. Space-separated values
            values = clean_output.split()
            if len(values) > 1:
                for i, val in enumerate(values):
                    parsed_data[f"value_{i}"] = val
                return parsed_data
            
            # 3. Single value
            parsed_data["single_value"] = clean_output
            return parsed_data
            
        except Exception as e:
            logger.warning(f"Could not parse output: {e}")
            parsed_data["parse_error"] = str(e)
            return parsed_data
    
    def _compare_with_parameters(self, parameters: Dict[str, Any], parsed_output: Dict[str, Any]) -> bool:
        """So sánh parameters với output"""
        # Remove excluded keys
        params_to_check = {
            k: v for k, v in parameters.items() 
            if k not in ["docs", "note", "description"]
        }
        
        if not params_to_check:
            return True
        
        expected_values = [str(v).strip() for v in params_to_check.values()]
        actual_values = [str(v).strip() for v in parsed_output.values()]
        
        # Compare as sorted lists
        if sorted(expected_values) != sorted(actual_values[:len(expected_values)]):
            logger.debug(f"Value mismatch: Expected {expected_values}, Got {actual_values}")
            return False
        
        return True
    
    def _publish_scan_result(self, scan_message: ScanInstanceMessage, status: str, error: Optional[str]):
        """Publish kết quả scan về Redis"""
        try:
            result_message = {
                "scan_request_id": scan_message.scan_request_id,
                "instance_id": scan_message.instance_id,
                "instance_name": scan_message.instance_name,
                "status": status,
                "error": error,
                "timestamp": time.time()
            }
            
            # Publish về một channel khác để backend có thể consume
            self.pubsub_manager.publish_scan_result(result_message)
            logger.info(f"📤 Published scan result for {scan_message.instance_name}: {status}")
            
        except Exception as e:
            logger.error(f"❌ Error publishing scan result: {e}")
    
    def _print_scan_message(self, msg: ScanInstanceMessage, thread_id: int):
        """In message ra màn hình"""
        print("\n" + "="*100)
        print(f"📨 THREAD {thread_id} - PROCESSING SCAN REQUEST")
        print("="*100)
        
        print(f"\n🖥️  INSTANCE:")
        print(f"   ID: {msg.instance_id}")
        print(f"   Name (IP): {msg.instance_name}")
        print(f"   SSH Port: {msg.ssh_port}")
        print(f"   Role: {msg.instance_role or 'N/A'}")
        
        print(f"\n📦 WORKLOAD:")
        print(f"   ID: {msg.workload_id}")
        print(f"   Name: {msg.workload_name}")
        
        print(f"\n💿 OS:")
        print(f"   Name: {msg.os_name}")
        print(f"   Type: {msg.os_type}")
        
        print(f"\n🔑 SSH CREDENTIALS:")
        print(f"   Username: {msg.credentials.username}")
        print(f"   Password: {'*' * 8 if msg.credentials.password else 'N/A'}")
        print(f"   Private Key: {'[PROVIDED]' if msg.credentials.private_key else 'N/A'}")
        
        print(f"\n📋 RULES ({len(msg.rules)}):")
        for idx, rule in enumerate(msg.rules, 1):
            print(f"   {idx}. {rule.name}")
        
        print("="*100 + "\n")