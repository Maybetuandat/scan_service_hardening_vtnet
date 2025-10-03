import logging
import os
import tempfile
import yaml 
import ansible_runner
import time 
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor
import threading



# Import RedisPubSubManager
from config.redis_pubsub import get_pubsub_manager, RedisPubSubManager
from schemas.scan_message import RuleInfo, RuleResultInfo, ScanInstanceMessage, ScanResponseMessage


logger = logging.getLogger(__name__)

class ScanService: 
    # ScanService không còn nhận db_engine hay session_maker
    def __init__(self, thread_pool: ThreadPoolExecutor, pubsub_manager: RedisPubSubManager):
        self.thread_pool = thread_pool # Nhận thread pool từ bên ngoài
        self.pubsub_manager = pubsub_manager # Nhận pubsub manager để publish
        self.ansible_timeout = 30
        
        logger.info("✅ ScanService initialized (DB-less mode)")

    def submit_scan_task(self, scan_message: ScanInstanceMessage):
        """
        Gửi một tác vụ quét server đơn lẻ tới thread pool.
        """
        self.thread_pool.submit(self._scan_single_server_from_message_threaded, scan_message)
        logger.info(f"🚀 Submitted scan task for instance {scan_message.instance_name} (ID: {scan_message.instance_id})")

    def _scan_single_server_from_message_threaded(self, scan_message: ScanInstanceMessage):
        """
        Thực hiện quét một server dựa trên thông tin từ ScanInstanceMessage.
        Tất cả thông tin cần thiết đã có trong message, không query DB.
        Kết quả được publish trở lại Redis.
        """
        thread_id = threading.current_thread().ident
        start_time = time.time()
        logger.info(f"⚙️ THREAD {thread_id} STARTED scanning for {scan_message.instance_name} at {start_time}")
        
        # Biến để lưu trữ RuleResultInfo
        all_rule_results_info: List[RuleResultInfo] = []
        overall_status = "completed"
        overall_error_detail: Optional[str] = None
        
        try:
            logger.debug(f"Thread {thread_id}: Scanning server {scan_message.instance_name} with message: {scan_message.dict()}")
            
            if not scan_message.rules:
                logger.warning(f"Thread {thread_id}: Instance {scan_message.instance_name} không có rule nào để quét.")
                overall_status = "failed"
                overall_error_detail = "Không có rule nào để quét."
                # Dù không có rules, vẫn tạo một response để báo cáo
                return self._publish_scan_response(
                    scan_message=scan_message,
                    overall_status=overall_status,
                    overall_error_detail=overall_error_detail,
                    all_rule_results_info=all_rule_results_info
                )

            rule_results_from_ansible, error_message = self._execute_rules_with_ansible_runner_threaded(
                scan_message, scan_message.rules, thread_id
            )
            
            all_rule_results_info.extend(rule_results_from_ansible)

            if error_message:
                overall_status = "failed"
                overall_error_detail = error_message
                logger.error(f"❌ Thread {thread_id}: Instance {scan_message.instance_name} scan failed with: {error_message}")
            else:
                logger.info(f"✅ Thread {thread_id}: Instance {scan_message.instance_name} scan completed successfully.")

        except Exception as e:
            overall_status = "failed"
            overall_error_detail = f"Unhandled error during scan: {str(e)}"
            logger.error(f"❌ Thread {thread_id}: Unhandled error scanning instance {scan_message.instance_id}: {str(e)}", exc_info=True)
        finally:
            # Luôn publish kết quả, dù thành công hay thất bại
            self._publish_scan_response(
                scan_message=scan_message,
                overall_status=overall_status,
                overall_error_detail=overall_error_detail,
                all_rule_results_info=all_rule_results_info
            )
            logger.info(f"⏳ Thread {thread_id}: Finished task for {scan_message.instance_name} in {time.time() - start_time:.2f}s. Response published.")

    def _publish_scan_response(
        self,
        scan_message: ScanInstanceMessage,
        overall_status: str,
        overall_error_detail: Optional[str],
        all_rule_results_info: List[RuleResultInfo]
    ):
        """Tạo và publish ScanResponseMessage lên Redis."""
        rules_passed_count = sum(1 for rr in all_rule_results_info if rr.status == "passed")
        rules_failed_count = sum(1 for rr in all_rule_results_info if rr.status == "failed")

        response_message = ScanResponseMessage(
            scan_request_id=scan_message.scan_request_id,
            instance_id=scan_message.instance_id,
            instance_name=scan_message.instance_name,
            workload_id=scan_message.workload_id,
            user_id=scan_message.user_id,
            status=overall_status,
            detail_error=overall_error_detail,
            total_rules=len(scan_message.rules), # Tổng số rules được yêu cầu quét
            rules_passed=rules_passed_count,
            rules_failed=rules_failed_count,
            rule_results=all_rule_results_info
        )
        
        # Publish Pydantic model đã chuyển thành dictionary
        self.pubsub_manager.publish_scan_response(response_message.dict())
        logger.info(f"Published scan response for instance {scan_message.instance_name}. Status: {overall_status}")


    def _execute_rules_with_ansible_runner_threaded(
        self, scan_message: ScanInstanceMessage, rules: List[RuleInfo], thread_id: int
    ) -> (List[RuleResultInfo], Optional[str]): 
       
        all_rule_results_info: List[RuleResultInfo] = []
        rules_to_run = {}
        playbook_tasks = []

        logger.info(f"Thread {thread_id}: Preparing {len(rules)} rules for instance {scan_message.instance_name}")

        # Chuẩn bị playbook tasks
        for rule in rules:
            start_time = time.time()
            task_name = f"Execute rule ID {rule.id}: {rule.name}"
            rules_to_run[task_name] = {'rule': rule, 'start_time': start_time}
            
            playbook_tasks.append({
                'name': task_name,
                'shell': rule.command,
                'ignore_errors': True # Cho phép playbook tiếp tục chạy dù một task bị lỗi
            })

        if not playbook_tasks:
            return all_rule_results_info, None

        # thực hiện gen ra file playbook để thực hiện với ansible 
        with tempfile.TemporaryDirectory() as private_data_dir:
            inventory = { 'all': { 'hosts': { scan_message.instance_name: {
                'ansible_user': scan_message.credentials.username, 
                'ansible_password': scan_message.credentials.password,
                'ansible_port': scan_message.ssh_port,
                'ansible_ssh_common_args': '-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'
            }}}}
            inventory_path = os.path.join(private_data_dir, 'inventory.yml')
            with open(inventory_path, 'w') as f: 
                yaml.dump(inventory, f)

            playbook = [{'hosts': 'all', 'gather_facts': False, 'tasks': playbook_tasks}]
            playbook_path = os.path.join(private_data_dir, 'scan_playbook.yml')
            with open(playbook_path, 'w') as f: 
                yaml.dump(playbook, f)

            logger.info(f"Thread {thread_id}: Running ansible-runner for {len(playbook_tasks)} rules on {scan_message.instance_name}")
            
            runner = ansible_runner.run(
                private_data_dir=private_data_dir, 
                playbook=playbook_path, 
                inventory=inventory_path,
                quiet=True, 
                cmdline=f'--timeout {self.ansible_timeout}' 
            )
            
            all_events = list(runner.events)
            
            if runner.status in ('failed', 'unreachable') or (runner.rc != 0 and not all_events and not runner.json_events):
                error_output = runner.stdout.read() if runner.stdout else ""
                full_error = f"Thread {thread_id}: Ansible run failed for {scan_message.instance_name}. Status: {runner.status}, RC: {runner.rc}. Output: {error_output}"
                logger.error(full_error)
                return [], f"Ansible connection failed or playbook execution error: {error_output[:500] or 'Check logs for details'}"
        
            # Xử lý kết quả của từng task
            for event in all_events:
                if event['event'] in ('runner_on_ok', 'runner_on_failed'):
                    task_name_from_event = event['event_data'].get('task')
                    if not task_name_from_event:
                        continue 

                    rule_info = rules_to_run.get(task_name_from_event)
                    if not rule_info:
                        logger.warning(f"Thread {thread_id}: Could not map event task '{task_name_from_event}' back to a rule.")
                        continue
                        
                    task_result = event['event_data']['res']
                    rule_obj = rule_info['rule'] # Đây là RuleInfo model
                    
                    output = task_result.get('stdout', '')
                    error = task_result.get('stderr', '')

                    is_passed, parsed_output_dict = self._evaluate_rule_result(rule_obj, output)
                    
                    status = "passed"
                    message = "Rule execution successful and parameters matched"
                    details_error = None
                    
                    if task_result.get('rc', 1) != 0 : # RC != 0 => lệnh thất bại
                        status = "failed"
                        message = "Rule command execution failed"
                        if error:
                            details_error = error[:500]
                        elif task_result.get('stderr_lines'):
                            details_error = "\n".join(task_result.get('stderr_lines', []))[:500]
                        
                    if not is_passed and status == "passed": # Nếu lệnh thành công nhưng tham số không khớp
                        status = "failed"
                        message = "Rule parameters mismatch"
                        # Có thể thêm output vào details_error nếu cần
                        details_error = f"Expected parameters not found in output: {parsed_output_dict}"[:500]
                    elif not is_passed and status == "failed": # Nếu lệnh đã thất bại rồi và tham số cũng không khớp
                         message += " and parameters mismatch"

                    all_rule_results_info.append(RuleResultInfo(
                        rule_id=rule_obj.id,
                        status=status,
                        message=message,
                        details_error=details_error,
                        output=parsed_output_dict
                    ))

        return all_rule_results_info, None
        
    def _evaluate_rule_result(self, rule: RuleInfo, command_output: str) -> tuple[bool, Dict[str, Any]]:
       
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
      
        parsed_data = {}
        clean_output = output.strip()
        if not clean_output:
            return parsed_data

        try:
            lines = [line.strip() for line in clean_output.splitlines() if line.strip()]
            
            # 1. Thử phân tích dưới dạng Key-Value (vd: key=value)
            if lines and '=' in clean_output:
                delimiter = '='
                temp_dict = {}
                for line in lines:
                    parts = line.split(delimiter, 1)
                    if len(parts) == 2:
                        temp_dict[parts[0].strip()] = parts[1].strip()
                if temp_dict and len(temp_dict) == len(lines): # Đảm bảo tất cả các dòng đều là key=value
                    parsed_data.update(temp_dict)
                    return parsed_data
            
            # 2. Thử phân tích dưới dạng các giá trị phân tách bằng dấu cách
            values = clean_output.split()
            if len(values) > 1:
                # Nếu có nhiều giá trị, gán chúng vào các key value_0, value_1,...
                for i, val in enumerate(values):
                    parsed_data[f"value_{i}"] = val
                return parsed_data
            
            # 3. Coi là một giá trị duy nhất
            parsed_data["single_value"] = clean_output
            return parsed_data
        except Exception as e:
            logger.warning(f"Could not parse command output. Error: {e}. Output: '{clean_output[:100]}'")
            parsed_data["parse_error"] = str(e)
            return parsed_data

    def _compare_with_parameters(self, parameters: Dict[str, Any], parsed_output: Dict[str, Any]) -> bool:
        logger.debug(f"Rule Parameters: {parameters}")
        logger.debug(f"Parsed Output for Comparison: {parsed_output}")

        params_to_check = {k: str(v).strip() for k, v in parameters.items() if k not in ["docs", "note", "description"]}
        if not params_to_check:
            return True

        expected_values = sorted(params_to_check.values())
        actual_values = sorted([str(v).strip() for v in parsed_output.values()]) # Chỉ lấy values để so sánh

        logger.debug(f"Expected Values (sorted): {expected_values}")
        logger.debug(f"Actual Values (sorted): {actual_values}")

        # So sánh các giá trị đã sắp xếp. Nếu số lượng bằng nhau và các giá trị khớp.
        if expected_values == actual_values:
            logger.debug("All values matched. PASSED.")
            return True
        
        logger.debug(f"Value mismatch: Expected {expected_values}, Got {actual_values}")
        return False