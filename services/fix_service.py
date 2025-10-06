import logging
import os
import tempfile
import yaml
import ansible_runner
import time
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor
import threading

from config.redis_pubsub import RedisPubSubManager
from schemas.fix_message import FixMessage, FixResultInfo, FixResponseMessage

logger = logging.getLogger(__name__)


class FixService:
    def __init__(self, thread_pool: ThreadPoolExecutor, pubsub_manager: RedisPubSubManager):
        self.thread_pool = thread_pool
        self.pubsub_manager = pubsub_manager
        self.ansible_timeout = 60  # Fix có thể mất nhiều thời gian hơn scan
        
        logger.info("✅ FixService initialized (DB-less mode)")

    def submit_fix_task(self, fix_message: FixMessage):
        """Gửi một tác vụ fix tới thread pool."""
        self.thread_pool.submit(self._fix_single_server_threaded, fix_message)
        logger.info(f"🚀 Submitted fix task for instance {fix_message.ip_address} (Scan ID: {fix_message.fix_id})")

    def _fix_single_server_threaded(self, fix_message: FixMessage):
        """
        Thực hiện fix một server dựa trên thông tin từ FixMessage.
        Kết quả được publish trở lại Redis.
        """
        thread_id = threading.current_thread().ident
        start_time = time.time()
        logger.info(f"⚙️ THREAD {thread_id} STARTED fixing for {fix_message.ip_address} at {start_time}")
        
        all_fix_results: List[FixResultInfo] = []
        overall_status = "completed"
        overall_error_detail: Optional[str] = None
        
        try:
            logger.debug(f"Thread {thread_id}: Fixing server {fix_message.ip_address} with message: {fix_message.dict()}")
            
            if not fix_message.suggest_fix:
                logger.warning(f"Thread {thread_id}: Instance {fix_message.ip_address} không có fix command nào.")
                overall_status = "failed"
                overall_error_detail = "Không có fix command nào để thực thi."
                return self._publish_fix_response(
                    fix_message=fix_message,
                    overall_status=overall_status,
                    overall_error_detail=overall_error_detail,
                    all_fix_results=all_fix_results
                )

            fix_results, error_message = self._execute_fixes_with_ansible_runner_threaded(
                fix_message, thread_id
            )
            
            all_fix_results.extend(fix_results)

            if error_message:
                overall_status = "failed"
                overall_error_detail = error_message
                logger.error(f"❌ Thread {thread_id}: Instance {fix_message.ip_address} fix failed with: {error_message}")
            else:
                logger.info(f"✅ Thread {thread_id}: Instance {fix_message.ip_address} fix completed successfully.")

        except Exception as e:
            overall_status = "failed"
            overall_error_detail = f"Unhandled error during fix: {str(e)}"
            logger.error(f"❌ Thread {thread_id}: Unhandled error fixing instance {fix_message.ip_address}: {str(e)}", exc_info=True)
        finally:
            self._publish_fix_response(
                fix_message=fix_message,
                overall_status=overall_status,
                overall_error_detail=overall_error_detail,
                all_fix_results=all_fix_results
            )
            logger.info(f"⏳ Thread {thread_id}: Finished fix task for {fix_message.ip_address} in {time.time() - start_time:.2f}s. Response published.")

    def _publish_fix_response(
        self,
        fix_message: FixMessage,
        overall_status: str,
        overall_error_detail: Optional[str],
        all_fix_results: List[FixResultInfo]
    ):
        """Tạo và publish FixResponseMessage lên Redis."""
        fixes_success_count = sum(1 for fr in all_fix_results if fr.status == "success")
        fixes_failed_count = sum(1 for fr in all_fix_results if fr.status == "failed")

        response_message = FixResponseMessage(
            fix_request_id=fix_message.fix_request_id,
            fix_id=fix_message.fix_id,
            ip_address=fix_message.ip_address,
            assessment_id=fix_message.assessment_id,
            user_id=fix_message.user_id,
            status=overall_status,
            detail_error=overall_error_detail,
            total_fixes=len(fix_message.suggest_fix),
            fixes_success=fixes_success_count,
            fixes_failed=fixes_failed_count,
            fix_results=all_fix_results
        )
        
        self.pubsub_manager.publish_fix_response(response_message.dict())
        logger.info(f"📤 Published fix response for instance {fix_message.ip_address}. Status: {overall_status}")

    def _execute_fixes_with_ansible_runner_threaded(
        self, fix_message: FixMessage, thread_id: int
    ) -> (List[FixResultInfo], Optional[str]):
        """
        Thực thi các fix commands sử dụng ansible-runner.
        Trả về list các FixResultInfo và error message nếu có.
        """
        all_fix_results: List[FixResultInfo] = []
        fixes_to_run = {}
        playbook_tasks = []

        logger.info(f"Thread {thread_id}: Preparing {len(fix_message.suggest_fix)} fix commands for instance {fix_message.ip_address}")

        # Chuẩn bị playbook tasks
        for idx, fix_command in enumerate(fix_message.suggest_fix):
            start_time = time.time()
            task_name = f"Execute fix command {idx + 1}: {fix_command[:50]}..."
            fixes_to_run[task_name] = {
                'command': fix_command,
                'start_time': start_time,
                'index': idx
            }
            
            playbook_tasks.append({
                'name': task_name,
                'shell': fix_command,
                'ignore_errors': True
            })

        if not playbook_tasks:
            return all_fix_results, None

        # Thực hiện gen ra file playbook để thực hiện với ansible
        with tempfile.TemporaryDirectory() as private_data_dir:
            inventory = {
                'all': {
                    'hosts': {
                        fix_message.ip_address: {
                            'ansible_user': fix_message.ssh_username,
                            'ansible_password': fix_message.ssh_password,
                            'ansible_port': fix_message.port,
                            'ansible_ssh_common_args': '-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null',
                            'ansible_become': True,  # Thường fix cần quyền sudo
                            'ansible_become_method': 'sudo',
                            'ansible_become_pass': fix_message.ssh_password
                        }
                    }
                }
            }
            inventory_path = os.path.join(private_data_dir, 'inventory.yml')
            with open(inventory_path, 'w') as f:
                yaml.dump(inventory, f)

            playbook = [{'hosts': 'all', 'gather_facts': False, 'tasks': playbook_tasks}]
            playbook_path = os.path.join(private_data_dir, 'fix_playbook.yml')
            with open(playbook_path, 'w') as f:
                yaml.dump(playbook, f)

            logger.info(f"Thread {thread_id}: Running ansible-runner for {len(playbook_tasks)} fix commands on {fix_message.ip_address}")
            
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
                full_error = f"Thread {thread_id}: Ansible run failed for {fix_message.ip_address}. Status: {runner.status}, RC: {runner.rc}. Output: {error_output}"
                logger.error(full_error)
                return [], f"Ansible connection failed or playbook execution error: {error_output[:500] or 'Check logs for details'}"
        
            # Xử lý kết quả của từng task
            for event in all_events:
                if event['event'] in ('runner_on_ok', 'runner_on_failed'):
                    task_name_from_event = event['event_data'].get('task')
                    if not task_name_from_event:
                        continue

                    fix_info = fixes_to_run.get(task_name_from_event)
                    if not fix_info:
                        logger.warning(f"Thread {thread_id}: Could not map event task '{task_name_from_event}' back to a fix command.")
                        continue
                        
                    task_result = event['event_data']['res']
                    fix_command = fix_info['command']
                    start_time = fix_info['start_time']
                    execution_time = time.time() - start_time
                    
                    output = task_result.get('stdout', '')
                    error = task_result.get('stderr', '')
                    rc = task_result.get('rc', 1)

                    status = "success"
                    message = "Fix command executed successfully"
                    details_error = None
                    
                    if rc != 0:
                        status = "failed"
                        message = "Fix command execution failed"
                        if error:
                            details_error = error[:500]
                        elif task_result.get('stderr_lines'):
                            details_error = "\n".join(task_result.get('stderr_lines', []))[:500]
                        else:
                            details_error = f"Command returned non-zero exit code: {rc}"

                    all_fix_results.append(FixResultInfo(
                        fix_command=fix_command,
                        status=status,
                        message=message,
                        details_error=details_error,
                        output=output[:1000] if output else None,  # Limit output length
                        execution_time=round(execution_time, 2)
                    ))

        return all_fix_results, None