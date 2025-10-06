import redis
import json
from datetime import datetime
import uuid

# Kết nối Redis
redis_client = redis.Redis(host='localhost', port=6380, db=0, decode_responses=True)

# Tạo fix request message
fix_data = {
    "fix_request_id": str(uuid.uuid4()),
    "fix_id": "fix_" + str(uuid.uuid4()),
    "ip_address": "192.168.122.121",
    "port": 22,
    "ssh_username": "maybetuandat",
    "ssh_password": "1",
    "fix_type": "security_hardening",
    "user_id": 1,
    "instance_id": 101,
    "timestamp": datetime.now().isoformat(),
    "suggest_fix":[

    # 1. Xác định process java/tomcat/jre/jdk/kubelet đang chạy bằng root
    "ps -ef | awk '$1==\"root\" && $8 ~ /(java|tomcat|jre|jdk|kubelet)/{print $2, $8}'",

    # 2. Backup file cấu hình service (ví dụ tomcat)
    "sudo cp -r /etc/tomcat /etc/tomcat.bak.$(date +%F-%H%M%S) || true",

    # 3. Stop service chạy root (ví dụ tomcat)
    "sudo systemctl stop tomcat || true",

    # 4. Tạo user dịch vụ (nếu chưa có)
    "id -u tomcat || sudo useradd -r -s /bin/false tomcat",

    # 5. Cấp quyền sở hữu thư mục cho user dịch vụ
    "sudo chown -R tomcat:tomcat /opt/tomcat || true",

    # 6. Nếu cần lệnh root thì gán quyền sudo không password cho user dịch vụ
    "echo 'tomcat ALL=(ALL) NOPASSWD: /bin/systemctl restart tomcat' | sudo tee /etc/sudoers.d/tomcat",

    # 7. Start lại service dưới user dịch vụ
    "sudo -u tomcat /opt/tomcat/bin/startup.sh || sudo systemctl start tomcat"
]
}

# Tạo message envelope
message = {
    "timestamp": datetime.now().isoformat(),
    "type": "fix_request",
    "data": fix_data
}

# Publish vào Redis
channel = "fix:request"
message_json = json.dumps(message, default=str)
num_subscribers = redis_client.publish(channel, message_json)

print("="*80)
print("🔧 FIX REQUEST PUSHED TO REDIS")
print("="*80)
print(f"Channel: {channel}")
print(f"Subscribers: {num_subscribers}")
print(f"Fix Request ID: {fix_data['fix_request_id']}")
print(f"IP Address: {fix_data['ip_address']}")
print(f"Fix Commands: {len(fix_data['suggest_fix'])}")
print("\nCommands:")
for i, cmd in enumerate(fix_data['suggest_fix'], 1):
    print(f"  {i}. {cmd}")
print("="*80)

redis_client.close()