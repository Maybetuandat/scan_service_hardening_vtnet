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
    "suggest_fix": [
    # Tạo banner với thông tin động
    "sudo bash -c 'cat > /etc/ssh/banner << \"EOF\"\n"
    "=======================================================\n"
    "        WARNING: AUTHORIZED ACCESS ONLY\n"
    "=======================================================\n"
    "\n"
    "Welcome to $(hostname)\n"
    "System: $(uname -s) $(uname -r)\n"
    "\n"
    "This computer system is for authorized users only.\n"
    "Individuals using this system without authority, or\n"
    "in excess of their authority, are subject to having\n"
    "all their activities monitored and recorded.\n"
    "\n"
    "Anyone using this system expressly consents to such\n"
    "monitoring and is advised that if such monitoring\n"
    "reveals possible criminal activity, system personnel\n"
    "may provide the evidence to law enforcement officials.\n"
    "\n"
    "=======================================================\n"
    "EOF'",
    
    # Enable banner trong SSH config
    "sudo sed -i 's/^#*Banner.*/Banner \\/etc\\/ssh\\/banner/' /etc/ssh/sshd_config",
    
    # Thêm dòng Banner nếu chưa có
    "grep -q '^Banner' /etc/ssh/sshd_config || echo 'Banner /etc/ssh/banner' | sudo tee -a /etc/ssh/sshd_config",
    
    # Set quyền đọc cho mọi user
    "sudo chmod 644 /etc/ssh/banner",
    
    # Test SSH config trước khi restart
    "sudo sshd -t",
    
    # Restart SSH service
    "sudo systemctl restart sshd || sudo service sshd restart"
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