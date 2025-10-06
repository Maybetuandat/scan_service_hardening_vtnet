from typing import List, Optional
class FixMessage:
    ssh_username: str
    ssh_password: Optional[str] = None
    suggest_fix: List[str]
    scan_id: str
    ip_address: str
    port: int
    scan_type: str