from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class FixMessage(BaseModel):
    ip_address: str
    port: int
    ssh_username: str
    ssh_password: Optional[str] = None
    
    # Fix info
    fix_id: str
    fix_type: str
    suggest_fix: List[str] 
    
    # Metadata
    timestamp: datetime = Field(default_factory=datetime.now)
    fix_request_id: Optional[str] = None
    user_id: Optional[int] = None
    assessment_id: Optional[int] = None
    
    class Config:
        from_attributes = True


class FixResultInfo(BaseModel):
    
    fix_command: str
    status: str  # "success", "failed"
    message: str
    details_error: Optional[str] = None
    output: Optional[str] = None
    execution_time: Optional[float] = None  # seconds
    
    class Config:
        from_attributes = True


class FixResponseMessage(BaseModel):
    """Message phản hồi sau khi fix được thực thi xong"""
    fix_request_id: Optional[str]
    fix_id: str
    ip_address: str
    
    user_id: Optional[int] = None
    
    status: str  # "completed", "failed"
    detail_error: Optional[str] = None
    
    total_fixes: int
    fixes_success: int
    fixes_failed: int
    
    fix_results: List[FixResultInfo] = Field(default_factory=list)
    
    timestamp: datetime = Field(default_factory=datetime.now)
    
    class Config:
        from_attributes = True