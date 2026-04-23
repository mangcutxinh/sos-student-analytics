from pydantic import BaseModel, EmailStr, ConfigDict
from typing import Optional
from datetime import datetime
from enum import Enum


class ChannelEnum(str, Enum):
    email  = "email"
    push   = "push"
    sms    = "sms"
    in_app = "in_app"


class NotificationCreate(BaseModel):
    recipient_id:    str
    recipient_email: Optional[EmailStr] = None
    channel:         ChannelEnum = ChannelEnum.in_app
    event_type:      str
    title:           str
    body:            str


class BulkNotificationCreate(BaseModel):
    """Send same notification to multiple recipients."""
    recipient_ids:   list[str]
    recipient_emails: Optional[list[EmailStr]] = None
    channel:         ChannelEnum = ChannelEnum.in_app
    event_type:      str
    title:           str
    body:            str


class ScorePostedEvent(BaseModel):
    """Convenience payload — triggers score_posted notification."""
    student_id:    str
    student_email: Optional[EmailStr] = None
    subject:       str
    gpa:           float
    grade:         str
    semester:      str


class AtRiskAlertEvent(BaseModel):
    """Triggers at_risk_alert notification for a list of students."""
    students: list[dict]   # [{student_id, avg_gpa, fail_count, risk_level}]
    semester: str


class NotificationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:             int
    recipient_id:   str
    channel:        str
    event_type:     str
    title:          str
    body:           str
    status:         str
    sent_at:        Optional[datetime]
    created_at:     datetime
    is_read:        bool


class NotificationListResponse(BaseModel):
    total: int
    items: list[NotificationResponse]
