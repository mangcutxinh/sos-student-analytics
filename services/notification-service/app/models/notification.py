from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, Enum as SAEnum
from sqlalchemy.sql import func
import enum
from app.db.session import Base


class ChannelEnum(str, enum.Enum):
    email = "email"
    push  = "push"
    sms   = "sms"
    in_app = "in_app"


class StatusEnum(str, enum.Enum):
    pending   = "pending"
    sent      = "sent"
    failed    = "failed"
    cancelled = "cancelled"


class Notification(Base):
    __tablename__ = "notifications"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    recipient_id = Column(String(20), nullable=False, index=True)   # student_id or user_id
    recipient_email = Column(String(120), nullable=True)
    channel      = Column(SAEnum(ChannelEnum), nullable=False, default=ChannelEnum.in_app)
    event_type   = Column(String(50), nullable=False, index=True)
    # e.g. "score_posted" | "at_risk_alert" | "grade_updated" | "welcome"
    title        = Column(String(200), nullable=False)
    body         = Column(Text, nullable=False)
    status       = Column(SAEnum(StatusEnum), default=StatusEnum.pending)
    error_msg    = Column(String(500), nullable=True)
    sent_at      = Column(DateTime(timezone=True), nullable=True)
    created_at   = Column(DateTime(timezone=True), server_default=func.now())
    is_read      = Column(Boolean, default=False)
