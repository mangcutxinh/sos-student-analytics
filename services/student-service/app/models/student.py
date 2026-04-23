"""
Student ORM model
"""
from sqlalchemy import Column, String, Integer, Boolean, DateTime, Enum as SAEnum
from sqlalchemy.sql import func
import enum
from app.db.session import Base


class GenderEnum(str, enum.Enum):
    male = "male"
    female = "female"
    other = "other"


class StatusEnum(str, enum.Enum):
    active = "active"
    inactive = "inactive"
    graduated = "graduated"
    suspended = "suspended"


class Student(Base):
    __tablename__ = "students"

    id            = Column(Integer, primary_key=True, index=True, autoincrement=True)
    student_id    = Column(String(10), unique=True, index=True, nullable=False)  # SV0001
    full_name     = Column(String(100), nullable=False)
    email         = Column(String(120), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    phone         = Column(String(15), nullable=True)
    gender        = Column(SAEnum(GenderEnum), default=GenderEnum.other)
    major         = Column(String(60), nullable=False)
    year_of_study = Column(Integer, default=1)
    semester      = Column(String(10), default="2024-1")
    status        = Column(SAEnum(StatusEnum), default=StatusEnum.active)
    role          = Column(String(20), default="student")   # student | lecturer | admin

    created_at    = Column(DateTime(timezone=True), server_default=func.now())
    updated_at    = Column(DateTime(timezone=True), onupdate=func.now())
    is_deleted    = Column(Boolean, default=False)

    def __repr__(self):
        return f"<Student {self.student_id} – {self.full_name}>"
