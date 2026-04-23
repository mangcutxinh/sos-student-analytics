"""
Pydantic request / response schemas for Student service
"""
from pydantic import BaseModel, EmailStr, field_validator, ConfigDict
from typing import Optional
from datetime import datetime
from enum import Enum


class GenderEnum(str, Enum):
    male = "male"
    female = "female"
    other = "other"


class StatusEnum(str, Enum):
    active = "active"
    inactive = "inactive"
    graduated = "graduated"
    suspended = "suspended"


# ── Auth ─────────────────────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds


class RefreshRequest(BaseModel):
    refresh_token: str


# ── Student CRUD ──────────────────────────────────────────────────────────────
class StudentCreate(BaseModel):
    student_id: str
    full_name: str
    email: EmailStr
    password: str
    phone: Optional[str] = None
    gender: GenderEnum = GenderEnum.other
    major: str
    year_of_study: int = 1
    semester: str = "2024-1"

    @field_validator("student_id")
    @classmethod
    def validate_student_id(cls, v: str) -> str:
        if not v.startswith("SV") or not v[2:].isdigit():
            raise ValueError("student_id must be in format SV0001")
        return v.upper()

    @field_validator("year_of_study")
    @classmethod
    def validate_year(cls, v: int) -> int:
        if not 1 <= v <= 6:
            raise ValueError("year_of_study must be 1–6")
        return v


class StudentUpdate(BaseModel):
    full_name: Optional[str] = None
    phone: Optional[str] = None
    gender: Optional[GenderEnum] = None
    major: Optional[str] = None
    year_of_study: Optional[int] = None
    status: Optional[StatusEnum] = None


class StudentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    student_id: str
    full_name: str
    email: str
    phone: Optional[str]
    gender: str
    major: str
    year_of_study: int
    semester: str
    status: str
    role: str
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class StudentListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[StudentResponse]


# ── Password change ───────────────────────────────────────────────────────────
class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str

    @field_validator("new_password")
    @classmethod
    def strong_password(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        return v
