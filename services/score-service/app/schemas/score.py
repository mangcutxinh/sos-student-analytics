from pydantic import BaseModel, field_validator, ConfigDict
from typing import Optional
from datetime import datetime


class ScoreCreate(BaseModel):
    student_id: str
    subject: str
    semester: str = "2024-1"
    midterm_score: float
    final_score: float
    attendance_rate: float = 1.0
    lecturer_id: Optional[str] = None
    notes: Optional[str] = None

    @field_validator("midterm_score", "final_score")
    @classmethod
    def validate_score(cls, v):
        if not 0 <= v <= 10:
            raise ValueError("Score must be between 0 and 10")
        return round(v, 1)

    @field_validator("attendance_rate")
    @classmethod
    def validate_attendance(cls, v):
        if not 0 <= v <= 1:
            raise ValueError("Attendance rate must be between 0 and 1")
        return round(v, 2)


class ScoreUpdate(BaseModel):
    midterm_score: Optional[float] = None
    final_score: Optional[float] = None
    attendance_rate: Optional[float] = None
    notes: Optional[str] = None


class ScoreResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    student_id: str
    subject: str
    semester: str
    midterm_score: float
    final_score: float
    attendance_rate: float
    gpa: Optional[float]
    grade: Optional[str]
    lecturer_id: Optional[str]
    notes: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class ScoreListResponse(BaseModel):
    total: int
    items: list[ScoreResponse]


class ScoreSummary(BaseModel):
    student_id: str
    semester: str
    subject_count: int
    avg_gpa: float
    avg_midterm: float
    avg_final: float
    avg_attendance: float
    overall_grade: str
    pass_count: int
    fail_count: int


class BulkScoreCreate(BaseModel):
    scores: list[ScoreCreate]
