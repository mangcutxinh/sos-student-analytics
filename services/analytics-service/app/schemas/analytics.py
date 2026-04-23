from pydantic import BaseModel, ConfigDict
from typing import Optional, Any
from datetime import datetime


class SnapshotResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    snapshot_type: str
    dimension: Optional[str]
    semester: str
    data: Any
    computed_at: datetime


class GPADistributionItem(BaseModel):
    grade: str
    count: int
    percentage: float


class MajorStat(BaseModel):
    major: str
    student_count: int
    avg_gpa: float
    pass_rate: float
    fail_rate: float


class SubjectStat(BaseModel):
    subject: str
    enrollments: int
    avg_gpa: float
    avg_midterm: float
    avg_final: float
    fail_rate: float
    difficulty: str


class AtRiskStudent(BaseModel):
    student_id: str
    avg_gpa: float
    fail_count: int
    attendance_avg: float
    risk_level: str   # HIGH | MEDIUM


class OverviewResponse(BaseModel):
    semester: str
    total_students: int
    total_scores: int
    avg_gpa: float
    pass_rate: float
    excellent_rate: float
    at_risk_count: int
    top_major: str
    hardest_subject: str
