"""
Score ORM model
"""
from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, CheckConstraint
from sqlalchemy.sql import func
from app.db.session import Base


class Score(Base):
    __tablename__ = "scores"
    __table_args__ = (
        CheckConstraint("midterm_score BETWEEN 0 AND 10", name="chk_midterm"),
        CheckConstraint("final_score   BETWEEN 0 AND 10", name="chk_final"),
        CheckConstraint("attendance_rate BETWEEN 0 AND 1", name="chk_attendance"),
    )

    id               = Column(Integer, primary_key=True, autoincrement=True)
    student_id       = Column(String(10), index=True, nullable=False)
    subject          = Column(String(60), nullable=False)
    semester         = Column(String(10), nullable=False, default="2024-1")
    midterm_score    = Column(Float, nullable=False)
    final_score      = Column(Float, nullable=False)
    attendance_rate  = Column(Float, nullable=False, default=1.0)
    gpa              = Column(Float, nullable=True)           # computed on save
    grade            = Column(String(2), nullable=True)
    lecturer_id      = Column(String(20), nullable=True)
    notes            = Column(String(500), nullable=True)

    created_at       = Column(DateTime(timezone=True), server_default=func.now())
    updated_at       = Column(DateTime(timezone=True), onupdate=func.now())
    is_deleted       = Column(Boolean, default=False)

    def compute_gpa(self) -> float:
        return round((self.midterm_score * 0.3 + self.final_score * 0.7) * self.attendance_rate, 2)

    def compute_grade(self) -> str:
        g = self.gpa or self.compute_gpa()
        if g >= 8.5: return "A"
        if g >= 7.0: return "B"
        if g >= 5.5: return "C"
        if g >= 4.0: return "D"
        return "F"
