from sqlalchemy import Column, Integer, String, Float, JSON, DateTime
from sqlalchemy.sql import func
from app.db.session import Base


class AnalyticsSnapshot(Base):
    """Stores pre-computed analytics snapshots (refreshed by ETL / on-demand)."""
    __tablename__ = "analytics_snapshots"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_type = Column(String(40), nullable=False, index=True)
    # e.g. "major_summary" | "subject_summary" | "gpa_distribution" | "at_risk"
    dimension    = Column(String(80), nullable=True, index=True)  # major name, subject, etc.
    semester     = Column(String(10), nullable=False, default="2024-1", index=True)
    data         = Column(JSON, nullable=False)
    computed_at  = Column(DateTime(timezone=True), server_default=func.now())
    updated_at   = Column(DateTime(timezone=True), onupdate=func.now())
