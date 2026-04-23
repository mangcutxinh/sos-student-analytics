from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.services.analytics_service import analytics_service

router = APIRouter()


async def _all_scores(semester: str, svc=analytics_service):
    """Helper: fetch all scores for a semester from score-service."""
    return await svc._fetch_scores(semester=semester)


@router.get("/analytics/overview", tags=["Analytics"])
async def overview(semester: str = Query("2024-1")):
    """High-level KPIs: total students, avg GPA, pass rate, at-risk count."""
    scores = await _all_scores(semester)
    result = await analytics_service.get_overview(scores)
    result["semester"] = semester
    return result


@router.get("/analytics/gpa-distribution", tags=["Analytics"])
async def gpa_distribution(semester: str = Query("2024-1")):
    """Grade A/B/C/D/F distribution across all students."""
    scores = await _all_scores(semester)
    return await analytics_service.gpa_distribution(scores)


@router.get("/analytics/majors", tags=["Analytics"])
async def major_analytics(semester: str = Query("2024-1")):
    """Per-major breakdown: avg GPA, pass/fail rates, student count."""
    scores = await _all_scores(semester)
    return await analytics_service.major_stats(scores)


@router.get("/analytics/subjects", tags=["Analytics"])
async def subject_analytics(semester: str = Query("2024-1")):
    """Per-subject stats: avg scores, fail rate, difficulty label."""
    scores = await _all_scores(semester)
    return await analytics_service.subject_stats(scores)


@router.get("/analytics/at-risk", tags=["Analytics"])
async def at_risk(semester: str = Query("2024-1")):
    """Students flagged as at-risk (low GPA, high fail count, low attendance)."""
    scores = await _all_scores(semester)
    return await analytics_service.at_risk_students(scores)


@router.get("/analytics/student/{student_id}", tags=["Analytics"])
async def student_trend(student_id: str, semester: str = Query("2024-1")):
    """Full performance breakdown for a single student."""
    return await analytics_service.student_trend(student_id, semester)


@router.get("/health", tags=["System"])
async def health():
    return {"service": "analytics-service", "status": "ok"}
