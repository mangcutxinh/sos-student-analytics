from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.core.config import settings
from app.db.session import get_db
from app.schemas.score import ScoreCreate, ScoreUpdate, ScoreResponse, ScoreListResponse, BulkScoreCreate, ScoreSummary
from app.services.score_service import score_service

router = APIRouter()


# ---------------------------------------------------------------------------
# Helper: route to CSV provider when DATA_MODE=csv
# ---------------------------------------------------------------------------

def _csv_items(items: list[dict]) -> dict:
    """Return paginated envelope matching ScoreListResponse shape."""
    return {"total": len(items), "items": items}


# ---------------------------------------------------------------------------
# List-all endpoint (used by analytics-service: GET /api/v1/scores?semester=)
# ---------------------------------------------------------------------------

@router.get("/scores", tags=["Scores"])
async def list_scores(
    semester: Optional[str] = Query(None),
    page_size: int = Query(10000),
    db: AsyncSession = Depends(get_db),
):
    """Return all scores.  In CSV mode the semester parameter is accepted but ignored."""
    if settings.DATA_MODE == "csv":
        from app.services.csv_score_provider import load_scores
        items = load_scores()
        return {"total": len(items), "items": items[:page_size]}

    from sqlalchemy import select, false
    from app.models.score import Score
    q = select(Score).where(Score.is_deleted.is_(False))
    if semester:
        q = q.where(Score.semester == semester)
    rows = (await db.execute(q)).scalars().all()
    return {"total": len(rows), "items": rows}


@router.post("/scores", response_model=ScoreResponse, status_code=201, tags=["Scores"])
async def create_score(body: ScoreCreate, db: AsyncSession = Depends(get_db)):
    return await score_service.create(db, body)


@router.post("/scores/bulk", tags=["Scores"])
async def bulk_create(body: BulkScoreCreate, db: AsyncSession = Depends(get_db)):
    return await score_service.bulk_create(db, body.scores)


@router.get("/scores/student/{student_id}", tags=["Scores"])
async def scores_by_student(
    student_id: str,
    semester: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    if settings.DATA_MODE == "csv":
        from app.services.csv_score_provider import load_scores
        items = [s for s in load_scores() if s["student_id"] == str(student_id)]
        return _csv_items(items)
    return await score_service.list_by_student(db, student_id, semester)


@router.get(
    "/scores/subject/{subject}",
    tags=["Scores"],
    description=(
        "Return scores for a specific subject. "
        "In CSV mode the dataset has no subject dimension, "
        "so this always returns an empty list with an explanatory note. "
        "Use GET /scores to retrieve all students."
    ),
)
async def scores_by_subject(
    subject: str,
    semester: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    if settings.DATA_MODE == "csv":
        return {"total": 0, "items": [], "note": "Dataset has no subject dimension; use GET /scores for all students."}
    return await score_service.list_by_subject(db, subject, semester)


@router.get("/scores/summary/{student_id}", response_model=ScoreSummary, tags=["Scores"])
async def score_summary(
    student_id: str,
    semester: str = Query("2024-1"),
    db: AsyncSession = Depends(get_db),
):
    if settings.DATA_MODE == "csv":
        from app.services.csv_score_provider import load_scores
        items = [s for s in load_scores() if s["student_id"] == str(student_id)]
        if not items:
            raise HTTPException(404, f"No scores found for student {student_id}")
        s = items[0]
        return {
            "student_id":    student_id,
            "semester":      semester,
            "subject_count": len(items),
            "avg_gpa":       s["grade_10"],
            "avg_midterm":   s["midterm_score"],
            "avg_final":     s["final_score"],
            "avg_attendance":s["attendance_rate"],
            "overall_grade": s["grade"],
            "pass_count":    0 if s["is_failing"] else 1,
            "fail_count":    1 if s["is_failing"] else 0,
        }
    return await score_service.get_summary(db, student_id, semester)


@router.get("/scores/{score_id}", response_model=ScoreResponse, tags=["Scores"])
async def get_score(score_id: int, db: AsyncSession = Depends(get_db)):
    return await score_service.get(db, score_id)


@router.patch("/scores/{score_id}", response_model=ScoreResponse, tags=["Scores"])
async def update_score(score_id: int, body: ScoreUpdate, db: AsyncSession = Depends(get_db)):
    return await score_service.update(db, score_id, body)


@router.delete("/scores/{score_id}", tags=["Scores"])
async def delete_score(score_id: int, db: AsyncSession = Depends(get_db)):
    return await score_service.delete(db, score_id)


@router.get("/health", tags=["System"])
async def health():
    return {"service": "score-service", "status": "ok", "mode": settings.DATA_MODE}
