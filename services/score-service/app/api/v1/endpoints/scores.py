from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.db.session import get_db
from app.schemas.score import ScoreCreate, ScoreUpdate, ScoreResponse, ScoreListResponse, BulkScoreCreate, ScoreSummary
from app.services.score_service import score_service

router = APIRouter()


@router.post("/scores", response_model=ScoreResponse, status_code=201, tags=["Scores"])
async def create_score(body: ScoreCreate, db: AsyncSession = Depends(get_db)):
    return await score_service.create(db, body)


@router.post("/scores/bulk", tags=["Scores"])
async def bulk_create(body: BulkScoreCreate, db: AsyncSession = Depends(get_db)):
    return await score_service.bulk_create(db, body.scores)


@router.get("/scores/{score_id}", response_model=ScoreResponse, tags=["Scores"])
async def get_score(score_id: int, db: AsyncSession = Depends(get_db)):
    return await score_service.get(db, score_id)


@router.patch("/scores/{score_id}", response_model=ScoreResponse, tags=["Scores"])
async def update_score(score_id: int, body: ScoreUpdate, db: AsyncSession = Depends(get_db)):
    return await score_service.update(db, score_id, body)


@router.delete("/scores/{score_id}", tags=["Scores"])
async def delete_score(score_id: int, db: AsyncSession = Depends(get_db)):
    return await score_service.delete(db, score_id)


@router.get("/scores/student/{student_id}", response_model=ScoreListResponse, tags=["Scores"])
async def scores_by_student(
    student_id: str,
    semester: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await score_service.list_by_student(db, student_id, semester)


@router.get("/scores/subject/{subject}", response_model=ScoreListResponse, tags=["Scores"])
async def scores_by_subject(
    subject: str,
    semester: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await score_service.list_by_subject(db, subject, semester)


@router.get("/scores/summary/{student_id}", response_model=ScoreSummary, tags=["Scores"])
async def score_summary(
    student_id: str,
    semester: str = Query("2024-1"),
    db: AsyncSession = Depends(get_db),
):
    return await score_service.get_summary(db, student_id, semester)


@router.get("/health", tags=["System"])
async def health():
    return {"service": "score-service", "status": "ok"}
