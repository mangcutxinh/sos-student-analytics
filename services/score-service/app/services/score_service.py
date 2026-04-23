"""
Score service – business logic
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from fastapi import HTTPException
from typing import Optional

from app.models.score import Score
from app.schemas.score import ScoreCreate, ScoreUpdate


class ScoreService:

    async def create(self, db: AsyncSession, data: ScoreCreate) -> Score:
        # Prevent duplicate entry for same student+subject+semester
        existing = await db.execute(
            select(Score).where(
                Score.student_id == data.student_id,
                Score.subject    == data.subject,
                Score.semester   == data.semester,
                Score.is_deleted == False,
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(409, f"Score for {data.student_id}/{data.subject}/{data.semester} already exists")

        score = Score(**data.model_dump())
        score.gpa   = score.compute_gpa()
        score.grade = score.compute_grade()
        db.add(score)
        await db.flush()
        await db.refresh(score)
        return score

    async def bulk_create(self, db: AsyncSession, items: list[ScoreCreate]) -> dict:
        created, skipped = 0, 0
        for item in items:
            try:
                await self.create(db, item)
                created += 1
            except HTTPException:
                skipped += 1
        return {"created": created, "skipped_duplicates": skipped}

    async def get(self, db: AsyncSession, score_id: int) -> Score:
        result = await db.execute(
            select(Score).where(Score.id == score_id, Score.is_deleted == False)
        )
        score = result.scalar_one_or_none()
        if not score:
            raise HTTPException(404, f"Score {score_id} not found")
        return score

    async def list_by_student(
        self, db: AsyncSession, student_id: str,
        semester: Optional[str] = None,
    ) -> dict:
        q = select(Score).where(Score.student_id == student_id, Score.is_deleted == False)
        if semester:
            q = q.where(Score.semester == semester)
        items = (await db.execute(q)).scalars().all()
        total = len(items)
        return {"total": total, "items": items}

    async def list_by_subject(
        self, db: AsyncSession, subject: str, semester: Optional[str] = None,
    ) -> dict:
        q = select(Score).where(Score.subject == subject, Score.is_deleted == False)
        if semester:
            q = q.where(Score.semester == semester)
        items = (await db.execute(q)).scalars().all()
        return {"total": len(items), "items": items}

    async def update(self, db: AsyncSession, score_id: int, data: ScoreUpdate) -> Score:
        score = await self.get(db, score_id)
        for k, v in data.model_dump(exclude_none=True).items():
            setattr(score, k, v)
        score.gpa   = score.compute_gpa()
        score.grade = score.compute_grade()
        await db.flush()
        await db.refresh(score)
        return score

    async def delete(self, db: AsyncSession, score_id: int) -> dict:
        score = await self.get(db, score_id)
        score.is_deleted = True
        return {"message": f"Score {score_id} deleted"}

    async def get_summary(
        self, db: AsyncSession, student_id: str, semester: str,
    ) -> dict:
        result = await db.execute(
            select(Score).where(
                Score.student_id == student_id,
                Score.semester   == semester,
                Score.is_deleted == False,
            )
        )
        scores = result.scalars().all()
        if not scores:
            raise HTTPException(404, f"No scores for {student_id} in {semester}")

        gpas      = [s.gpa for s in scores if s.gpa]
        avg_gpa   = round(sum(gpas) / len(gpas), 2) if gpas else 0
        fail_count = sum(1 for s in scores if s.grade == "F")
        pass_count = len(scores) - fail_count
        overall    = "A" if avg_gpa>=8.5 else "B" if avg_gpa>=7 else "C" if avg_gpa>=5.5 else "D" if avg_gpa>=4 else "F"

        return {
            "student_id":    student_id,
            "semester":      semester,
            "subject_count": len(scores),
            "avg_gpa":       avg_gpa,
            "avg_midterm":   round(sum(s.midterm_score for s in scores) / len(scores), 2),
            "avg_final":     round(sum(s.final_score   for s in scores) / len(scores), 2),
            "avg_attendance":round(sum(s.attendance_rate for s in scores) / len(scores), 2),
            "overall_grade": overall,
            "pass_count":    pass_count,
            "fail_count":    fail_count,
        }


score_service = ScoreService()
