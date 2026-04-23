"""
Student service – business logic layer
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func
from fastapi import HTTPException, status
from typing import Optional

from app.models.student import Student
from app.schemas.student import StudentCreate, StudentUpdate
from app.core.security import hash_password, verify_password, create_access_token, create_refresh_token
from app.core.config import settings


class StudentService:

    # ── Auth ──────────────────────────────────────────────────────────────────
    async def login(self, db: AsyncSession, email: str, password: str) -> dict:
        stmt = select(Student).where(Student.email == email, Student.is_deleted == False)
        result = await db.execute(stmt)
        student = result.scalar_one_or_none()

        if not student or not verify_password(password, student.password_hash):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password",
            )
        if student.status == "suspended":
            raise HTTPException(status_code=403, detail="Account suspended")

        access_token  = create_access_token(
            subject=student.student_id,
            extra={"role": student.role, "name": student.full_name}
        )
        refresh_token = create_refresh_token(subject=student.student_id)
        return {
            "access_token":  access_token,
            "refresh_token": refresh_token,
            "token_type":    "bearer",
            "expires_in":    settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        }

    # ── CRUD ──────────────────────────────────────────────────────────────────
    async def create(self, db: AsyncSession, data: StudentCreate) -> Student:
        # Check duplicate
        exists = await db.execute(
            select(Student).where(
                (Student.student_id == data.student_id) | (Student.email == data.email)
            )
        )
        if exists.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="student_id or email already exists")

        student = Student(
            **data.model_dump(exclude={"password"}),
            password_hash=hash_password(data.password),
        )
        db.add(student)
        await db.flush()
        await db.refresh(student)
        return student

    async def get_by_id(self, db: AsyncSession, student_id: str) -> Student:
        result = await db.execute(
            select(Student).where(
                Student.student_id == student_id,
                Student.is_deleted == False,
            )
        )
        student = result.scalar_one_or_none()
        if not student:
            raise HTTPException(status_code=404, detail=f"Student {student_id} not found")
        return student

    async def list_students(
        self,
        db: AsyncSession,
        page: int = 1,
        page_size: int = 20,
        major: Optional[str] = None,
        status: Optional[str] = None,
        search: Optional[str] = None,
    ) -> dict:
        query = select(Student).where(Student.is_deleted == False)

        if major:
            query = query.where(Student.major.ilike(f"%{major}%"))
        if status:
            query = query.where(Student.status == status)
        if search:
            query = query.where(
                Student.full_name.ilike(f"%{search}%") |
                Student.student_id.ilike(f"%{search}%")
            )

        # Count
        count_q = select(func.count()).select_from(query.subquery())
        total = (await db.execute(count_q)).scalar()

        # Paginate
        query = query.offset((page - 1) * page_size).limit(page_size)
        items = (await db.execute(query)).scalars().all()

        return {"total": total, "page": page, "page_size": page_size, "items": items}

    async def update(self, db: AsyncSession, student_id: str, data: StudentUpdate) -> Student:
        student = await self.get_by_id(db, student_id)
        update_data = data.model_dump(exclude_none=True)
        for k, v in update_data.items():
            setattr(student, k, v)
        await db.flush()
        await db.refresh(student)
        return student

    async def delete(self, db: AsyncSession, student_id: str) -> dict:
        student = await self.get_by_id(db, student_id)
        student.is_deleted = True
        return {"message": f"Student {student_id} deleted"}

    async def change_password(
        self, db: AsyncSession, student_id: str, old_pw: str, new_pw: str
    ) -> dict:
        student = await self.get_by_id(db, student_id)
        if not verify_password(old_pw, student.password_hash):
            raise HTTPException(status_code=400, detail="Old password incorrect")
        student.password_hash = hash_password(new_pw)
        return {"message": "Password updated successfully"}


student_service = StudentService()
