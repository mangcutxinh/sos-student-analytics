"""
Student API v1 endpoints
"""
from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.db.session import get_db
from app.core.security import get_current_user, require_role
from app.schemas.student import (
    StudentCreate, StudentUpdate, StudentResponse,
    StudentListResponse, LoginRequest, TokenResponse,
    RefreshRequest, ChangePasswordRequest,
)
from app.services.student_service import student_service
from app.core.security import decode_token, create_access_token
from app.core.config import settings

router = APIRouter()


# ── Auth ──────────────────────────────────────────────────────────────────────
@router.post("/auth/login", response_model=TokenResponse, tags=["Auth"])
async def login(body: LoginRequest, db: AsyncSession = Depends(get_db)):
    """Authenticate student/admin and return JWT tokens."""
    return await student_service.login(db, body.email, body.password)


@router.post("/auth/refresh", response_model=TokenResponse, tags=["Auth"])
async def refresh_token(body: RefreshRequest):
    """Get new access token using refresh token."""
    payload = decode_token(body.refresh_token)
    if payload.get("type") != "refresh":
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not a refresh token")
    new_access = create_access_token(subject=payload["sub"])
    return {
        "access_token":  new_access,
        "refresh_token": body.refresh_token,
        "token_type":    "bearer",
        "expires_in":    settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
    }


# ── Students CRUD ─────────────────────────────────────────────────────────────
@router.post(
    "/students",
    response_model=StudentResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Students"],
)
async def create_student(
    body: StudentCreate,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(require_role("admin", "lecturer")),
):
    """Create a new student (admin/lecturer only)."""
    return await student_service.create(db, body)


@router.get("/students", response_model=StudentListResponse, tags=["Students"])
async def list_students(
    page:      int            = Query(1,    ge=1),
    page_size: int            = Query(20,   ge=1, le=100),
    major:     Optional[str]  = Query(None),
    status:    Optional[str]  = Query(None),
    search:    Optional[str]  = Query(None, description="Search by name or student_id"),
    db:        AsyncSession   = Depends(get_db),
    _:         dict           = Depends(get_current_user),
):
    """List students with pagination, filtering, search."""
    return await student_service.list_students(db, page, page_size, major, status, search)


@router.get("/students/{student_id}", response_model=StudentResponse, tags=["Students"])
async def get_student(
    student_id: str,
    db: AsyncSession = Depends(get_db),
    current: dict = Depends(get_current_user),
):
    """Get student by student_id. Students can only view themselves."""
    if current["role"] == "student" and current["sub"] != student_id:
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="Access denied")
    return await student_service.get_by_id(db, student_id)


@router.patch("/students/{student_id}", response_model=StudentResponse, tags=["Students"])
async def update_student(
    student_id: str,
    body: StudentUpdate,
    db: AsyncSession = Depends(get_db),
    current: dict = Depends(get_current_user),
):
    """Update student profile."""
    if current["role"] == "student" and current["sub"] != student_id:
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="Access denied")
    return await student_service.update(db, student_id, body)


@router.delete(
    "/students/{student_id}",
    status_code=status.HTTP_200_OK,
    tags=["Students"],
)
async def delete_student(
    student_id: str,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(require_role("admin")),
):
    """Soft-delete a student (admin only)."""
    return await student_service.delete(db, student_id)


@router.put("/students/{student_id}/password", tags=["Students"])
async def change_password(
    student_id: str,
    body: ChangePasswordRequest,
    db: AsyncSession = Depends(get_db),
    current: dict = Depends(get_current_user),
):
    """Change password (own account only)."""
    if current["sub"] != student_id:
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="Cannot change another student's password")
    return await student_service.change_password(db, student_id, body.old_password, body.new_password)


# ── Health ────────────────────────────────────────────────────────────────────
@router.get("/health", tags=["System"])
async def health():
    return {"service": "student-service", "status": "ok"}
