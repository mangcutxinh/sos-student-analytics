"""
Student Service – FastAPI application entry point
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.core.config import settings
from app.db.session import create_tables
from app.api.v1.endpoints.students import router as student_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await create_tables()  # <-- NÓ ĐÃ CÓ LỆNH TẠO BẢNG Ở ĐÂY RỒI!
    print(f"✅ {settings.APP_NAME} started – tables ready")
    yield
    # Shutdown
    print(f"🛑 {settings.APP_NAME} shutting down")

app = FastAPI(
    title="Student Service",
    description="...",
    version=settings.APP_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Giữ nguyên phần CORS và Routes phía dưới của bạn...
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(student_router, prefix="/api/v1")

@app.get("/", tags=["Root"])
async def root():
    return {
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "docs":    "/docs",
    }