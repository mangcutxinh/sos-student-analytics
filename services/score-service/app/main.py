from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.core.config import settings
from app.db.session import create_tables
from app.api.v1.endpoints.scores import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_tables()
    print(f"✅ {settings.APP_NAME} ready")
    yield


app = FastAPI(
    title="Score Service",
    description="Manages student scores, GPA computation, grade assignment.",
    version=settings.APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api/v1")


@app.get("/")
async def root():
    return {"service": settings.APP_NAME, "version": settings.APP_VERSION, "docs": "/docs"}
