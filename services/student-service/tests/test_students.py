"""
Unit tests – student-service
Run: pytest tests/ -v
"""
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.pool import StaticPool

from app.main import app
from app.db.session import Base, get_db

# ── In-memory SQLite for tests ────────────────────────────────────────────────
TEST_DB_URL = "sqlite+aiosqlite:///:memory:"

test_engine = create_async_engine(
    TEST_DB_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestSession = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)


async def override_get_db():
    async with TestSession() as session:
        yield session
        await session.rollback()


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(autouse=True, scope="session")
async def setup_db():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# ── Tests ─────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_health(client):
    r = await client.get("/api/v1/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_create_student(client):
    payload = {
        "student_id":    "SV9001",
        "full_name":     "Test Student",
        "email":         "test9001@example.com",
        "password":      "password123",
        "major":         "Engineering",
        "year_of_study": 2,
    }
    r = await client.post("/api/v1/students", json=payload)
    assert r.status_code == 201
    data = r.json()
    assert data["student_id"] == "SV9001"
    assert data["major"] == "Engineering"


@pytest.mark.asyncio
async def test_create_duplicate_student(client):
    payload = {
        "student_id": "SV9001",
        "full_name":  "Duplicate",
        "email":      "test9001@example.com",
        "password":   "password123",
        "major":      "IT",
        "year_of_study": 1,
    }
    r = await client.post("/api/v1/students", json=payload)
    assert r.status_code == 409


@pytest.mark.asyncio
async def test_invalid_student_id(client):
    payload = {
        "student_id": "BADID",
        "full_name":  "Bad",
        "email":      "bad@example.com",
        "password":   "password123",
        "major":      "IT",
    }
    r = await client.post("/api/v1/students", json=payload)
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_login(client):
    r = await client.post("/api/v1/auth/login", json={
        "email": "test9001@example.com",
        "password": "password123",
    })
    assert r.status_code == 200
    assert "access_token" in r.json()


@pytest.mark.asyncio
async def test_login_wrong_password(client):
    r = await client.post("/api/v1/auth/login", json={
        "email": "test9001@example.com",
        "password": "wrongpassword",
    })
    assert r.status_code == 401
