"""
Unit tests – score-service
Run: pytest tests/ -v
"""
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.pool import StaticPool

from app.main import app
from app.db.session import Base, get_db

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"
test_engine = create_async_engine(TEST_DB_URL, connect_args={"check_same_thread": False}, poolclass=StaticPool)
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


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


SCORE_PAYLOAD = {
    "student_id":      "SV0001",
    "subject":         "Math",
    "semester":        "2024-1",
    "midterm_score":   7.5,
    "final_score":     8.0,
    "attendance_rate": 0.9,
}


@pytest.mark.asyncio
async def test_health(client):
    r = await client.get("/api/v1/health")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_create_score(client):
    r = await client.post("/api/v1/scores", json=SCORE_PAYLOAD)
    assert r.status_code == 201
    data = r.json()
    assert data["grade"] in ("A", "B", "C", "D", "F")
    assert data["gpa"] is not None
    # GPA = (7.5*0.3 + 8.0*0.7) * 0.9 = (2.25+5.6)*0.9 = 7.065
    assert abs(data["gpa"] - 7.065) < 0.01


@pytest.mark.asyncio
async def test_duplicate_score(client):
    r = await client.post("/api/v1/scores", json=SCORE_PAYLOAD)
    assert r.status_code == 409


@pytest.mark.asyncio
async def test_score_out_of_range(client):
    bad = {**SCORE_PAYLOAD, "student_id": "SV0002", "midterm_score": 11.0}
    r = await client.post("/api/v1/scores", json=bad)
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_get_scores_by_student(client):
    r = await client.get("/api/v1/scores/student/SV0001?semester=2024-1")
    assert r.status_code == 200
    assert r.json()["total"] >= 1


@pytest.mark.asyncio
async def test_update_score(client):
    # create a fresh score first
    payload = {**SCORE_PAYLOAD, "student_id": "SV0003", "subject": "Physics"}
    create_r = await client.post("/api/v1/scores", json=payload)
    score_id = create_r.json()["id"]

    update_r = await client.patch(f"/api/v1/scores/{score_id}", json={"final_score": 9.0})
    assert update_r.status_code == 200
    assert update_r.json()["final_score"] == 9.0


@pytest.mark.asyncio
async def test_bulk_create(client):
    r = await client.post("/api/v1/scores/bulk", json={"scores": [
        {**SCORE_PAYLOAD, "student_id": "SV0010", "subject": "History"},
        {**SCORE_PAYLOAD, "student_id": "SV0011", "subject": "Biology"},
    ]})
    assert r.status_code == 200
    assert r.json()["created"] == 2
