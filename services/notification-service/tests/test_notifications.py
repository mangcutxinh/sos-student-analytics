"""
Unit tests – notification-service
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


@pytest.mark.asyncio
async def test_health(client):
    r = await client.get("/api/v1/health")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_send_in_app(client):
    r = await client.post("/api/v1/notifications", json={
        "recipient_id": "SV0001",
        "channel":      "in_app",
        "event_type":   "test_event",
        "title":        "Hello",
        "body":         "Test notification",
    })
    assert r.status_code == 201
    assert r.json()["status"] == "sent"


@pytest.mark.asyncio
async def test_score_posted_event(client):
    r = await client.post("/api/v1/notifications/events/score-posted", json={
        "student_id": "SV0001",
        "subject":    "Math",
        "gpa":        8.5,
        "grade":      "A",
        "semester":   "2024-1",
    })
    assert r.status_code == 201
    assert r.json()["event_type"] == "score_posted"


@pytest.mark.asyncio
async def test_at_risk_event(client):
    r = await client.post("/api/v1/notifications/events/at-risk", json={
        "semester": "2024-1",
        "students": [
            {"student_id": "SV0002", "avg_gpa": 3.5, "fail_count": 2, "risk_level": "HIGH"},
            {"student_id": "SV0003", "avg_gpa": 4.8, "fail_count": 1, "risk_level": "MEDIUM"},
        ]
    })
    assert r.status_code == 200
    assert r.json()["at_risk_notifications_sent"] == 2


@pytest.mark.asyncio
async def test_list_and_mark_read(client):
    # list
    r = await client.get("/api/v1/notifications/SV0001")
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) >= 1
    notif_id = items[0]["id"]

    # mark read
    r2 = await client.patch(f"/api/v1/notifications/SV0001/{notif_id}/read")
    assert r2.status_code == 200

    # mark all read
    r3 = await client.patch("/api/v1/notifications/SV0001/read-all")
    assert r3.status_code == 200


@pytest.mark.asyncio
async def test_bulk_send(client):
    r = await client.post("/api/v1/notifications/bulk", json={
        "recipient_ids": ["SV0001", "SV0002", "SV0003"],
        "channel":       "in_app",
        "event_type":    "announcement",
        "title":         "Exam schedule released",
        "body":          "Check the portal for details.",
    })
    assert r.status_code == 200
    assert r.json()["sent"] == 3
