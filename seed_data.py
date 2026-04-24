"""
seed_data.py — Load 300 students from CSV into running services

Usage:
    pip install httpx pandas
    python seed_data.py

Requirements: docker-compose must be running
    docker-compose up -d
"""
import csv, httpx, time, sys
from pathlib import Path

STUDENT_URL  = "http://localhost:8000/api/v1"
SCORE_URL    = "http://localhost:8001/api/v1"
NOTIF_URL    = "http://localhost:8003/api/v1"
CSV_PATH     = Path("data/mock/student_score_dataset.csv")

OK   = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"
INFO = "\033[94m→\033[0m"

def to_float(x):
    try:
        return float(x)
    except:
        return 0.0

def to_int(x):
    try:
        return int(x)
    except:
        return 0

def check_services():
    print(f"\n{INFO} Checking services...")
    for name, url in [
        ("student-service",      f"{STUDENT_URL.replace('/api/v1','')}/api/v1/health"),
        ("score-service",        f"{SCORE_URL.replace('/api/v1','')}/api/v1/health"),
        ("notification-service", f"{NOTIF_URL.replace('/api/v1','')}/api/v1/health"),
    ]:
        try:
            r = httpx.get(url, timeout=3)
            print(f"  {OK} {name}")
        except Exception:
            print(f"  {FAIL} {name} — not reachable. Run: docker-compose up -d")
            sys.exit(1)


def load_csv():
    rows = []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    print(f"\n{INFO} Loaded {len(rows)} rows from {CSV_PATH}")
    return rows


def seed_students(rows: list[dict], client: httpx.Client):
    print(f"\n{INFO} Seeding students into student-service...")
    created = skipped = failed = 0

    for row in rows:
        sid = int(row["student_id"])
        payload = {
            "student_id":   sid,
            "name":         row["name"].strip(),
            "age":          int(row["age"]) if row["age"] else None,
            "gender":       row["gender"].strip() if row["gender"] else None,
            "previous_gpa": float(row["previous_gpa"]) if row["previous_gpa"] else None,
            "email":        f"student{sid}@soa.edu.vn",
            "password":     f"12345678",  # Mật khẩu mặc định cho tất cả sinh viên
            "role":         "student",
        }
        try:
            r = client.post(f"{STUDENT_URL}/students", json=payload, timeout=5)
            if r.status_code == 201:
                created += 1
            elif r.status_code == 409:
                skipped += 1
            else:
                failed += 1
                if failed <= 3:
                    print(f"    {FAIL} Score {sid}: {r.status_code}")
                    print(r.text)
        except Exception as e:
            failed += 1

    print(f"  {OK} Students — created: {created}  skipped: {skipped}  failed: {failed}")
    return created + skipped


def seed_scores(rows: list[dict], client: httpx.Client):
    print(f"\n{INFO} Seeding scores into score-service...")
    created = skipped = failed = 0

    for row in rows:
        sid = int(row["student_id"])
        payload = {
    "student_id": sid,
    "quiz1_marks": to_float(row["quiz1_marks"]),
    "quiz2_marks": to_float(row["quiz2_marks"]),
    "quiz3_marks": to_float(row["quiz3_marks"]),
    "midterm_marks": to_float(row["midterm_marks"]),
    "final_marks": to_float(row["final_marks"]),
    "previous_gpa": to_float(row["previous_gpa"]),
    "lectures_attended": to_int(row["lectures_attended"]),
    "labs_attended": to_int(row["labs_attended"]),
}
        try:
            r = client.post(f"{SCORE_URL}/scores", json=payload, timeout=5)
            if r.status_code == 201:
                created += 1
                # Send pass/fail notification
                data = r.json()
                notif_payload = {
                    "student_id":  str(sid),
                    "total_score": data.get("total_score", 0),
                    "grade":       data.get("grade", "?"),
                    "pass_fail":   data.get("pass_fail", "?"),
                }
                client.post(f"{NOTIF_URL}/notifications/events/score-posted",
                            json=notif_payload, timeout=3)
            elif r.status_code == 409:
                skipped += 1
            else:
                failed += 1
                if failed <= 3:
                    print(f"    {FAIL} Score {sid}: {r.status_code}")
                    print(r.text)
        except Exception as e:
            failed += 1

    print(f"  {OK} Scores  — created: {created}  skipped: {skipped}  failed: {failed}")


def print_summary(client: httpx.Client):
    print(f"\n{'='*50}")
    print("  SEED SUMMARY")
    print(f"{'='*50}")

    try:
        r = client.get(f"{SCORE_URL}/scores/summary", timeout=5)
        if r.status_code == 200:
            d = r.json()
            print(f"  Total students : {d.get('total_students', '?')}")
            print(f"  Pass           : {d.get('pass_count')} ({d.get('pass_rate_pct')}%)")
            print(f"  Fail           : {d.get('fail_count')} ({d.get('fail_rate_pct')}%)")
            print(f"  Avg score      : {d.get('avg_total_score')}/100")
            print(f"  Grade breakdown: {d.get('grade_breakdown')}")
    except Exception as e:
        print(f"  Could not fetch summary: {e}")

    print(f"\n  Swagger UI:")
    print(f"  Student  → http://localhost:8000/docs")
    print(f"  Score    → http://localhost:8001/docs")
    print(f"  Analytics→ http://localhost:8002/docs")
    print(f"  Notif    → http://localhost:8003/docs")
    print(f"  pgAdmin  → http://localhost:5050  (admin@soa.local / admin)")
    print(f"{'='*50}\n")


def main():
    print("=" * 50)
    print("  SOA Student Analytics — Seed Script")
    print("=" * 50)

    check_services()
    rows = load_csv()

    with httpx.Client() as client:
        seed_students(rows, client)
        seed_scores(rows, client)
        print_summary(client)

    print(f"{OK} Done! All data loaded.\n")


if __name__ == "__main__":
    main()
