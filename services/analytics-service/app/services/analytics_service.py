"""
Analytics Service – pulls data from score-service and computes stats.
All heavy computation is done here so the API layer stays thin.
"""
import httpx
from collections import defaultdict
from fastapi import HTTPException
from app.core.config import settings


class AnalyticsService:

    async def _fetch_scores(self, student_id: str = None, subject: str = None, semester: str = "2024-1") -> list[dict]:
        """Fetch scores from score-service via HTTP."""
        if student_id:
            url = f"{settings.SCORE_SERVICE_URL}/api/v1/scores/student/{student_id}?semester={semester}"
        elif subject:
            url = f"{settings.SCORE_SERVICE_URL}/api/v1/scores/subject/{subject}?semester={semester}"
        else:
            # List all — score-service would need a /scores?semester= endpoint
            url = f"{settings.SCORE_SERVICE_URL}/api/v1/scores?semester={semester}&page_size=10000"

        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                payload = resp.json()
                return payload.get("items", payload) if isinstance(payload, dict) else payload
            except httpx.HTTPStatusError as e:
                raise HTTPException(status_code=e.response.status_code, detail=f"score-service error: {e}")
            except httpx.RequestError:
                raise HTTPException(status_code=503, detail="score-service unreachable")

    # ── Overview ──────────────────────────────────────────────────────────────
    async def get_overview(self, scores: list[dict]) -> dict:
        if not scores:
            return {}

        gpas = [s["gpa"] for s in scores if s.get("gpa") is not None]
        avg_gpa = round(sum(gpas) / len(gpas), 2) if gpas else 0
        pass_count = sum(1 for s in scores if s.get("grade") not in ("F", None))
        fail_count = len(scores) - pass_count
        excellent = sum(1 for s in scores if s.get("grade") == "A")

        # group by student for at-risk
        by_student = defaultdict(list)
        for s in scores:
            by_student[s["student_id"]].append(s)

        def _avg_gpa(ss: list) -> float:
            gpas = [s["gpa"] for s in ss if s.get("gpa") is not None]
            return sum(gpas) / len(gpas) if gpas else 0.0

        at_risk = sum(
            1 for sid, ss in by_student.items()
            if _avg_gpa(ss) < 5.0 or sum(1 for s in ss if s.get("grade") == "F") >= 2
        )

        # top major
        major_gpa = defaultdict(list)
        for s in scores:
            if s.get("gpa"):
                major_gpa[s.get("major","Unknown")].append(s["gpa"])
        top_major = max(major_gpa, key=lambda m: sum(major_gpa[m])/len(major_gpa[m])) if major_gpa else "N/A"

        # hardest subject (highest fail rate)
        subj_fail = defaultdict(lambda: [0, 0])  # [fail, total]
        for s in scores:
            subj = s.get("subject", "Unknown")
            subj_fail[subj][1] += 1
            if s.get("grade") == "F":
                subj_fail[subj][0] += 1
        hardest = max(subj_fail, key=lambda k: subj_fail[k][0] / max(subj_fail[k][1], 1)) if subj_fail else "N/A"

        return {
            "total_students": len(by_student),
            "total_scores":   len(scores),
            "avg_gpa":        avg_gpa,
            "pass_rate":      round(pass_count / len(scores) * 100, 1) if scores else 0,
            "excellent_rate": round(excellent  / len(scores) * 100, 1) if scores else 0,
            "at_risk_count":  at_risk,
            "top_major":      top_major,
            "hardest_subject": hardest,
        }

    # ── GPA Distribution ──────────────────────────────────────────────────────
    async def gpa_distribution(self, scores: list[dict]) -> list[dict]:
        counts = defaultdict(int)
        for s in scores:
            g = s.get("grade") or "N/A"
            counts[g] += 1
        total = len(scores) or 1
        return [
            {"grade": g, "count": c, "percentage": round(c / total * 100, 1)}
            for g, c in sorted(counts.items())
        ]

    # ── Major Analytics ───────────────────────────────────────────────────────
    async def major_stats(self, scores: list[dict]) -> list[dict]:
        by_major = defaultdict(list)
        for s in scores:
            by_major[s.get("major", "Unknown")].append(s)

        result = []
        for major, ss in by_major.items():
            gpas = [s["gpa"] for s in ss if s.get("gpa") is not None]
            fails = sum(1 for s in ss if s.get("grade") == "F")
            result.append({
                "major":         major,
                "student_count": len(set(s["student_id"] for s in ss)),
                "avg_gpa":       round(sum(gpas) / len(gpas), 2) if gpas else 0,
                "pass_rate":     round((len(ss) - fails) / len(ss) * 100, 1),
                "fail_rate":     round(fails / len(ss) * 100, 1),
            })
        return sorted(result, key=lambda x: x["avg_gpa"], reverse=True)

    # ── Subject Analytics ─────────────────────────────────────────────────────
    async def subject_stats(self, scores: list[dict]) -> list[dict]:
        by_subj = defaultdict(list)
        for s in scores:
            by_subj[s.get("subject", "Unknown")].append(s)

        result = []
        for subj, ss in by_subj.items():
            gpas    = [s["gpa"] for s in ss if s.get("gpa") is not None]
            midterms= [s["midterm_score"] for s in ss if s.get("midterm_score") is not None]
            finals  = [s["final_score"] for s in ss if s.get("final_score") is not None]
            fails   = sum(1 for s in ss if s.get("grade") == "F")
            fail_rate = round(fails / len(ss) * 100, 1)
            result.append({
                "subject":     subj,
                "enrollments": len(ss),
                "avg_gpa":     round(sum(gpas)     / len(gpas),     2) if gpas     else 0,
                "avg_midterm": round(sum(midterms)  / len(midterms), 2) if midterms else 0,
                "avg_final":   round(sum(finals)    / len(finals),   2) if finals   else 0,
                "fail_rate":   fail_rate,
                "difficulty":  "Hard" if fail_rate >= 30 else "Medium" if fail_rate >= 15 else "Easy",
            })
        return sorted(result, key=lambda x: x["fail_rate"], reverse=True)

    # ── At-Risk Students ──────────────────────────────────────────────────────
    async def at_risk_students(self, scores: list[dict]) -> list[dict]:
        by_student = defaultdict(list)
        for s in scores:
            by_student[s["student_id"]].append(s)

        result = []
        for sid, ss in by_student.items():
            gpas = [s["gpa"] for s in ss if s.get("gpa") is not None]
            avg  = sum(gpas) / len(gpas) if gpas else 0
            fails = sum(1 for s in ss if s.get("grade") == "F")
            atts  = [s["attendance_rate"] for s in ss if s.get("attendance_rate") is not None]
            avg_att = sum(atts) / len(atts) if atts else 1

            if avg < 5.0 or fails >= 2 or avg_att < 0.6:
                is_failing = avg < 5.0 or any(s.get("is_failing", False) for s in ss)
                result.append({
                    "student_id":    sid,
                    "avg_gpa":       round(avg, 2),
                    "fail_count":    fails,
                    "attendance_avg": round(avg_att, 2),
                    "is_failing":    is_failing,
                    "warning":       "Rớt môn" if is_failing else None,
                    "risk_level":    "HIGH" if (avg < 4.0 or fails >= 3) else "MEDIUM",
                })
        return sorted(result, key=lambda x: x["avg_gpa"])

    # ── Student Trend ─────────────────────────────────────────────────────────
    async def student_trend(self, student_id: str, semester: str) -> dict:
        scores = await self._fetch_scores(student_id=student_id, semester=semester)
        if not scores:
            raise HTTPException(404, f"No scores found for {student_id}")
        return {
            "student_id": student_id,
            "semester":   semester,
            "subjects":   [
                {
                    "subject":        s["subject"],
                    "midterm_score":  s["midterm_score"],
                    "final_score":    s["final_score"],
                    "gpa":            s["gpa"],
                    "grade":          s["grade"],
                    "attendance_rate":s["attendance_rate"],
                }
                for s in scores
            ],
            "summary": (await self.get_overview(scores)),
        }


analytics_service = AnalyticsService()
