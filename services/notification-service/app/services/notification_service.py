"""
Notification Service – send emails (SMTP) or store in-app notifications.
Falls back to console logging if SMTP is not configured.
"""
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from fastapi import HTTPException

from app.models.notification import Notification, StatusEnum
from app.schemas.notification import NotificationCreate, ScorePostedEvent, AtRiskAlertEvent
from app.core.config import settings

logger = logging.getLogger("notification-service")


class NotificationService:

    # ── Internal send ─────────────────────────────────────────────────────────
    async def _send_email(self, to: str, subject: str, body: str) -> bool:
        """Send via SMTP. Returns True on success, False if SMTP not configured."""
        if not settings.SMTP_HOST or not settings.SMTP_USER:
            logger.info(f"[EMAIL-MOCK] To={to} | Subject={subject} | Body={body[:80]}...")
            return True   # mock success in dev

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"]    = settings.EMAIL_FROM
            msg["To"]      = to
            msg.attach(MIMEText(body, "html"))

            with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
                server.ehlo()
                server.starttls()
                server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
                server.sendmail(settings.EMAIL_FROM, to, msg.as_string())
            return True
        except Exception as e:
            logger.error(f"SMTP error sending to {to}: {e}")
            return False

    # ── Core CRUD ─────────────────────────────────────────────────────────────
    async def create_and_send(self, db: AsyncSession, data: NotificationCreate) -> Notification:
        notif = Notification(
            recipient_id    = data.recipient_id,
            recipient_email = data.recipient_email,
            channel         = data.channel,
            event_type      = data.event_type,
            title           = data.title,
            body            = data.body,
            status          = StatusEnum.pending,
        )
        db.add(notif)
        await db.flush()

        # Dispatch
        success = True
        if data.channel == "email" and data.recipient_email:
            success = await self._send_email(data.recipient_email, data.title, data.body)
        elif data.channel == "in_app":
            success = True  # stored in DB = delivered
        else:
            logger.info(f"[{data.channel.upper()}] {data.recipient_id}: {data.title}")

        notif.status  = StatusEnum.sent if success else StatusEnum.failed
        notif.sent_at = datetime.now(timezone.utc) if success else None
        await db.flush()
        await db.refresh(notif)
        return notif

    async def bulk_send(self, db: AsyncSession, recipient_ids: list[str],
                        emails: list[str], channel, event_type, title, body) -> dict:
        sent, failed = 0, 0
        for i, rid in enumerate(recipient_ids):
            email = emails[i] if emails and i < len(emails) else None
            from app.schemas.notification import NotificationCreate, ChannelEnum
            data = NotificationCreate(
                recipient_id=rid, recipient_email=email,
                channel=channel, event_type=event_type,
                title=title, body=body,
            )
            try:
                await self.create_and_send(db, data)
                sent += 1
            except Exception:
                failed += 1
        return {"sent": sent, "failed": failed}

    # ── Event helpers (called by other services) ──────────────────────────────
    async def notify_score_posted(self, db: AsyncSession, event: ScorePostedEvent) -> Notification:
        emoji = "🎉" if event.grade in ("A","B") else "📊"
        title = f"{emoji} Score posted – {event.subject}"
        body  = (
            f"<p>Dear <b>{event.student_id}</b>,</p>"
            f"<p>Your score for <b>{event.subject}</b> (Semester {event.semester}) has been recorded:</p>"
            f"<ul><li>GPA: <b>{event.gpa}</b></li><li>Grade: <b>{event.grade}</b></li></ul>"
            f"<p>Log in to view your full report.</p>"
        )
        from app.schemas.notification import NotificationCreate, ChannelEnum
        return await self.create_and_send(db, NotificationCreate(
            recipient_id    = event.student_id,
            recipient_email = event.student_email,
            channel         = ChannelEnum.email if event.student_email else ChannelEnum.in_app,
            event_type      = "score_posted",
            title=title, body=body,
        ))

    async def notify_at_risk(self, db: AsyncSession, event: AtRiskAlertEvent) -> dict:
        sent = 0
        for s in event.students:
            title = f"⚠️ Academic Alert – {event.semester}"
            body  = (
                f"<p>Student <b>{s['student_id']}</b> is flagged as <b>{s['risk_level']} RISK</b>.</p>"
                f"<ul>"
                f"<li>Average GPA: {s.get('avg_gpa','N/A')}</li>"
                f"<li>Failed subjects: {s.get('fail_count',0)}</li>"
                f"</ul>"
                f"<p>Please reach out for academic support.</p>"
            )
            from app.schemas.notification import NotificationCreate, ChannelEnum
            await self.create_and_send(db, NotificationCreate(
                recipient_id=s["student_id"],
                channel=ChannelEnum.in_app,
                event_type="at_risk_alert",
                title=title, body=body,
            ))
            sent += 1
        return {"at_risk_notifications_sent": sent}

    # ── Queries ───────────────────────────────────────────────────────────────
    async def list_for_recipient(self, db: AsyncSession, recipient_id: str,
                                  unread_only: bool = False) -> dict:
        q = select(Notification).where(Notification.recipient_id == recipient_id)
        if unread_only:
            q = q.where(Notification.is_read == False)
        q = q.order_by(Notification.created_at.desc())
        items = (await db.execute(q)).scalars().all()
        return {"total": len(items), "items": items}

    async def mark_read(self, db: AsyncSession, notif_id: int, recipient_id: str) -> dict:
        result = await db.execute(
            select(Notification).where(
                Notification.id == notif_id,
                Notification.recipient_id == recipient_id,
            )
        )
        notif = result.scalar_one_or_none()
        if not notif:
            raise HTTPException(404, "Notification not found")
        notif.is_read = True
        return {"message": "Marked as read"}

    async def mark_all_read(self, db: AsyncSession, recipient_id: str) -> dict:
        q = (
            update(Notification)
            .where(Notification.recipient_id == recipient_id, Notification.is_read == False)
            .values(is_read=True)
        )
        await db.execute(q)
        return {"message": "All notifications marked as read"}


notification_service = NotificationService()
