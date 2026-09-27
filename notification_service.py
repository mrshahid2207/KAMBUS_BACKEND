"""Centralised persisted + optional Firebase notification delivery."""
import json
import logging
import os
from sqlalchemy.orm import Session
from models import Notification, DeviceToken

log = logging.getLogger(__name__)
_firebase_ready = False

def _firebase():
    global _firebase_ready
    if _firebase_ready:
        from firebase_admin import messaging
        return messaging
    credential_path = os.getenv("FCM_SERVICE_ACCOUNT_FILE")
    if not credential_path:
        log.warning(
            "FCM skipped: FCM_SERVICE_ACCOUNT_FILE is not configured; "
            "notification will be persisted but push delivery is unavailable."
        )
        return None
    try:
        import firebase_admin
        from firebase_admin import credentials, messaging
        if not firebase_admin._apps:
            firebase_admin.initialize_app(credentials.Certificate(credential_path))
        _firebase_ready = True
        return messaging
    except Exception:
        log.exception("FCM is not configured; notification was persisted only")
        return None

def send_notification(db: Session, user_id: int, title: str, message: str,
                      notification_type: str, data: dict | None = None,
                      related_bus_id: int | None = None, related_trip_id: int | None = None,
                      related_wait_request_id: int | None = None,
                      delivery_summary: dict | None = None) -> Notification:
    notification = Notification(user_id=user_id, title=title, message=message,
        type=notification_type, payload=json.dumps(data or {}), related_bus_id=related_bus_id,
        related_trip_id=related_trip_id, related_wait_request_id=related_wait_request_id)
    db.add(notification)
    db.flush()
    messaging = _firebase()
    if not messaging:
        if delivery_summary is not None:
            delivery_summary["fcm_available"] = False
            delivery_summary["skipped"] += 1
        return notification
    devices = db.query(DeviceToken).filter(
        DeviceToken.user_id == user_id,
        DeviceToken.is_active == 1,
    ).all()
    if not devices and delivery_summary is not None:
        delivery_summary["no_active_tokens"] += 1
    for device in devices:
        try:
            if delivery_summary is not None:
                delivery_summary["attempted"] += 1
            message_id = messaging.send(messaging.Message(
                notification=messaging.Notification(title=title, body=message),
                data={str(k): str(v) for k, v in (data or {}).items()},
                token=device.token,
            ))
            # Log the row ID, not the FCM token itself, because FCM tokens are credentials.
            log.info(
                "FCM send succeeded: user_id=%s device_token_id=%s message_id=%s",
                user_id,
                device.id,
                message_id,
            )
            if delivery_summary is not None:
                delivery_summary["succeeded"] += 1
        except Exception:
            # Invalid tokens must never break the business action that triggered a notification.
            log.exception("FCM send failed for device token %s", device.id)
            if delivery_summary is not None:
                delivery_summary["failed"] += 1
            device.is_active = 0
    return notification
