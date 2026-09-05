from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, status
from sqlalchemy.orm import Session
from sqlalchemy import inspect, text, func
from datetime import datetime, timedelta, date
from math import asin, cos, radians, sin, sqrt, atan2
import json
import logging
import asyncio
import hashlib
import hmac
import urllib.request
import urllib.parse

from database import Base, engine, SessionLocal
from models import (
    User,
    Student,
    Driver,
    Route,
    Bus,
    Stop,
    BusLocation,
    WaitRequest,
    TravelStatus,
    Trip,
    Notification,
    DeviceToken,
    BusEntryLog,
    DriverComplaint,
    ComplaintVerification,
    AdminActivityLog,
    AnnouncementHistory,
    StudentOTP,
    MissedBusAllotment,
    TemporaryStopChange
)
from schemas import (
    LoginRequest,
    LoginResponse,
    RouteCreate,
    BusCreate,
    StopCreate,
    StudentCreate,
    LocationCreate,
    DriverCreate,
    AssignDriverRequest,
    DriverStopCreate,
    WaitRequestCreate,
    TravelStatusCreate,
    AdminBusCreate,
    AdminBusUpdate,
    AdminStudentBusUpdate,
    AdminStudentStopUpdate,
    AdminRouteCreate,
    AdminRouteUpdate,
    AdminStopCreate,
    AdminStopUpdate,
    AdminReorderStopsRequest,
    AdminAssignBusDriverRequest,
    AdminAssignBusRouteRequest,
    AdminCalculateRecipientsRequest,
    AdminBroadcastAnnouncementRequest,
    AdminDriverCreate,
    AdminDriverUpdate,
    AdminStudentCreate,
    AdminStudentUpdate,
    DeviceTokenCreate,
    DriverComplaintCreate,
    ComplaintVerificationCreate,
    VerifyPassRequest,
    DriverDetourCreate,
    DriverEmergencySosCreate,
    StudentSignupRequest,
    StudentVerifyOtpRequest,
    StudentResendOtpRequest,
    StudentSelectStopRequest,
    TemporaryStopChangeCreate,
    MissedBusAllotmentRequest
)
from email_service import generate_otp_code, send_student_verification_email
from notification_service import send_notification
from auth import (
    hash_password,
    verify_password,
    create_access_token,
    get_current_user,
    require_driver,
    require_admin,
    require_student
)

logger = logging.getLogger("kambus")

def to_utc_iso(dt):
    if dt is None:
        return None
    return dt.isoformat() + "Z"

ALLOWED_COMPLAINT_REASONS = [
    "driver_not_on_time",
    "foul_language",
    "over_speeding_rash_driving",
    "improper_behaviour",
    "other"
]

# Complaint corroboration thresholds — tune these based on pitch/demo feedback
COMPLAINT_MIN_VOTES = 2
COMPLAINT_MIN_YES_RATIO = 0.6  # 60% of respondents must vote yes

app = FastAPI(title="KAMBUS API")

Base.metadata.create_all(bind=engine)

# ============================================================
# WEBSOCKET REAL-TIME NOTIFICATION MANAGER
# ============================================================

class NotificationConnectionManager:
    def __init__(self):
        # user_id -> set of active WebSockets
        self.active_connections: dict[int, set[WebSocket]] = {}

    async def connect(self, user_id: int, websocket: WebSocket):
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = set()
        self.active_connections[user_id].add(websocket)

    def disconnect(self, user_id: int, websocket: WebSocket):
        if user_id in self.active_connections:
            self.active_connections[user_id].discard(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]

    async def send_personal_message(self, message: dict, user_id: int):
        if user_id not in self.active_connections:
            return False

        dead_sockets = set()
        for connection in list(self.active_connections[user_id]):
            try:
                await connection.send_json(message)
            except Exception as e:
                dead_sockets.add(connection)

        for dead in dead_sockets:
            self.active_connections[user_id].discard(dead)
        return True

    def push_notification_sync(self, user_id: int, message: dict):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self.send_personal_message(message, user_id))
            else:
                loop.run_until_complete(self.send_personal_message(message, user_id))
        except Exception as e:
            logger.warning(f"Realtime push error for user {user_id}: {e}")

notification_manager = NotificationConnectionManager()


@app.websocket("/ws/notifications")
async def websocket_notifications(
    websocket: WebSocket,
    token: str | None = None
):
    if not token:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    db = SessionLocal()
    try:
        user_payload = get_current_user(token=token, db=db)
        user_id = user_payload["user_id"]
    except Exception:
        db.close()
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    finally:
        db.close()

    await notification_manager.connect(user_id, websocket)

    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        notification_manager.disconnect(user_id, websocket)
    except Exception:
        notification_manager.disconnect(user_id, websocket)


@app.on_event("startup")
def initialize_trip_database():
    from database import Base, engine
    from models import (
        Trip,
        Notification,
        DeviceToken,
        DriverComplaint,
        ComplaintVerification,
        AdminActivityLog,
        AnnouncementHistory,
        StudentOTP,
        MissedBusAllotment,
        TemporaryStopChange,
        Stop
    )

    # 1. Ensure all core tables exist
    Trip.__table__.create(bind=engine, checkfirst=True)
    Notification.__table__.create(bind=engine, checkfirst=True)
    DeviceToken.__table__.create(bind=engine, checkfirst=True)
    DriverComplaint.__table__.create(bind=engine, checkfirst=True)
    ComplaintVerification.__table__.create(bind=engine, checkfirst=True)
    AdminActivityLog.__table__.create(bind=engine, checkfirst=True)
    AnnouncementHistory.__table__.create(bind=engine, checkfirst=True)
    StudentOTP.__table__.create(bind=engine, checkfirst=True)
    MissedBusAllotment.__table__.create(bind=engine, checkfirst=True)
    TemporaryStopChange.__table__.create(bind=engine, checkfirst=True)

    # 2. Non-destructive column additions
    inspector = inspect(engine)

    if inspector.has_table("bus_locations"):
        bus_loc_cols = {col["name"] for col in inspector.get_columns("bus_locations")}
        if "trip_id" not in bus_loc_cols:
            with engine.begin() as connection:
                try:
                    connection.execute(text("ALTER TABLE bus_locations ADD COLUMN trip_id INTEGER REFERENCES trips(id)"))
                except Exception:
                    pass

    if inspector.has_table("students"):
        student_cols = {col["name"] for col in inspector.get_columns("students")}
        if "stop_id" not in student_cols:
            with engine.begin() as connection:
                try:
                    connection.execute(text("ALTER TABLE students ADD COLUMN stop_id INTEGER REFERENCES stops(id)"))
                except Exception:
                    pass

    if inspector.has_table("users"):
        user_cols = {col["name"] for col in inspector.get_columns("users")}
        if "email" not in user_cols:
            with engine.begin() as connection:
                try:
                    connection.execute(text("ALTER TABLE users ADD COLUMN email VARCHAR(120)"))
                except Exception:
                    pass
        if "is_verified" not in user_cols:
            with engine.begin() as connection:
                try:
                    connection.execute(text("ALTER TABLE users ADD COLUMN is_verified INTEGER DEFAULT 1"))
                except Exception:
                    pass

    if inspector.has_table("stops"):
        stop_cols = {col["name"] for col in inspector.get_columns("stops")}
        with engine.begin() as connection:
            if "is_custom" not in stop_cols:
                try:
                    connection.execute(text("ALTER TABLE stops ADD COLUMN is_custom BOOLEAN NOT NULL DEFAULT FALSE"))
                except Exception:
                    pass
            if "is_active" not in stop_cols:
                try:
                    connection.execute(text("ALTER TABLE stops ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT TRUE"))
                except Exception:
                    pass
            if "created_by_student_id" not in stop_cols:
                try:
                    connection.execute(text("ALTER TABLE stops ADD COLUMN created_by_student_id INTEGER REFERENCES students(id)"))
                except Exception:
                    pass

    if inspector.has_table("temporary_stop_changes"):
        temp_cols = {col["name"] for col in inspector.get_columns("temporary_stop_changes")}
        with engine.begin() as connection:
            if "selected_latitude" not in temp_cols:
                try:
                    connection.execute(text("ALTER TABLE temporary_stop_changes ADD COLUMN selected_latitude FLOAT"))
                except Exception:
                    pass
            if "selected_longitude" not in temp_cols:
                try:
                    connection.execute(text("ALTER TABLE temporary_stop_changes ADD COLUMN selected_longitude FLOAT"))
                except Exception:
                    pass
            if "selected_address" not in temp_cols:
                try:
                    connection.execute(text("ALTER TABLE temporary_stop_changes ADD COLUMN selected_address VARCHAR(255)"))
                except Exception:
                    pass


WAIT_BUDGET_PER_TRIP = 10
WAIT_DRIVER_SKIP_WINDOW_SECONDS = 10
WAIT_MIN_ETA_MINUTES = 1
WAIT_MAX_ETA_MINUTES = 10
WAIT_RATE_LIMIT_SECONDS = 30
WAIT_WEEKLY_LIMIT = 3
WAIT_SKIP_COOLDOWN_MINUTES = 15


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    earth_radius_km = 6371.0
    d_lat = radians(lat2 - lat1)
    d_lon = radians(lon2 - lon1)
    a = (
        sin(d_lat / 2) ** 2
        + cos(radians(lat1)) * cos(radians(lat2)) * sin(d_lon / 2) ** 2
    )
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return earth_radius_km * c


def get_active_trip_for_bus(db: Session, bus_id: int):
    return (
        db.query(Trip)
        .filter(Trip.bus_id == bus_id, Trip.status == "active")
        .order_by(Trip.started_at.desc())
        .first()
    )


def get_latest_location(db: Session, bus_id: int, trip_id: int | None = None):
    query = db.query(BusLocation).filter(BusLocation.bus_id == bus_id)
    if trip_id is not None:
        query = query.filter(BusLocation.trip_id == trip_id)
    return query.order_by(BusLocation.timestamp.desc()).first()


def student_is_travelling_today(db: Session, student_id: int) -> bool:
    today = date.today()
    status = (
        db.query(TravelStatus)
        .filter(TravelStatus.student_id == student_id, TravelStatus.date == today)
        .order_by(TravelStatus.created_at.desc())
        .first()
    )
    if status is None:
        return True
    return status.status == "travelling"


def get_eta_minutes_to_stop(db: Session, bus_id: int, trip_id: int, stop: Stop):
    location = get_latest_location(db, bus_id, trip_id)
    if not location:
        return None

    distance_km = haversine_km(
        location.latitude,
        location.longitude,
        stop.latitude,
        stop.longitude
    )

    speed = location.speed
    if speed is None or speed <= 0:
        previous_location = (
            db.query(BusLocation)
            .filter(
                BusLocation.bus_id == bus_id,
                BusLocation.trip_id == trip_id,
                BusLocation.speed.isnot(None),
                BusLocation.speed > 0,
                BusLocation.timestamp <= location.timestamp
            )
            .order_by(BusLocation.timestamp.desc())
            .first()
        )
        if previous_location:
            speed = previous_location.speed

    if speed is None or speed <= 0:
        return None

    metres_per_minute = (speed * 1000) / 60
    minutes = (distance_km * 1000) / metres_per_minute
    return max(1, int(minutes + 0.999))


def has_passed_stop(db: Session, trip: Trip, stop: Stop) -> bool:
    location = get_latest_location(db, trip.bus_id, trip.id)
    if not location:
        return False

    # Query regular active stops along the route
    route_stops = (
        db.query(Stop)
        .filter(
            Stop.route_id == trip.route_id,
            Stop.is_active == True,
            Stop.is_custom == False
        )
        .order_by(Stop.stop_order.asc())
        .all()
    )
    if not route_stops:
        # Fallback to all stops if no active flag is populated
        route_stops = (
            db.query(Stop)
            .filter(Stop.route_id == trip.route_id)
            .order_by(Stop.stop_order.asc())
            .all()
        )
    if not route_stops:
        return False

    nearest_stop = None
    nearest_distance = float("inf")
    for route_stop in route_stops:
        distance = haversine_km(
            location.latitude,
            location.longitude,
            route_stop.latitude,
            route_stop.longitude
        )
        if distance < nearest_distance:
            nearest_distance = distance
            nearest_stop = route_stop

    if not nearest_stop:
        return False

    # If the target stop is a custom stop, find its nearest regular route stop as reference
    target_stop_order = stop.stop_order
    if getattr(stop, "is_custom", False):
        ref_stop = None
        ref_dist = float("inf")
        for route_stop in route_stops:
            dist = haversine_km(stop.latitude, stop.longitude, route_stop.latitude, route_stop.longitude)
            if dist < ref_dist:
                ref_dist = dist
                ref_stop = route_stop
        if ref_stop:
            target_stop_order = ref_stop.stop_order

    return nearest_stop.stop_order > target_stop_order


def active_student_request_exists(db: Session, student_id: int, trip_id: int) -> bool:
    request = (
        db.query(WaitRequest)
        .filter(
            WaitRequest.student_id == student_id,
            WaitRequest.trip_id == trip_id,
            WaitRequest.status.in_(["pending", "accepted"])
        )
        .first()
    )
    return request is not None


def student_rate_limited(db: Session, student_id: int) -> bool:
    cutoff = datetime.utcnow() - timedelta(seconds=WAIT_RATE_LIMIT_SECONDS)
    request = (
        db.query(WaitRequest)
        .filter(
            WaitRequest.student_id == student_id,
            WaitRequest.created_at >= cutoff
        )
        .first()
    )
    return request is not None


def weekly_wait_limit_reached(db: Session, student_id: int) -> bool:
    cutoff = datetime.utcnow() - timedelta(days=7)
    count = (
        db.query(WaitRequest)
        .filter(
            WaitRequest.student_id == student_id,
            WaitRequest.created_at >= cutoff
        )
        .count()
    )
    return count >= WAIT_WEEKLY_LIMIT


def active_skip_cooldown(db: Session, student_id: int, trip_id: int) -> bool:
    now = datetime.utcnow()
    request = (
        db.query(WaitRequest)
        .filter(
            WaitRequest.student_id == student_id,
            WaitRequest.trip_id == trip_id,
            WaitRequest.status == "rejected",
            WaitRequest.cooldown_until.isnot(None),
            WaitRequest.cooldown_until > now
        )
        .order_by(WaitRequest.created_at.desc())
        .first()
    )
    return request is not None


@app.get("/")
def root():
    return {"message": "KAMBUS Backend is running 🚍"}


@app.get("/health")
def health():
    try:
        with engine.connect():
            return {"status": "database connected"}
    except Exception as e:
        return {"status": "database connection failed", "error": str(e)}


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ============================================================
# COMPLAINT VERIFICATION (ANSWER POLL)
# ============================================================

@app.post("/student/driver-complaint/verify")
def verify_driver_complaint(
    data: ComplaintVerificationCreate,
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    student = (
        db.query(Student)
        .filter(Student.user_id == current_student["user_id"])
        .first()
    )

    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")

    if data.response not in ["yes", "no"]:
        raise HTTPException(status_code=400, detail="Response must be yes or no")

    complaint = (
        db.query(DriverComplaint)
        .filter(DriverComplaint.id == data.complaint_id)
        .first()
    )

    if not complaint:
        raise HTTPException(status_code=404, detail="Complaint not found")

    if complaint.student_id == student.id:
        raise HTTPException(status_code=400, detail="You cannot verify your own complaint")

    if student.bus_id != complaint.bus_id:
        raise HTTPException(status_code=403, detail="This complaint is not related to your bus")

    existing_verification = (
        db.query(ComplaintVerification)
        .filter(
            ComplaintVerification.complaint_id == complaint.id,
            ComplaintVerification.student_id == student.id
        )
        .first()
    )

    if existing_verification:
        raise HTTPException(status_code=400, detail="You have already responded to this complaint")

    verification = ComplaintVerification(
        complaint_id=complaint.id,
        student_id=student.id,
        response=data.response
    )

    db.add(verification)
    db.commit()
    db.refresh(verification)

    return {
        "message": "Complaint verification submitted successfully",
        "complaint_id": complaint.id,
        "response": verification.response
    }


# ============================================================
# NOTIFICATIONS API (WITH AUTO-HYDRATION OF DESCRIPTION)
# ============================================================

@app.post("/notifications/device-token")
def register_device_token(
    data: DeviceTokenCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    token = data.token.strip()
    if len(token) < 20 or len(token) > 512:
        raise HTTPException(status_code=400, detail="Invalid device token")
    device = db.query(DeviceToken).filter(DeviceToken.token == token).first()
    if device:
        device.user_id, device.platform, device.is_active = current_user["user_id"], data.platform[:30], 1
    else:
        device = DeviceToken(user_id=current_user["user_id"], token=token, platform=data.platform[:30])
        db.add(device)
    db.commit()
    return {"message": "Device registered", "device_id": device.id}


@app.get("/notifications")
def list_notifications(
    limit: int = 30,
    offset: int = 0,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    limit = max(1, min(limit, 100))
    offset = max(0, offset)
    query = db.query(Notification).filter(Notification.user_id == current_user["user_id"])
    notifications = query.order_by(Notification.created_at.desc()).offset(offset).limit(limit).all()

    result = []
    for n in notifications:
        payload_data = None
        if hasattr(n, "payload") and n.payload:
            if isinstance(n.payload, str):
                try:
                    payload_data = json.loads(n.payload)
                except Exception:
                    payload_data = {"raw": n.payload}
            elif isinstance(n.payload, dict):
                payload_data = n.payload

        # Auto-hydrate complaint details from DB if this is a complaint poll
        if n.type == "driver_complaint_poll":
            if not isinstance(payload_data, dict):
                payload_data = {}
            complaint_id = payload_data.get("complaint_id")
            if complaint_id:
                complaint = db.query(DriverComplaint).filter(DriverComplaint.id == complaint_id).first()
                if complaint:
                    payload_data["reason"] = complaint.reason
                    payload_data["description"] = complaint.description

        result.append({
            "id": n.id,
            "title": n.title,
            "message": n.message,
            "type": n.type,
            "payload": payload_data,
            "is_read": bool(n.is_read),
            "created_at": to_utc_iso(n.created_at)
        })

    return {
        "notifications": result,
        "unread_count": query.filter(Notification.is_read == 0).count()
    }


@app.patch("/notifications/{notification_id}/read")
def mark_notification_read(
    notification_id: int,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    notification = db.query(Notification).filter(
        Notification.id == notification_id,
        Notification.user_id == current_user["user_id"]
    ).first()
    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")
    notification.is_read = 1
    db.commit()
    return {"message": "Notification marked as read", "notification_id": notification.id}


@app.patch("/notifications/read-all")
def mark_all_notifications_read(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    db.query(Notification).filter(
        Notification.user_id == current_user["user_id"],
        Notification.is_read == 0
    ).update({Notification.is_read: 1})
    db.commit()
    return {"message": "Notifications marked as read"}


# ============================================================
# AUTHENTICATION
# ============================================================

@app.post("/auth/login", response_model=LoginResponse)
def login(data: LoginRequest, db: Session = Depends(get_db)):
    user = None

    if data.role == "student":
        student = db.query(Student).filter(Student.roll_number == data.identifier).first()
        if student:
            user = db.query(User).filter(User.id == student.user_id).first()

    elif data.role == "driver":
        driver = db.query(Driver).filter(Driver.driver_code == data.identifier).first()
        if driver:
            user = db.query(User).filter(User.id == driver.user_id).first()

    elif data.role == "admin":
        try:
            admin_id = int(data.identifier)
        except ValueError:
            raise HTTPException(status_code=401, detail="Invalid admin ID or password")

        user = db.query(User).filter(User.id == admin_id, User.role == "admin").first()

    else:
        raise HTTPException(status_code=400, detail="Invalid role")

    if not user:
        raise HTTPException(status_code=401, detail="Invalid ID or password")

    if not verify_password(data.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid ID or password")

    if user.role == "student" and getattr(user, "is_verified", 1) == 0:
        raise HTTPException(
            status_code=403,
            detail="Your email is not verified yet. Please verify your email with the OTP verification code."
        )

    token = create_access_token(user.id, user.role)

    return {
        "access_token": token,
        "token_type": "bearer",
        "role": user.role,
        "user_id": user.id
    }


def hash_otp(otp_code: str) -> str:
    return hashlib.sha256(otp_code.strip().encode("utf-8")).hexdigest()


def verify_otp_hash(entered_otp: str, stored_hash: str) -> bool:
    return hmac.compare_digest(hash_otp(entered_otp), stored_hash)


@app.post("/auth/student/signup")
def student_signup(data: StudentSignupRequest, db: Session = Depends(get_db)):
    email = data.email.strip().lower()
    phone = data.phone.strip()
    roll_number = data.roll_number.strip().upper()
    name = data.name.strip()

    if not name:
        raise HTTPException(status_code=400, detail="Full name is required")
    if not roll_number:
        raise HTTPException(status_code=400, detail="Student roll number is required")
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="A valid email address is required")
    if not phone or len(phone) < 10:
        raise HTTPException(status_code=400, detail="A valid phone number is required")
    if not data.password or len(data.password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters long")

    # Check if active verified student exists with this roll number, email, or phone
    existing_roll = db.query(Student).filter(Student.roll_number.ilike(roll_number)).first()
    if existing_roll:
        existing_user = db.query(User).filter(User.id == existing_roll.user_id).first()
        if existing_user and getattr(existing_user, "is_verified", 1) == 1:
            raise HTTPException(status_code=400, detail="This Student Roll Number is already registered and active. Please sign in.")
        elif existing_user:
            db.delete(existing_roll)
            db.delete(existing_user)
            db.commit()

    existing_email_user = db.query(User).filter(User.email == email).first()
    if existing_email_user:
        if getattr(existing_email_user, "is_verified", 1) == 1:
            raise HTTPException(status_code=400, detail="This email address is already registered. Please sign in.")
        else:
            db.query(Student).filter(Student.user_id == existing_email_user.id).delete()
            db.delete(existing_email_user)
            db.commit()

    existing_phone_user = db.query(User).filter(User.phone == phone).first()
    if existing_phone_user:
        if getattr(existing_phone_user, "is_verified", 1) == 1:
            raise HTTPException(status_code=400, detail="This phone number is already registered. Please sign in.")
        else:
            db.query(Student).filter(Student.user_id == existing_phone_user.id).delete()
            db.delete(existing_phone_user)
            db.commit()

    # Create unverified user and student
    user = User(
        name=name,
        email=email,
        phone=phone,
        password_hash=hash_password(data.password),
        role="student",
        is_verified=0
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    student = Student(
        user_id=user.id,
        roll_number=roll_number,
        department=data.department,
        bus_id=None,
        stop_id=None
    )
    db.add(student)
    db.commit()
    db.refresh(student)

    # Invalidate previous unused OTPs
    db.query(StudentOTP).filter(StudentOTP.email == email, StudentOTP.is_used == 0).update({"is_used": 1})
    db.commit()

    # Generate 6-digit OTP code and store hash
    otp_code = generate_otp_code(6)
    expires_at = datetime.utcnow() + timedelta(minutes=10)

    otp_record = StudentOTP(
        email=email,
        otp_code=hash_otp(otp_code),
        purpose="student_signup",
        user_id=user.id,
        expires_at=expires_at,
        is_used=0
    )
    db.add(otp_record)
    db.commit()

    # Dispatch email
    email_sent, email_error = send_student_verification_email(email, name, otp_code)
    if not email_sent:
        logger.error(f"Signup OTP email dispatch failed for user_id={user.id}: {email_error}")
        # NOTE: user/student/otp rows are intentionally kept (not rolled back) so the
        # student can retry via /auth/student/resend-otp once SMTP is fixed, without
        # having to re-submit the whole signup form.
        raise HTTPException(
            status_code=502,
            detail="Unable to send verification email. Please try again later."
        )

    return {
        "success": True,
        "message": "Verification code has been sent to your email address.",
        "email": email,
        "roll_number": roll_number,
        "expires_in_minutes": 10
    }


@app.post("/auth/student/verify-otp")
def verify_student_otp(data: StudentVerifyOtpRequest, db: Session = Depends(get_db)):
    email = data.email.strip().lower()
    otp_code = data.otp_code.strip()

    if not email or not otp_code:
        raise HTTPException(status_code=400, detail="Email and 6-digit OTP code are required")

    otp_record = (
        db.query(StudentOTP)
        .filter(
            StudentOTP.email == email,
            StudentOTP.is_used == 0
        )
        .order_by(StudentOTP.created_at.desc())
        .first()
    )

    if not otp_record:
        raise HTTPException(status_code=400, detail="No active verification code found for this email. Please request a new code.")

    if datetime.utcnow() > otp_record.expires_at:
        raise HTTPException(status_code=400, detail="Verification code has expired. Please request a new code.")

    if not verify_otp_hash(otp_code, otp_record.otp_code):
        raise HTTPException(status_code=400, detail="Invalid verification code. Please check the code and try again.")

    # Mark OTP as used
    otp_record.is_used = 1

    # Activate student user
    user = db.query(User).filter(User.email == email).first()
    if not user and otp_record.user_id:
        user = db.query(User).filter(User.id == otp_record.user_id).first()

    if not user:
        raise HTTPException(status_code=404, detail="Student account not found")

    user.is_verified = 1
    db.commit()

    student = db.query(Student).filter(Student.user_id == user.id).first()

    token = create_access_token(user.id, "student")

    return {
        "success": True,
        "message": "Email verified successfully.",
        "access_token": token,
        "token_type": "bearer",
        "role": "student",
        "user_id": user.id,
        "student_id": student.id if student else None,
        "roll_number": student.roll_number if student else None,
        "name": user.name,
        "needs_onboarding": (student.stop_id is None) if student else True,
        "assigned_bus_id": student.bus_id if student else None
    }


@app.post("/auth/student/resend-otp")
def resend_student_otp(data: StudentResendOtpRequest, db: Session = Depends(get_db)):
    email = data.email.strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email address is required")

    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(status_code=404, detail="No registered account found with this email.")

    if getattr(user, "is_verified", 1) == 1:
        raise HTTPException(status_code=400, detail="This account is already verified. Please sign in.")

    # Rate limiting: 45 seconds between resends
    recent_otp = (
        db.query(StudentOTP)
        .filter(StudentOTP.email == email)
        .order_by(StudentOTP.created_at.desc())
        .first()
    )
    if recent_otp and (datetime.utcnow() - recent_otp.created_at).total_seconds() < 45:
        seconds_left = int(45 - (datetime.utcnow() - recent_otp.created_at).total_seconds())
        raise HTTPException(status_code=429, detail=f"Please wait {seconds_left} seconds before requesting another code.")

    # Invalidate previous OTPs
    db.query(StudentOTP).filter(StudentOTP.email == email, StudentOTP.is_used == 0).update({"is_used": 1})
    db.commit()

    otp_code = generate_otp_code(6)
    expires_at = datetime.utcnow() + timedelta(minutes=10)

    new_otp = StudentOTP(
        email=email,
        otp_code=hash_otp(otp_code),
        purpose="student_signup",
        user_id=user.id,
        expires_at=expires_at,
        is_used=0
    )
    db.add(new_otp)
    db.commit()

    email_sent, email_error = send_student_verification_email(email, user.name, otp_code)
    if not email_sent:
        logger.error(f"Resend OTP email dispatch failed for user_id={user.id}: {email_error}")
        raise HTTPException(
            status_code=502,
            detail="Unable to send verification email. Please try again later."
        )

    return {
        "success": True,
        "message": "A new 6-digit verification code has been sent to your email.",
        "email": email,
        "expires_in_minutes": 10
    }


@app.get("/student/onboarding-status")
def get_student_onboarding_status(
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")

    user = db.query(User).filter(User.id == student.user_id).first()

    bus = db.query(Bus).filter(Bus.id == student.bus_id).first() if student.bus_id else None
    route = db.query(Route).filter(Route.id == bus.route_id).first() if (bus and bus.route_id) else None
    assigned_stop = db.query(Stop).filter(Stop.id == student.stop_id).first() if student.stop_id else None

    available_stops = []
    if bus and bus.route_id:
        stops = db.query(Stop).filter(Stop.route_id == bus.route_id).order_by(Stop.stop_order.asc()).all()
        available_stops = [
            {
                "stop_id": s.id,
                "name": s.name,
                "latitude": s.latitude,
                "longitude": s.longitude,
                "stop_order": s.stop_order
            }
            for s in stops
        ]

    return {
        "student_id": student.id,
        "name": user.name if user else "Student",
        "roll_number": student.roll_number,
        "bus_assigned": bus is not None,
        "bus_id": bus.id if bus else None,
        "bus_number": bus.bus_number if bus else None,
        "route_id": route.id if route else None,
        "route_name": route.name if route else None,
        "stop_id": assigned_stop.id if assigned_stop else None,
        "stop_name": assigned_stop.name if assigned_stop else None,
        "available_stops": available_stops,
        "is_completed": student.stop_id is not None
    }


@app.post("/student/select-stop")
def student_select_pickup_stop(
    data: StudentSelectStopRequest,
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")

    if not student.bus_id:
        raise HTTPException(
            status_code=400,
            detail="Your bus has not been assigned yet. Please contact the transport administrator."
        )

    bus = db.query(Bus).filter(Bus.id == student.bus_id).first()
    if not bus or not bus.route_id:
        raise HTTPException(
            status_code=400,
            detail="Your assigned bus does not have an active route. Please contact the transport administrator."
        )

    stop = db.query(Stop).filter(Stop.id == data.stop_id).first()
    if not stop:
        raise HTTPException(status_code=404, detail="Selected pickup stop not found")

    if stop.route_id != bus.route_id:
        raise HTTPException(
            status_code=400,
            detail="The selected stop does not belong to your assigned bus route."
        )

    student.stop_id = stop.id
    db.commit()

    return {
        "success": True,
        "message": "Pickup stop selected successfully",
        "stop_id": stop.id,
        "stop_name": stop.name,
        "bus_id": bus.id,
        "bus_number": bus.bus_number
    }


# ============================================================
# ROUTES & BUSES MANAGEMENT
# ============================================================

@app.post("/routes")
def create_route(data: RouteCreate, db: Session = Depends(get_db)):
    route = Route(name=data.name, description=data.description)
    db.add(route)
    db.commit()
    db.refresh(route)
    return {"message": "Route created successfully", "route_id": route.id, "name": route.name}


@app.post("/buses")
def create_bus(data: BusCreate, db: Session = Depends(get_db)):
    bus = Bus(
        bus_number=data.bus_number,
        route_id=data.route_id,
        driver_id=data.driver_id,
        registration_number=data.registration_number,
        status=data.status
    )
    db.add(bus)
    db.commit()
    db.refresh(bus)
    return {"message": "Bus created successfully", "bus_id": bus.id, "bus_number": bus.bus_number}


@app.post("/routes/{route_id}/stops")
def create_stop(route_id: int, data: StopCreate, db: Session = Depends(get_db)):
    route = db.query(Route).filter(Route.id == route_id).first()
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")

    stop = Stop(
        route_id=route_id,
        name=data.name,
        latitude=data.latitude,
        longitude=data.longitude,
        stop_order=data.stop_order
    )
    db.add(stop)
    db.commit()
    db.refresh(stop)
    return {"message": "Stop created successfully", "stop_id": stop.id, "route_id": route_id, "name": stop.name}


# ============================================================
# DRIVER COMPLAINT CREATION (WITH DESCRIPTION IN PAYLOAD & WS)
# ============================================================

@app.post("/student/driver-complaint")
def create_driver_complaint(
    data: DriverComplaintCreate,
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    student = (
        db.query(Student)
        .filter(Student.user_id == current_student["user_id"])
        .first()
    )

    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")

    if data.reason not in ALLOWED_COMPLAINT_REASONS:
        raise HTTPException(status_code=400, detail="Invalid complaint reason")

    if data.reason == "other" and (not data.description or not data.description.strip()):
        raise HTTPException(status_code=400, detail="Please provide a description for this complaint")

    if not student.bus_id:
        raise HTTPException(status_code=400, detail="You are not assigned to a bus")

    bus = db.query(Bus).filter(Bus.id == student.bus_id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="Assigned bus not found")

    if not bus.driver_id:
        raise HTTPException(status_code=400, detail="No driver is currently assigned to your bus")

    active_trip = (
        db.query(Trip)
        .filter(
            Trip.bus_id == bus.id,
            Trip.driver_id == bus.driver_id,
            Trip.status == "active"
        )
        .order_by(Trip.started_at.desc())
        .first()
    )

    complaint = DriverComplaint(
        student_id=student.id,
        driver_id=bus.driver_id,
        bus_id=bus.id,
        trip_id=active_trip.id if active_trip else None,
        reason=data.reason,
        description=data.description.strip() if data.description else None,
        status="pending"
    )

    db.add(complaint)
    db.commit()
    db.refresh(complaint)

    # Query OTHER students on the same bus (exclude complainant)
    other_students = (
        db.query(Student)
        .filter(
            Student.bus_id == bus.id,
            Student.id != student.id
        )
        .all()
    )

    # Use custom description in the message if reason is "other"
    reported_issue = (
        complaint.description.strip()
        if complaint.reason == "other" and complaint.description
        else complaint.reason
    )

    neutral_message = (
        f"A student has reported a problem with your bus driver: {reported_issue}. "
        "Have you faced the same problem?"
    )

    # Distribute DB notifications and trigger instant realtime push with description
    for other_student in other_students:
        payload = {
            "complaint_id": complaint.id,
            "bus_id": bus.id,
            "driver_id": bus.driver_id,
            "reason": complaint.reason,
            "description": complaint.description,
            "poll": True
        }

        # Save to DB (fallback)
        send_notification(
            db,
            other_student.user_id,
            "Driver Complaint Verification",
            neutral_message,
            "driver_complaint_poll",
            payload,
            related_bus_id=bus.id,
            related_trip_id=complaint.trip_id
        )

        # Immediate Real-time WebSocket Push
        try:
            latest_notif = (
                db.query(Notification)
                .filter(
                    Notification.user_id == other_student.user_id,
                    Notification.type == "driver_complaint_poll"
                )
                .order_by(Notification.id.desc())
                .first()
            )
            notif_id = latest_notif.id if latest_notif else None

            realtime_event = {
                "type": "driver_complaint_poll",
                "id": notif_id,
                "title": "Driver Complaint Verification",
                "message": neutral_message,
                "payload": payload,
                "reason": complaint.reason,
                "description": complaint.description,
                "complaint_id": complaint.id,
                "created_at": to_utc_iso(datetime.utcnow())
            }
            notification_manager.push_notification_sync(other_student.user_id, realtime_event)
        except Exception as ws_err:
            logger.warning(f"Realtime WebSocket push skipped for user {other_student.user_id}: {ws_err}")

    db.commit()

    return {
        "message": "Driver complaint submitted successfully",
        "complaint_id": complaint.id,
        "bus_id": complaint.bus_id,
        "driver_id": complaint.driver_id,
        "trip_id": complaint.trip_id,
        "reason": complaint.reason,
        "description": complaint.description,
        "status": complaint.status
    }


# ============================================================
# STUDENT PROFILE & MAP ENDPOINTS
# ============================================================

@app.post("/students")
def create_student(data: StudentCreate, db: Session = Depends(get_db)):
    existing_user = db.query(User).filter(User.phone == data.phone).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Phone number already registered")

    existing_student = db.query(Student).filter(Student.roll_number == data.roll_number).first()
    if existing_student:
        raise HTTPException(status_code=400, detail="Roll number already registered")

    if data.stop_id is not None:
        stop = db.query(Stop).filter(Stop.id == data.stop_id).first()
        bus = db.query(Bus).filter(Bus.id == data.bus_id).first() if data.bus_id is not None else None
        if not stop:
            raise HTTPException(status_code=404, detail="Assigned stop not found")
        if not bus or bus.route_id != stop.route_id:
            raise HTTPException(status_code=400, detail="Assigned stop must belong to the student's bus route")

    user = User(
        name=data.name,
        phone=data.phone,
        password_hash=hash_password(data.password),
        role="student"
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    student = Student(
        user_id=user.id,
        roll_number=data.roll_number,
        department=data.department,
        bus_id=data.bus_id,
        stop_id=data.stop_id
    )
    db.add(student)
    db.commit()
    db.refresh(student)

    return {
        "message": "Student created successfully",
        "user_id": user.id,
        "student_id": student.id,
        "name": user.name,
        "roll_number": student.roll_number
    }


@app.post("/buses/{bus_id}/location")
def update_bus_location(
    bus_id: int,
    data: LocationCreate,
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver)
):
    COLLEGE_LAT = 18.0542435
    COLLEGE_LNG = 79.5351399
    GEOFENCE_RADIUS_METRES =50   

    driver = db.query(Driver).filter(Driver.user_id == current_driver["user_id"]).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")

    bus = db.query(Bus).filter(Bus.id == bus_id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="Bus not found")

    if bus.driver_id != driver.id:
        raise HTTPException(status_code=403, detail="You are not assigned to this bus")

    active_trip = (
        db.query(Trip)
        .filter(
            Trip.driver_id == driver.id,
            Trip.bus_id == bus.id,
            Trip.status == "active"
        )
        .order_by(Trip.started_at.desc())
        .first()
    )

    if not active_trip:
        raise HTTPException(status_code=409, detail="Start a trip before publishing location")

    location = BusLocation(
        bus_id=bus.id,
        trip_id=active_trip.id,
        latitude=data.latitude,
        longitude=data.longitude,
        speed=data.speed
    )
    db.add(location)
    db.commit()
    db.refresh(location)

    lat_delta = radians(COLLEGE_LAT - location.latitude)
    lng_delta = radians(COLLEGE_LNG - location.longitude)
    a = sin(lat_delta / 2) ** 2 + cos(radians(location.latitude)) * cos(radians(COLLEGE_LAT)) * sin(lng_delta / 2) ** 2
    distance_from_college = 6371000 * 2 * asin(sqrt(a))

    if distance_from_college <= GEOFENCE_RADIUS_METRES:
        existing_entry = db.query(BusEntryLog).filter(
            BusEntryLog.bus_id == bus.id,
            BusEntryLog.trip_id == active_trip.id
        ).first()

        if not existing_entry:
            entry_log = BusEntryLog(
                bus_id=bus.id,
                trip_id=active_trip.id,
                entry_time=location.timestamp,
                latitude=location.latitude,
                longitude=location.longitude
            )
            db.add(entry_log)
            db.commit()
            db.refresh(entry_log)

    for student in db.query(Student).filter(Student.bus_id == bus.id, Student.stop_id.isnot(None)).all():
        stop = db.query(Stop).filter(Stop.id == student.stop_id).first()
        if not stop:
            continue

        lat_delta = radians(stop.latitude - location.latitude)
        lng_delta = radians(stop.longitude - location.longitude)
        a = sin(lat_delta / 2) ** 2 + cos(radians(location.latitude)) * cos(radians(stop.latitude)) * sin(lng_delta / 2) ** 2
        metres = 6371000 * 2 * asin(sqrt(a))

        already_notified = db.query(Notification).filter(
            Notification.user_id == student.user_id,
            Notification.type == "bus_approaching",
            Notification.related_trip_id == active_trip.id,
            Notification.message.like(f"%{stop.name}%")
        ).first()

        if metres <= 150 and not already_notified:
            send_notification(
                db,
                student.user_id,
                "Bus Approaching",
                f"{bus.bus_number} is approaching your stop, {stop.name}.",
                "bus_approaching",
                {"stop_id": stop.id, "trip_id": active_trip.id},
                related_bus_id=bus.id,
                related_trip_id=active_trip.id
            )

    db.commit()

    return {
        "message": "Bus location updated",
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "driver_code": driver.driver_code,
        "latitude": location.latitude,
        "longitude": location.longitude,
        "speed": location.speed,
        "location_id": location.id,
        "geo_fence": {
            "inside": distance_from_college <= GEOFENCE_RADIUS_METRES,
            "distance_metres": round(distance_from_college, 2),
            "radius_metres": GEOFENCE_RADIUS_METRES
        }
    }


@app.get("/buses/{bus_id}/location")
def get_bus_location(
    bus_id: int,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] == "student":
        student = db.query(Student).filter(Student.user_id == current_user["user_id"]).first()
        if not student or student.bus_id != bus_id:
            raise HTTPException(status_code=403, detail="You are not assigned to this bus")

    elif current_user["role"] == "driver":
        driver = db.query(Driver).filter(Driver.user_id == current_user["user_id"]).first()
        if not driver or not db.query(Bus).filter(Bus.id == bus_id, Bus.driver_id == driver.id).first():
            raise HTTPException(status_code=403, detail="You are not assigned to this bus")

    elif current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Invalid role")

    location = db.query(BusLocation).filter(BusLocation.bus_id == bus_id).order_by(BusLocation.timestamp.desc()).first()
    if not location:
        raise HTTPException(status_code=404, detail="No location found for this bus")

    active_wait = (
        db.query(WaitRequest)
        .filter(
            WaitRequest.trip_id == location.trip_id,
            WaitRequest.bus_id == bus_id,
            WaitRequest.status == "accepted",
            WaitRequest.wait_until.isnot(None),
            WaitRequest.wait_until > datetime.utcnow()
        )
        .order_by(WaitRequest.wait_until.desc())
        .first()
        if location.trip_id
        else None
    )

    wait_remaining_seconds = None
    wait_stop_id = None
    wait_minutes = None

    if active_wait:
        wait_remaining_seconds = max(0, int((active_wait.wait_until - datetime.utcnow()).total_seconds()))
        wait_stop_id = active_wait.stop_id
        wait_minutes = active_wait.minutes

    return {
        "bus_id": bus_id,
        "latitude": location.latitude,
        "longitude": location.longitude,
        "speed": location.speed,
        "timestamp": to_utc_iso(location.timestamp),
        "trip_id": location.trip_id,
        "is_waiting": active_wait is not None,
        "wait_remaining_seconds": wait_remaining_seconds,
        "wait_stop_id": wait_stop_id,
        "wait_minutes": wait_minutes
    }


@app.post("/drivers")
def create_driver(data: DriverCreate, db: Session = Depends(get_db)):
    existing_driver = db.query(Driver).filter(Driver.driver_code == data.driver_code).first()
    if existing_driver:
        raise HTTPException(status_code=400, detail="Driver ID already registered")

    user = User(
        name=data.name,
        phone=data.phone,
        password_hash=hash_password(data.password),
        role="driver"
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    driver = Driver(
        user_id=user.id,
        driver_code=data.driver_code,
        license_number=data.license_number
    )
    db.add(driver)
    db.commit()
    db.refresh(driver)

    return {
        "message": "Driver created successfully",
        "driver_id": driver.id,
        "driver_code": driver.driver_code,
        "user_id": user.id,
        "name": user.name,
        "license_number": driver.license_number
    }


@app.post("/buses/{bus_id}/assign-driver")
def assign_driver(bus_id: int, data: AssignDriverRequest, db: Session = Depends(get_db)):
    bus = db.query(Bus).filter(Bus.id == bus_id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="Bus not found")

    driver = db.query(Driver).filter(Driver.driver_code == data.driver_code).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    existing_bus = db.query(Bus).filter(Bus.driver_id == driver.id, Bus.id != bus_id).first()
    if existing_bus:
        raise HTTPException(status_code=400, detail="Driver already assigned to another bus")

    bus.driver_id = driver.id
    db.commit()
    db.refresh(bus)

    return {
        "message": "Driver assigned successfully",
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "driver_id": driver.id,
        "driver_code": driver.driver_code
    }


@app.post("/driver/add-stop")
def add_driver_stop(
    data: DriverStopCreate,
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver)
):
    user_id = current_driver["user_id"]
    driver = db.query(Driver).filter(Driver.user_id == user_id).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")

    bus = db.query(Bus).filter(Bus.driver_id == driver.id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="No bus assigned to this driver")

    if bus.route_id is None:
        raise HTTPException(status_code=400, detail="Bus is not assigned to a route")

    last_stop = (
        db.query(Stop)
        .filter(Stop.route_id == bus.route_id)
        .order_by(Stop.stop_order.desc())
        .first()
    )
    next_stop_order = last_stop.stop_order + 1 if last_stop else 1

    stop = Stop(
        route_id=bus.route_id,
        name=f"Stop {next_stop_order}",
        latitude=data.latitude,
        longitude=data.longitude,
        stop_order=next_stop_order
    )
    db.add(stop)
    db.commit()
    db.refresh(stop)

    return {
        "message": "Stop added successfully",
        "stop_id": stop.id,
        "stop_name": stop.name,
        "stop_order": stop.stop_order,
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "route_id": bus.route_id,
        "latitude": stop.latitude,
        "longitude": stop.longitude
    }


@app.get("/driver/my-bus")
def get_my_bus(
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver)
):
    driver = db.query(Driver).filter(Driver.user_id == current_driver["user_id"]).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")

    user = db.query(User).filter(User.id == driver.user_id).first()
    bus = db.query(Bus).filter(Bus.driver_id == driver.id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="No bus assigned to this driver")

    route = db.query(Route).filter(Route.id == bus.route_id).first()

    return {
        "driver_id": driver.id,
        "driver_code": driver.driver_code,
        "driver_name": user.name if user else None,
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "route_id": bus.route_id,
        "route_name": route.name if route else None,
        "route_description": route.description if route else None
    }


@app.get("/student/my-route-stops")
def get_student_route_stops(
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")
    if student.bus_id is None:
        raise HTTPException(status_code=404, detail="No bus assigned to this student")

    bus = db.query(Bus).filter(Bus.id == student.bus_id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="Assigned bus not found")
    if bus.route_id is None:
        raise HTTPException(status_code=404, detail="No route assigned to this bus")

    stops = (
        db.query(Stop)
        .filter(Stop.route_id == bus.route_id, Stop.is_active == True)
        .order_by(Stop.stop_order.asc())
        .all()
    )

    return {
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "route_id": bus.route_id,
        "stops": [
            {
                "stop_id": stop.id,
                "name": stop.name,
                "latitude": stop.latitude,
                "longitude": stop.longitude,
                "stop_order": stop.stop_order
            }
            for stop in stops
        ]
    }


# ============================================================
# AUTOMATIC STUDENT BUS / STOP FEATURES & HELPERS
# ============================================================

MAX_TEMP_STOP_DISTANCE_KM = 1.5


def reverse_geocode(latitude: float, longitude: float) -> str | None:
    """Best-effort reverse geocoding with standard library fallback; never blocks a valid request."""
    url = f"https://nominatim.openstreetmap.org/reverse?lat={latitude}&lon={longitude}&format=json"
    headers = {"User-Agent": "KAMBUS-App/1.0 (campus-transport)"}
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=2.0) as response:
            if response.status == 200:
                data = json.loads(response.read().decode("utf-8"))
                return data.get("display_name")
    except Exception:
        pass
    return None


def is_point_on_route(
    db: Session,
    route_id: int,
    latitude: float,
    longitude: float,
) -> bool:
    """Validate that a selected map point is close enough to an active route stop."""
    route_stops = (
        db.query(Stop)
        .filter(Stop.route_id == route_id, Stop.is_active == True, Stop.is_custom == False)
        .all()
    )
    if not route_stops:
        route_stops = (
            db.query(Stop)
            .filter(Stop.route_id == route_id)
            .all()
        )

    for stop in route_stops:
        if stop.latitude is None or stop.longitude is None:
            continue
        if haversine_km(latitude, longitude, stop.latitude, stop.longitude) <= MAX_TEMP_STOP_DISTANCE_KM:
            return True
    return False


def get_or_create_custom_stop(
    db: Session,
    route_id: int,
    latitude: float,
    longitude: float,
    address: str | None,
    student_id: int,
) -> Stop:
    """Create a one-off custom Stop for this student's temporary selection."""
    name = address or reverse_geocode(latitude, longitude) or "Custom Stop"

    max_stop_order = (
        db.query(func.max(Stop.stop_order))
        .filter(Stop.route_id == route_id)
        .scalar()
    )
    next_stop_order = int(max_stop_order or 0) + 1

    stop = Stop(
        route_id=route_id,
        name=name[:100],
        latitude=latitude,
        longitude=longitude,
        stop_order=next_stop_order,
        is_custom=True,
        is_active=True,
        created_by_student_id=student_id,
    )
    db.add(stop)
    db.flush()
    return stop


def _deactivate_custom_stop(db: Session, change: TemporaryStopChange) -> None:
    """Soft-retire the custom Stop tied to an expired/cancelled change."""
    if change and change.temporary_stop_id:
        stop = db.query(Stop).filter(Stop.id == change.temporary_stop_id).first()
        if stop and getattr(stop, "is_custom", False) and getattr(stop, "is_active", True):
            stop.is_active = False


def expire_temporary_stop_changes(db: Session, student_id: int | None = None):
    today = date.today()
    query = db.query(TemporaryStopChange).filter(
        TemporaryStopChange.end_date < today,
        TemporaryStopChange.status.in_(["scheduled", "active"]),
    )
    if student_id is not None:
        query = query.filter(TemporaryStopChange.student_id == student_id)

    changes = query.all()
    for change in changes:
        change.status = "expired"
        _deactivate_custom_stop(db, change)

    if changes:
        db.commit()


def get_active_temporary_stop_change(db: Session, student_id: int, on_date: date | None = None):
    effective_date = on_date or date.today()
    change = (
        db.query(TemporaryStopChange)
        .filter(
            TemporaryStopChange.student_id == student_id,
            TemporaryStopChange.status.in_(["scheduled", "active"]),
            TemporaryStopChange.start_date <= effective_date,
            TemporaryStopChange.end_date >= effective_date,
        )
        .order_by(TemporaryStopChange.created_at.desc())
        .first()
    )
    return change


def get_effective_student_stop(db: Session, student: Student):
    """Return the currently effective stop, preferring an active temporary stop."""
    expire_temporary_stop_changes(db, student.id)
    temp_change = get_active_temporary_stop_change(db, student.id)
    if temp_change:
        temp_stop = db.query(Stop).filter(Stop.id == temp_change.temporary_stop_id).first()
        if temp_stop and getattr(temp_stop, "is_active", True):
            return temp_stop, temp_change

    original_stop = (
        db.query(Stop).filter(Stop.id == student.stop_id).first()
        if student.stop_id
        else None
    )
    return original_stop, None


def get_active_missed_bus_allotment(db: Session, student_id: int):
    allotment = (
        db.query(MissedBusAllotment)
        .filter(
            MissedBusAllotment.student_id == student_id,
            MissedBusAllotment.status == "active",
        )
        .order_by(MissedBusAllotment.created_at.desc())
        .first()
    )
    if not allotment:
        return None

    trip = (
        db.query(Trip)
        .filter(Trip.id == allotment.alternative_trip_id)
        .first()
        if allotment.alternative_trip_id
        else None
    )
    if trip and trip.status == "active" and (
        allotment.expires_at is None or allotment.expires_at > datetime.utcnow()
    ):
        return allotment

    allotment.status = "completed"
    db.commit()
    return None


def choose_alternative_bus_for_student(
    db: Session,
    student: Student,
    stop: Stop,
    original_bus: Bus,
    current_latitude: float | None = None,
    current_longitude: float | None = None,
):
    if original_bus.route_id is None:
        return None, None, None

    active_trips = (
        db.query(Trip)
        .join(Bus, Bus.id == Trip.bus_id)
        .filter(
            Trip.status == "active",
            Trip.route_id == original_bus.route_id,
            Trip.bus_id != original_bus.id,
            Bus.status == "active",
        )
        .order_by(Trip.started_at.asc())
        .all()
    )

    candidates = []
    for trip in active_trips:
        bus = db.query(Bus).filter(Bus.id == trip.bus_id).first()
        if not bus:
            continue
        if has_passed_stop(db, trip, stop):
            continue

        location = get_latest_location(db, bus.id, trip.id)
        if not location:
            continue

        eta = get_eta_minutes_to_stop(db, bus.id, trip.id, stop)
        if eta is None:
            continue

        user_distance = None
        if current_latitude is not None and current_longitude is not None:
            user_distance = haversine_km(
                current_latitude,
                current_longitude,
                location.latitude,
                location.longitude,
            )

        score = (
            eta,
            user_distance if user_distance is not None else 0.0,
            trip.started_at.timestamp(),
        )
        candidates.append((score, bus, trip, eta, location))

    if not candidates:
        return None, None, None

    candidates.sort(key=lambda item: item[0])
    _, selected_bus, selected_trip, selected_eta, _ = candidates[0]
    return selected_bus, selected_trip, selected_eta


@app.get("/student/my-stop")
def get_student_my_stop(
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")
    if student.bus_id is None:
        raise HTTPException(status_code=404, detail="No bus assigned to this student")
    if student.stop_id is None:
        raise HTTPException(status_code=404, detail="No stop assigned to this student")

    allotment = get_active_missed_bus_allotment(db, student.id)
    target_bus_id = allotment.alternative_bus_id if allotment else student.bus_id
    bus = db.query(Bus).filter(Bus.id == target_bus_id).first()
    stop, temp_change = get_effective_student_stop(db, student)
    if not bus or not stop:
        raise HTTPException(status_code=404, detail="Assigned stop or bus not found")

    return {
        "stop_id": stop.id,
        "stop_name": stop.name,
        "latitude": stop.latitude,
        "longitude": stop.longitude,
        "stop_order": stop.stop_order,
        "route_id": stop.route_id,
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "is_temporary": temp_change is not None,
        "is_custom_point": bool(getattr(stop, "is_custom", False)) if temp_change is not None else False,
        "temporary_change_id": temp_change.id if temp_change else None,
        "temporary_start_date": temp_change.start_date if temp_change else None,
        "temporary_end_date": temp_change.end_date if temp_change else None,
    }


# ============================================================
# MISSED BUS ALLOTMENT ENDPOINTS
# ============================================================

@app.post("/student/missed-bus/allot")
def automatically_allot_alternative_bus(
    data: MissedBusAllotmentRequest,
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student),
):
    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")
    if not student.bus_id:
        raise HTTPException(status_code=400, detail="No regular bus is assigned to this student")
    if not student.stop_id:
        raise HTTPException(status_code=400, detail="No pickup stop is assigned to this student")
    if not student_is_travelling_today(db, student.id):
        raise HTTPException(status_code=400, detail="You marked yourself as not travelling today")

    active_allotment = get_active_missed_bus_allotment(db, student.id)
    if active_allotment:
        bus = db.query(Bus).filter(Bus.id == active_allotment.alternative_bus_id).first()
        trip = (
            db.query(Trip)
            .filter(Trip.id == active_allotment.alternative_trip_id)
            .first()
            if active_allotment.alternative_trip_id
            else None
        )
        return {
            "success": True,
            "message": "You already have an active alternative bus for this journey",
            "allotment_id": active_allotment.id,
            "original_bus_id": active_allotment.original_bus_id,
            "alternative_bus_id": bus.id if bus else None,
            "alternative_bus_number": bus.bus_number if bus else None,
            "alternative_trip_id": trip.id if trip else None,
            "stop_id": active_allotment.stop_id,
            "status": active_allotment.status,
        }

    original_bus = db.query(Bus).filter(Bus.id == student.bus_id).first()
    stop, _ = get_effective_student_stop(db, student)
    if not original_bus or not stop:
        raise HTTPException(status_code=404, detail="Current bus or stop could not be found")

    original_trip = get_active_trip_for_bus(db, original_bus.id)
    alternative_bus, alternative_trip, eta = choose_alternative_bus_for_student(
        db,
        student,
        stop,
        original_bus,
        data.latitude,
        data.longitude,
    )
    if not alternative_bus or not alternative_trip:
        raise HTTPException(
            status_code=409,
            detail="No suitable alternative bus is currently available for your stop",
        )

    allotment = MissedBusAllotment(
        student_id=student.id,
        original_bus_id=original_bus.id,
        alternative_bus_id=alternative_bus.id,
        original_trip_id=original_trip.id if original_trip else None,
        alternative_trip_id=alternative_trip.id,
        stop_id=stop.id,
        status="active",
        created_at=datetime.utcnow(),
        expires_at=(alternative_trip.started_at + timedelta(hours=6)) if alternative_trip.started_at else None,
    )
    db.add(allotment)
    db.commit()
    db.refresh(allotment)

    send_notification(
        db,
        student.user_id,
        "Alternative Bus Allotted",
        f"{alternative_bus.bus_number} has been automatically allotted because you missed {original_bus.bus_number}.",
        "alternative_bus_allotted",
        {
            "allotment_id": allotment.id,
            "original_bus_id": original_bus.id,
            "alternative_bus_id": alternative_bus.id,
            "alternative_trip_id": alternative_trip.id,
            "stop_id": stop.id,
            "eta_minutes": eta,
        },
        related_bus_id=alternative_bus.id,
        related_trip_id=alternative_trip.id,
    )
    db.commit()

    return {
        "success": True,
        "message": "Alternative bus allotted automatically",
        "allotment_id": allotment.id,
        "original_bus_id": original_bus.id,
        "original_bus_number": original_bus.bus_number,
        "alternative_bus_id": alternative_bus.id,
        "alternative_bus_number": alternative_bus.bus_number,
        "alternative_trip_id": alternative_trip.id,
        "stop_id": stop.id,
        "stop_name": stop.name,
        "eta_minutes": eta,
        "status": allotment.status,
    }


@app.get("/student/missed-bus/allotment")
def get_alternative_bus_allotment(
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student),
):
    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")

    allotment = get_active_missed_bus_allotment(db, student.id)
    if not allotment:
        return {"active": False}

    bus = db.query(Bus).filter(Bus.id == allotment.alternative_bus_id).first()
    trip = (
        db.query(Trip).filter(Trip.id == allotment.alternative_trip_id).first()
        if allotment.alternative_trip_id
        else None
    )
    stop = db.query(Stop).filter(Stop.id == allotment.stop_id).first()

    return {
        "active": True,
        "allotment_id": allotment.id,
        "original_bus_id": allotment.original_bus_id,
        "alternative_bus_id": allotment.alternative_bus_id,
        "alternative_bus_number": bus.bus_number if bus else None,
        "alternative_trip_id": allotment.alternative_trip_id,
        "stop_id": stop.id if stop else None,
        "stop_name": stop.name if stop else None,
        "status": allotment.status,
        "created_at": to_utc_iso(allotment.created_at),
        "expires_at": to_utc_iso(allotment.expires_at),
        "trip_active": bool(trip and trip.status == "active"),
    }


# ============================================================
# MAP-BASED TEMPORARY STOP ENDPOINTS
# ============================================================

@app.post("/student/temporary-stop-change")
def create_temporary_stop_change(
    data: TemporaryStopChangeCreate,
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student),
):
    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")
    if not student.bus_id or not student.stop_id:
        raise HTTPException(
            status_code=400,
            detail="A regular bus and stop must be assigned before using temporary stop change",
        )
    if data.start_date > data.end_date:
        raise HTTPException(status_code=400, detail="Start date cannot be after end date")
    if (data.end_date - data.start_date).days > 30:
        raise HTTPException(status_code=400, detail="Temporary stop changes can be scheduled for at most 31 days")

    original_bus = db.query(Bus).filter(Bus.id == student.bus_id).first()
    original_stop = db.query(Stop).filter(Stop.id == student.stop_id).first()
    if not original_bus or not original_stop:
        raise HTTPException(status_code=404, detail="Bus or stop not found")
    if original_bus.route_id is None:
        raise HTTPException(status_code=400, detail="Your assigned bus has no route")
    if original_stop.route_id != original_bus.route_id:
        raise HTTPException(status_code=409, detail="Your assigned stop does not belong to your bus route")

    if not is_point_on_route(db, original_bus.route_id, data.latitude, data.longitude):
        raise HTTPException(
            status_code=400,
            detail=f"Selected location is too far from your route (must be within {MAX_TEMP_STOP_DISTANCE_KM} km of a stop on it)",
        )

    overlapping = (
        db.query(TemporaryStopChange)
        .filter(
            TemporaryStopChange.student_id == student.id,
            TemporaryStopChange.status.in_(["scheduled", "active"]),
            TemporaryStopChange.start_date <= data.end_date,
            TemporaryStopChange.end_date >= data.start_date,
        )
        .first()
    )
    if overlapping:
        raise HTTPException(status_code=409, detail="You already have an overlapping temporary stop change")

    temporary_stop = get_or_create_custom_stop(
        db,
        route_id=original_bus.route_id,
        latitude=data.latitude,
        longitude=data.longitude,
        address=data.address,
        student_id=student.id,
    )

    status_value = "active" if data.start_date <= date.today() <= data.end_date else "scheduled"
    change = TemporaryStopChange(
        student_id=student.id,
        original_stop_id=original_stop.id,
        temporary_stop_id=temporary_stop.id,
        start_date=data.start_date,
        end_date=data.end_date,
        status=status_value,
        created_at=datetime.utcnow(),
        selected_latitude=data.latitude,
        selected_longitude=data.longitude,
        selected_address=temporary_stop.name,
    )
    db.add(change)
    db.commit()
    db.refresh(change)

    return {
        "success": True,
        "message": "Temporary stop change scheduled automatically",
        "request_id": change.id,
        "original_stop_id": original_stop.id,
        "original_stop_name": original_stop.name,
        "temporary_stop_id": temporary_stop.id,
        "temporary_stop_name": temporary_stop.name,
        "temporary_latitude": temporary_stop.latitude,
        "temporary_longitude": temporary_stop.longitude,
        "start_date": change.start_date,
        "end_date": change.end_date,
        "status": change.status,
    }


@app.get("/student/temporary-stop-change")
def get_temporary_stop_change(
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student),
):
    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")

    expire_temporary_stop_changes(db, student.id)
    change = (
        db.query(TemporaryStopChange)
        .filter(TemporaryStopChange.student_id == student.id)
        .order_by(TemporaryStopChange.created_at.desc())
        .first()
    )

    if not change:
        return {"active": False, "scheduled": False}

    temporary_stop = db.query(Stop).filter(Stop.id == change.temporary_stop_id).first()
    today = date.today()
    active = change.start_date <= today <= change.end_date and change.status in ("scheduled", "active")
    scheduled = change.start_date > today and change.status == "scheduled"

    return {
        "active": active,
        "scheduled": scheduled,
        "request_id": change.id,
        "status": change.status,
        "original_stop_id": change.original_stop_id,
        "temporary_stop_id": change.temporary_stop_id,
        "temporary_stop_name": temporary_stop.name if temporary_stop else change.selected_address,
        "temporary_latitude": temporary_stop.latitude if temporary_stop else change.selected_latitude,
        "temporary_longitude": temporary_stop.longitude if temporary_stop else change.selected_longitude,
        "start_date": change.start_date,
        "end_date": change.end_date,
    }


@app.delete("/student/temporary-stop-change")
def cancel_temporary_stop_change(
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student),
):
    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")

    expire_temporary_stop_changes(db, student.id)
    change = (
        db.query(TemporaryStopChange)
        .filter(
            TemporaryStopChange.student_id == student.id,
            TemporaryStopChange.status.in_(["scheduled", "active"]),
        )
        .order_by(TemporaryStopChange.created_at.desc())
        .first()
    )

    if not change:
        raise HTTPException(status_code=404, detail="No active or scheduled temporary stop change found")

    change.status = "cancelled"
    _deactivate_custom_stop(db, change)
    db.commit()

    return {"success": True, "message": "Temporary stop change cancelled", "request_id": change.id}


# ============================================================
# WAIT REQUESTS (STUDENT & DRIVER)
# ============================================================

@app.post("/student/wait-request")
def create_wait_request(
    data: WaitRequestCreate,
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    if data.minutes <= 0 or data.minutes > 4:
        raise HTTPException(status_code=400, detail="Wait time must be between 1 and 4 minutes")

    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")

    # Resolve effective stop and bus (respecting temporary stop change and missed bus allotment)
    stop, _ = get_effective_student_stop(db, student)
    if not stop:
        raise HTTPException(status_code=404, detail="Assigned stop not found")

    allotment = get_active_missed_bus_allotment(db, student.id)
    target_bus_id = allotment.alternative_bus_id if allotment else student.bus_id
    if not target_bus_id:
        raise HTTPException(status_code=400, detail="No bus assigned to this student")

    if not student_is_travelling_today(db, student.id):
        raise HTTPException(status_code=400, detail="You marked yourself as not travelling today")

    trip = (
        db.query(Trip)
        .filter(Trip.id == allotment.alternative_trip_id, Trip.status == "active")
        .first()
        if (allotment and allotment.alternative_trip_id)
        else get_active_trip_for_bus(db, target_bus_id)
    )
    if not trip:
        raise HTTPException(status_code=400, detail="No active bus trip right now")

    if has_passed_stop(db, trip, stop):
        raise HTTPException(status_code=400, detail="The bus has already passed your stop")

    eta_minutes = get_eta_minutes_to_stop(db, target_bus_id, trip.id, stop)
    if eta_minutes is None:
        raise HTTPException(status_code=400, detail="Live ETA is currently unavailable")

    if eta_minutes < WAIT_MIN_ETA_MINUTES or eta_minutes > WAIT_MAX_ETA_MINUTES:
        raise HTTPException(
            status_code=400,
            detail=f"Wait requests are available only when your bus ETA is between {WAIT_MIN_ETA_MINUTES} and {WAIT_MAX_ETA_MINUTES} minutes. Current ETA: {eta_minutes} minutes."
        )

    if active_student_request_exists(db, student.id, trip.id):
        raise HTTPException(status_code=400, detail="You already have an active wait request for this trip")

    if active_skip_cooldown(db, student.id, trip.id):
        raise HTTPException(status_code=400, detail="You recently had a wait request skipped by the driver. Please try again later.")

    if student_rate_limited(db, student.id):
        raise HTTPException(status_code=429, detail="Please wait 30 seconds before requesting another wait.")

    if weekly_wait_limit_reached(db, student.id):
        raise HTTPException(status_code=400, detail="Your weekly wait-request limit has been reached.")

    remaining_budget = trip.wait_budget_total - trip.wait_budget_used
    if remaining_budget <= 0:
        send_notification(
            db,
            current_student["user_id"],
            "Wait unavailable",
            "The bus can't wait further today.",
            "wait_budget_exhausted",
            {"trip_id": trip.id, "bus_id": trip.bus_id},
            related_bus_id=trip.bus_id,
            related_trip_id=trip.id
        )
        db.commit()
        raise HTTPException(status_code=400, detail="The bus can't wait further today.")

    existing_group_requests = db.query(WaitRequest).filter(
        WaitRequest.trip_id == trip.id,
        WaitRequest.stop_id == stop.id,
        WaitRequest.status == "pending"
    ).all()

    existing_group_max = max([r.minutes for r in existing_group_requests], default=0)
    grouped_requested_minutes = max(existing_group_max, data.minutes)

    if grouped_requested_minutes > remaining_budget:
        send_notification(
            db,
            current_student["user_id"],
            "Wait unavailable",
            "The bus can't wait further today.",
            "wait_budget_exhausted",
            {"trip_id": trip.id, "bus_id": trip.bus_id, "remaining_budget": remaining_budget},
            related_bus_id=trip.bus_id,
            related_trip_id=trip.id
        )
        db.commit()
        raise HTTPException(status_code=400, detail="The bus has insufficient remaining wait buffer today.")

    now = datetime.utcnow()
    wait_request = WaitRequest(
        student_id=student.id,
        bus_id=target_bus_id,
        trip_id=trip.id,
        stop_id=stop.id,
        minutes=data.minutes,
        status="pending",
        created_at=now,
        auto_accept_at=(now + timedelta(seconds=WAIT_DRIVER_SKIP_WINDOW_SECONDS))
    )
    db.add(wait_request)
    db.commit()
    db.refresh(wait_request)

    bus = db.query(Bus).filter(Bus.id == target_bus_id).first()
    driver = db.query(Driver).filter(Driver.id == bus.driver_id).first() if bus and bus.driver_id else None

    if driver:
        grouped_count = len(existing_group_requests) + 1
        student_word = "student" if grouped_count == 1 else "students"
        message = f"{grouped_count} {student_word} at {stop.name} — waiting {grouped_requested_minutes} min."

        send_notification(
            db,
            driver.user_id,
            "Bus Wait Request",
            message,
            "wait_request",
            {
                "trip_id": trip.id,
                "stop_id": stop.id,
                "minutes": grouped_requested_minutes,
                "student_count": grouped_count,
                "wait_request_id": wait_request.id,
                "action": "skip_only"
            },
            related_bus_id=target_bus_id,
            related_trip_id=trip.id,
            related_wait_request_id=wait_request.id
        )

    db.commit()

    return {
        "message": "Wait request created",
        "request_id": wait_request.id,
        "student_id": student.id,
        "bus_id": target_bus_id,
        "trip_id": trip.id,
        "stop_id": stop.id,
        "minutes": wait_request.minutes,
        "grouped_minutes": grouped_requested_minutes,
        "eta_minutes": eta_minutes,
        "status": wait_request.status,
        "auto_accept_at": wait_request.auto_accept_at
    }


@app.get("/student/wait-request/status")
def get_wait_request_status(
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")

    wait_request = (
        db.query(WaitRequest)
        .filter(WaitRequest.student_id == student.id)
        .order_by(WaitRequest.created_at.desc())
        .first()
    )

    if not wait_request:
        raise HTTPException(status_code=404, detail="No wait request found")

    trip = db.query(Trip).filter(Trip.id == wait_request.trip_id).first() if wait_request.trip_id else None

    if trip and trip.status == "active":
        reconcile_wait_requests(db, trip)
        db.refresh(wait_request)

    if (
        wait_request.status == "accepted"
        and wait_request.wait_until is not None
        and wait_request.wait_until <= datetime.utcnow()
    ):
        wait_request.status = "completed"
        db.commit()
        db.refresh(wait_request)

    return {
        "request_id": wait_request.id,
        "student_id": wait_request.student_id,
        "bus_id": wait_request.bus_id,
        "trip_id": wait_request.trip_id,
        "stop_id": wait_request.stop_id,
        "minutes": wait_request.minutes,
        "status": wait_request.status,
        "created_at": to_utc_iso(wait_request.created_at),
        "auto_accept_at": to_utc_iso(wait_request.auto_accept_at),
        "wait_until": to_utc_iso(wait_request.wait_until),
        "skipped_at": to_utc_iso(wait_request.skipped_at),
        "cooldown_until": to_utc_iso(wait_request.cooldown_until)
    }


@app.get("/driver/wait-requests")
def get_driver_wait_requests(
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver)
):
    driver = db.query(Driver).filter(Driver.user_id == current_driver["user_id"]).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")

    bus = db.query(Bus).filter(Bus.driver_id == driver.id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="No bus assigned to this driver")

    trip = get_active_trip_for_bus(db, bus.id)
    if not trip:
        return {"bus_id": bus.id, "bus_number": bus.bus_number, "requests": []}

    now = datetime.utcnow()
    pending_requests = (
        db.query(WaitRequest)
        .filter(
            WaitRequest.bus_id == bus.id,
            WaitRequest.trip_id == trip.id,
            WaitRequest.status == "pending"
        )
        .order_by(WaitRequest.created_at.asc())
        .all()
    )

    grouped = {}
    for request in pending_requests:
        if request.stop_id is None:
            continue
        key = (request.trip_id, request.stop_id)
        grouped.setdefault(key, []).append(request)

    for key, group in grouped.items():
        group_deadline = max(
            (r.auto_accept_at for r in group if r.auto_accept_at),
            default=None
        )

        if group_deadline is not None and group_deadline > now:
            continue

        stop_id = key[1]
        stop = db.query(Stop).filter(Stop.id == stop_id).first()
        if not stop:
            continue

        if has_passed_stop(db, trip, stop):
            for request in group:
                request.status = "rejected"
                request.skipped_at = now
                request.cooldown_until = now + timedelta(minutes=WAIT_SKIP_COOLDOWN_MINUTES)
                student = db.query(Student).filter(Student.id == request.student_id).first()
                if student:
                    send_notification(
                        db,
                        student.user_id,
                        "Wait Request Cancelled",
                        "The bus has already passed your stop.",
                        "wait_rejected",
                        {"wait_request_id": request.id, "trip_id": trip.id, "stop_id": stop.id},
                        related_bus_id=bus.id,
                        related_trip_id=trip.id,
                        related_wait_request_id=request.id
                    )
            continue

        requested_minutes = max(r.minutes for r in group)
        remaining_budget = trip.wait_budget_total - trip.wait_budget_used

        if remaining_budget <= 0 or requested_minutes > remaining_budget:
            for request in group:
                request.status = "rejected"
                student = db.query(Student).filter(Student.id == request.student_id).first()
                if student:
                    send_notification(
                        db,
                        student.user_id,
                        "Wait unavailable",
                        "The bus can't wait further today.",
                        "wait_budget_exhausted",
                        {"wait_request_id": request.id, "trip_id": trip.id, "stop_id": stop.id, "remaining_budget": remaining_budget},
                        related_bus_id=bus.id,
                        related_trip_id=trip.id,
                        related_wait_request_id=request.id
                    )
            continue

        trip.wait_budget_used += requested_minutes
        wait_until = now + timedelta(minutes=requested_minutes)

        for request in group:
            request.status = "accepted"
            request.wait_until = wait_until
            student = db.query(Student).filter(Student.id == request.student_id).first()
            if student:
                send_notification(
                    db,
                    student.user_id,
                    "Wait Request Accepted",
                    f"The bus will wait approximately {requested_minutes} minute{'s' if requested_minutes != 1 else ''} at {stop.name}.",
                    "wait_accepted",
                    {"wait_request_id": request.id, "trip_id": trip.id, "stop_id": stop.id, "minutes": requested_minutes, "wait_until": to_utc_iso(wait_until)},
                    related_bus_id=bus.id,
                    related_trip_id=trip.id,
                    related_wait_request_id=request.id
                )

    db.commit()

    pending_requests = (
        db.query(WaitRequest)
        .filter(
            WaitRequest.bus_id == bus.id,
            WaitRequest.trip_id == trip.id,
            WaitRequest.status == "pending"
        )
        .order_by(WaitRequest.created_at.asc())
        .all()
    )

    grouped = {}
    for request in pending_requests:
        if request.stop_id is None:
            continue
        key = (request.trip_id, request.stop_id)
        grouped.setdefault(key, []).append(request)

    result = []
    for key, group in grouped.items():
        stop_id = key[1]
        stop = db.query(Stop).filter(Stop.id == stop_id).first()
        if not stop:
            continue

        requested_minutes = max(r.minutes for r in group)
        earliest_deadline = min((r.auto_accept_at for r in group if r.auto_accept_at), default=None)

        students = []
        for request in group:
            student = db.query(Student).filter(Student.id == request.student_id).first()
            students.append({
                "request_id": request.id,
                "student_id": request.student_id,
                "roll_number": student.roll_number if student else None
            })

        result.append({
            "trip_id": trip.id,
            "stop_id": stop.id,
            "stop_name": stop.name,
            "student_count": len(group),
            "minutes": requested_minutes,
            "auto_accept_at": earliest_deadline,
            "action": "skip_only",
            "students": students
        })

    return {
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "trip_id": trip.id,
        "wait_budget_total": trip.wait_budget_total,
        "wait_budget_used": trip.wait_budget_used,
        "wait_budget_remaining": max(0, trip.wait_budget_total - trip.wait_budget_used),
        "requests": result
    }


@app.post("/driver/wait-request/{request_id}/accept")
def accept_wait_request(
    request_id: int,
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver)
):
    driver = db.query(Driver).filter(Driver.user_id == current_driver["user_id"]).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")

    bus = db.query(Bus).filter(Bus.driver_id == driver.id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="No bus assigned to this driver")

    request = db.query(WaitRequest).filter(
        WaitRequest.id == request_id,
        WaitRequest.bus_id == bus.id
    ).first()

    if not request:
        raise HTTPException(status_code=404, detail="Wait request not found")

    if request.status != "pending":
        raise HTTPException(status_code=400, detail="Request is no longer pending")

    request.status = "accepted"
    db.commit()
    db.refresh(request)

    student = db.query(Student).filter(Student.id == request.student_id).first()
    if student:
        send_notification(
            db,
            student.user_id,
            "Wait Request Accepted",
            "Your driver has accepted your wait request.",
            "wait_accepted",
            {"wait_request_id": request.id},
            related_bus_id=request.bus_id,
            related_wait_request_id=request.id
        )
        db.commit()

    return {
        "message": "Wait request accepted",
        "request_id": request.id,
        "minutes": request.minutes,
        "status": request.status
    }


@app.post("/driver/wait-request/{request_id}/reject")
def reject_wait_request(
    request_id: int,
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver)
):
    driver = db.query(Driver).filter(Driver.user_id == current_driver["user_id"]).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")

    bus = db.query(Bus).filter(Bus.driver_id == driver.id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="No bus assigned to this driver")

    request = db.query(WaitRequest).filter(
        WaitRequest.id == request_id,
        WaitRequest.bus_id == bus.id
    ).first()

    if not request:
        raise HTTPException(status_code=404, detail="Wait request not found")

    if request.status != "pending":
        raise HTTPException(status_code=400, detail="Request is no longer pending")

    request.status = "rejected"
    db.commit()
    db.refresh(request)

    student = db.query(Student).filter(Student.id == request.student_id).first()
    if student:
        send_notification(
            db,
            student.user_id,
            "Wait Request Rejected",
            "Your driver could not accept your wait request.",
            "wait_rejected",
            {"wait_request_id": request.id},
            related_bus_id=request.bus_id,
            related_wait_request_id=request.id
        )
        db.commit()

    return {
        "message": "Wait request rejected",
        "request_id": request.id,
        "status": request.status
    }

@app.get("/driver/route-stops")
def get_driver_route_stops(
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver)
):
    # Authoritative college geofence coordinates
    COLLEGE_LAT = 18.054145359568437
    COLLEGE_LNG = 79.53558731724873

    driver = db.query(Driver).filter(Driver.user_id == current_driver["user_id"]).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")

    bus = db.query(Bus).filter(Bus.driver_id == driver.id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="No bus assigned to this driver")

    if bus.route_id is None:
        raise HTTPException(status_code=400, detail="No route assigned to this bus")

    today = date.today()

    # 1. Fetch active route stops (excluding soft-deleted / inactive custom stops)
    regular_stops = (
        db.query(Stop)
        .filter(Stop.route_id == bus.route_id, Stop.is_active == True)
        .order_by(Stop.stop_order.asc())
        .all()
    )

    # 2. Determine all students boarding this bus today:
    # A) Regularly assigned students
    regular_students = db.query(Student).filter(Student.bus_id == bus.id).all()
    regular_student_ids = [s.id for s in regular_students]

    # Check which regular students have allotted to another bus today
    missed_other_ids = set()
    if regular_student_ids:
        missed_other_rows = (
            db.query(MissedBusAllotment.student_id)
            .filter(
                MissedBusAllotment.student_id.in_(regular_student_ids),
                MissedBusAllotment.original_bus_id == bus.id,
                MissedBusAllotment.alternative_bus_id != bus.id,
                MissedBusAllotment.status == "active",
            )
            .all()
        )
        missed_other_ids = {r[0] for r in missed_other_rows}

    # B) Alternative students allotted to this bus today
    allotted_to_this_bus = (
        db.query(Student)
        .join(MissedBusAllotment, MissedBusAllotment.student_id == Student.id)
        .filter(
            MissedBusAllotment.alternative_bus_id == bus.id,
            MissedBusAllotment.status == "active",
        )
        .all()
    )

    # Combined active student passenger list for this bus
    boarding_students = [s for s in regular_students if s.id not in missed_other_ids] + allotted_to_this_bus
    boarding_student_ids = [s.id for s in boarding_students]

    # 3. Find students who marked 'not_travelling' for today
    not_travelling_ids = set()
    if boarding_student_ids:
        not_travelling_rows = db.query(TravelStatus.student_id).filter(
            TravelStatus.student_id.in_(boarding_student_ids),
            TravelStatus.date == today,
            TravelStatus.status == "not_travelling"
        ).all()
        not_travelling_ids = {row[0] for row in not_travelling_rows}

    # 4. Count passengers per effective stop
    stop_student_counts = {}
    total_expected_today = 0
    stops_by_id = {st.id: st for st in regular_stops}

    for s in boarding_students:
        if s.id not in not_travelling_ids:
            total_expected_today += 1
            eff_stop, _ = get_effective_student_stop(db, s)
            if eff_stop and eff_stop.is_active:
                stop_student_counts[eff_stop.id] = stop_student_counts.get(eff_stop.id, 0) + 1
                if eff_stop.id not in stops_by_id:
                    stops_by_id[eff_stop.id] = eff_stop

    stops_list = sorted(stops_by_id.values(), key=lambda s: s.stop_order)

    return {
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "route_id": bus.route_id,
        "total_stops": len(stops_list),
        "total_students_today": total_expected_today,
        "total_assigned_students": len(regular_students),
        "college_location": {
            "latitude": COLLEGE_LAT,
            "longitude": COLLEGE_LNG,
            "name": "KITSW / College"
        },
        "stops": [
            {
                "stop_id": stop.id,
                "name": stop.name,
                "latitude": stop.latitude,
                "longitude": stop.longitude,
                "stop_order": stop.stop_order,
                "is_custom": getattr(stop, "is_custom", False),
                "student_count": stop_student_counts.get(stop.id, 0)
            }
            for stop in stops_list
        ]
    }
@app.post("/student/travel-status")
def update_travel_status(
    data: TravelStatusCreate,
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")

    allowed_statuses = ["travelling", "not_travelling"]
    if data.status not in allowed_statuses:
        raise HTTPException(status_code=400, detail="Invalid travel status")

    today = date.today()
    travel_status = db.query(TravelStatus).filter(
        TravelStatus.student_id == student.id,
        TravelStatus.date == today
    ).first()

    if travel_status:
        travel_status.status = data.status
    else:
        travel_status = TravelStatus(
            student_id=student.id,
            date=today,
            status=data.status
        )
        db.add(travel_status)

    db.commit()
    db.refresh(travel_status)

    return {
        "message": "Travel status updated successfully",
        "student_id": student.id,
        "date": travel_status.date,
        "status": travel_status.status
    }


@app.get("/student/travel-status")
def get_travel_status(
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")

    travel_status = (
        db.query(TravelStatus)
        .filter(TravelStatus.student_id == student.id, TravelStatus.date == date.today())
        .order_by(TravelStatus.created_at.desc())
        .first()
    )

    if travel_status is None:
        return {"student_id": student.id, "status": "travelling", "date": date.today()}

    return {"student_id": student.id, "status": travel_status.status, "date": travel_status.date}


# ============================================================
# ADMIN CONTROL PANEL & TRANSPORT MANAGEMENT ENDPOINTS
# ============================================================

def log_admin_activity(db: Session, admin_user_id: int, action: str, entity_type: str | None = None, entity_id: str | None = None, details: str | None = None):
    try:
        activity = AdminActivityLog(
            admin_user_id=admin_user_id,
            action=action,
            entity_type=entity_type,
            entity_id=str(entity_id) if entity_id is not None else None,
            details=details
        )
        db.add(activity)
        db.commit()
    except Exception as log_err:
        logger.warning(f"Admin activity logging failed: {log_err}")
        db.rollback()


def admin_bus_payload(bus: Bus, db: Session):
    route = db.query(Route).filter(Route.id == bus.route_id).first() if bus.route_id else None
    driver = db.query(Driver).filter(Driver.id == bus.driver_id).first() if bus.driver_id else None
    driver_user = db.query(User).filter(User.id == driver.user_id).first() if driver else None
    location = db.query(BusLocation).filter(BusLocation.bus_id == bus.id).order_by(BusLocation.timestamp.desc()).first()
    active_trip = db.query(Trip).filter(Trip.bus_id == bus.id, Trip.status == "active").first()
    
    student_count = db.query(Student).filter(Student.bus_id == bus.id).count()
    student_ids = [s.id for s in db.query(Student.id).filter(Student.bus_id == bus.id).all()]
    
    today = date.today()
    travelling_today_count = 0
    if student_ids:
        travelling_today_count = db.query(TravelStatus).filter(
            TravelStatus.student_id.in_(student_ids),
            TravelStatus.date == today,
            TravelStatus.status == "travelling"
        ).count()

    return {
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "registration_number": bus.registration_number,
        "status": bus.status,
        "route_id": bus.route_id,
        "route_name": route.name if route else None,
        "driver_id": bus.driver_id,
        "driver_code": driver.driver_code if driver else None,
        "driver_name": driver_user.name if driver_user else None,
        "driver_phone": driver_user.phone if driver_user else None,
        "student_count": student_count,
        "travelling_today_count": travelling_today_count,
        "trip_status": "active" if active_trip else "idle",
        "active_trip_id": active_trip.id if active_trip else None,
        "latest_location": (
            {
                "latitude": location.latitude,
                "longitude": location.longitude,
                "speed": location.speed,
                "timestamp": location.timestamp.isoformat() + "Z"
            }
            if location else None
        )
    }


def admin_driver_payload(driver: Driver, db: Session):
    user = db.query(User).filter(User.id == driver.user_id).first()
    bus = db.query(Bus).filter(Bus.driver_id == driver.id).first()
    route = db.query(Route).filter(Route.id == bus.route_id).first() if bus and bus.route_id else None
    active_trip = db.query(Trip).filter(Trip.driver_id == driver.id, Trip.status == "active").first()
    latest_loc = db.query(BusLocation).filter(BusLocation.bus_id == bus.id).order_by(BusLocation.timestamp.desc()).first() if bus else None
    complaints_count = db.query(DriverComplaint).filter(DriverComplaint.driver_id == driver.id).count()

    return {
        "driver_id": driver.id,
        "user_id": driver.user_id,
        "driver_code": driver.driver_code,
        "license_number": driver.license_number,
        "name": user.name if user else None,
        "phone": user.phone if user else None,
        "bus_id": bus.id if bus else None,
        "bus_number": bus.bus_number if bus else None,
        "route_id": route.id if route else None,
        "route_name": route.name if route else None,
        "trip_status": "active" if active_trip else "idle",
        "is_online": True if active_trip or latest_loc else False,
        "complaints_count": complaints_count
    }


def admin_student_payload(student: Student, db: Session):
    user = db.query(User).filter(User.id == student.user_id).first()
    bus = db.query(Bus).filter(Bus.id == student.bus_id).first() if student.bus_id else None
    stop = db.query(Stop).filter(Stop.id == student.stop_id).first() if student.stop_id else None
    route = db.query(Route).filter(Route.id == stop.route_id).first() if stop else (db.query(Route).filter(Route.id == bus.route_id).first() if bus and bus.route_id else None)
    
    today = date.today()
    travel = db.query(TravelStatus).filter(TravelStatus.student_id == student.id, TravelStatus.date == today).order_by(TravelStatus.created_at.desc()).first()
    travelling_today = travel.status == "travelling" if travel else True  # default travelling

    return {
        "student_id": student.id,
        "user_id": student.user_id,
        "roll_number": student.roll_number,
        "name": user.name if user else None,
        "phone": user.phone if user else None,
        "department": student.department,
        "bus_id": student.bus_id,
        "bus_number": bus.bus_number if bus else None,
        "stop_id": student.stop_id,
        "stop_name": stop.name if stop else None,
        "stop_order": stop.stop_order if stop else None,
        "route_id": route.id if route else None,
        "route_name": route.name if route else None,
        "travelling_today": travelling_today
    }


def validate_bus_links(db: Session, route_id: int | None, driver_id: int | None, bus_id: int | None = None):
    if route_id is not None and not db.query(Route).filter(Route.id == route_id).first():
        raise HTTPException(status_code=404, detail="Route not found")
    if driver_id is not None:
        driver = db.query(Driver).filter(Driver.id == driver_id).first()
        if not driver:
            raise HTTPException(status_code=404, detail="Driver not found")
        other_bus = db.query(Bus).filter(Bus.driver_id == driver_id).first()
        if other_bus and other_bus.id != bus_id:
            other_bus.driver_id = None


# ------------------------------------------------------------
# 1. ADMIN DASHBOARD / OVERVIEW
# ------------------------------------------------------------

@app.get("/admin/dashboard")
def get_admin_dashboard(db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    buses = db.query(Bus).order_by(Bus.bus_number.asc()).all()
    total_students = db.query(Student).count()
    total_drivers = db.query(Driver).count()
    total_routes = db.query(Route).count()
    total_stops = db.query(Stop).count()

    active_buses = [b for b in buses if b.status == "active"]
    offline_buses = [b for b in buses if b.status != "active"]

    # Active trips
    active_trips_raw = db.query(Trip).filter(Trip.status == "active").order_by(Trip.started_at.desc()).all()
    active_trips = []
    for t in active_trips_raw:
        bus = db.query(Bus).filter(Bus.id == t.bus_id).first()
        driver = db.query(Driver).filter(Driver.id == t.driver_id).first()
        driver_user = db.query(User).filter(User.id == driver.user_id).first() if driver else None
        route = db.query(Route).filter(Route.id == t.route_id).first() if t.route_id else (db.query(Route).filter(Route.id == bus.route_id).first() if bus and bus.route_id else None)
        active_trips.append({
            "trip_id": t.id,
            "bus_id": t.bus_id,
            "bus_number": bus.bus_number if bus else f"Bus #{t.bus_id}",
            "driver_name": driver_user.name if driver_user else "Driver",
            "route_name": route.name if route else "Assigned Route",
            "started_at": t.started_at.isoformat() + "Z" if t.started_at else None
        })

    # Recently completed trips
    recent_trips_raw = db.query(Trip).filter(Trip.status == "completed").order_by(Trip.ended_at.desc()).limit(5).all()
    recent_completed_trips = []
    for t in recent_trips_raw:
        bus = db.query(Bus).filter(Bus.id == t.bus_id).first()
        driver = db.query(Driver).filter(Driver.id == t.driver_id).first()
        driver_user = db.query(User).filter(User.id == driver.user_id).first() if driver else None
        recent_completed_trips.append({
            "trip_id": t.id,
            "bus_number": bus.bus_number if bus else f"Bus #{t.bus_id}",
            "driver_name": driver_user.name if driver_user else "Driver",
            "ended_at": t.ended_at.isoformat() + "Z" if t.ended_at else None
        })

    # Students travelling today count
    today = date.today()
    travelling_count = db.query(TravelStatus).filter(
        TravelStatus.date == today,
        TravelStatus.status == "travelling"
    ).count()

    # Recent alerts
    recent_alerts = db.query(Notification).filter(
        Notification.type.in_(["emergency_sos", "detour_alert", "driver_complaint_poll"])
    ).order_by(Notification.created_at.desc()).limit(8).all()

    alerts_list = []
    for a in recent_alerts:
        bus = db.query(Bus).filter(Bus.id == a.related_bus_id).first() if a.related_bus_id else None
        alerts_list.append({
            "id": a.id,
            "title": a.title,
            "message": a.message,
            "type": a.type,
            "is_read": a.is_read,
            "bus_number": bus.bus_number if bus else None,
            "created_at": a.created_at.isoformat() + "Z" if a.created_at else None
        })

    return {
        "total_buses": len(buses),
        "active_buses": len(active_buses),
        "offline_buses": len(offline_buses),
        "total_drivers": total_drivers,
        "total_students": total_students,
        "students_travelling_today": travelling_count,
        "total_routes": total_routes,
        "total_stops": total_stops,
        "active_trips": active_trips,
        "recent_completed_trips": recent_completed_trips,
        "recent_alerts": alerts_list,
        "buses": [admin_bus_payload(bus, db) for bus in buses]
    }


# ------------------------------------------------------------
# 2. BUS MANAGEMENT
# ------------------------------------------------------------

@app.get("/admin/buses")
def list_admin_buses(db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    buses = db.query(Bus).order_by(Bus.bus_number.asc()).all()
    return {"buses": [admin_bus_payload(bus, db) for bus in buses]}


@app.get("/admin/buses/{bus_id}")
def get_admin_bus_details(bus_id: int, db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    bus = db.query(Bus).filter(Bus.id == bus_id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="Bus not found")
    payload = admin_bus_payload(bus, db)
    
    # Add assigned students list
    students = db.query(Student).filter(Student.bus_id == bus.id).all()
    payload["students"] = [admin_student_payload(s, db) for s in students]
    return payload


@app.post("/admin/buses", status_code=201)
def admin_create_bus(
    data: AdminBusCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    if db.query(Bus).filter(Bus.bus_number == data.bus_number).first():
        raise HTTPException(status_code=400, detail="Bus number already exists")

    validate_bus_links(db, data.route_id, data.driver_id)
    bus = Bus(**data.model_dump())
    db.add(bus)
    db.commit()
    db.refresh(bus)
    log_admin_activity(db, current_user["user_id"], "CREATE_BUS", "bus", str(bus.id), f"Created Bus {bus.bus_number}")
    return admin_bus_payload(bus, db)


@app.patch("/admin/buses/{bus_id}")
def admin_update_bus(
    bus_id: int,
    data: AdminBusUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    bus = db.query(Bus).filter(Bus.id == bus_id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="Bus not found")

    changes = data.model_dump(exclude_unset=True)
    if "bus_number" in changes and changes["bus_number"] != bus.bus_number:
        if db.query(Bus).filter(Bus.bus_number == changes["bus_number"]).first():
            raise HTTPException(status_code=400, detail="Bus number already exists")

    validate_bus_links(db, changes.get("route_id", bus.route_id), changes.get("driver_id", bus.driver_id), bus.id)
    for field, value in changes.items():
        setattr(bus, field, value)

    db.commit()
    db.refresh(bus)
    log_admin_activity(db, current_user["user_id"], "UPDATE_BUS", "bus", str(bus.id), f"Updated Bus {bus.bus_number}")
    return admin_bus_payload(bus, db)


@app.delete("/admin/buses/{bus_id}")
def admin_delete_bus(bus_id: int, db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    bus = db.query(Bus).filter(Bus.id == bus_id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="Bus not found")

    active_trip = db.query(Trip).filter(Trip.bus_id == bus.id, Trip.status == "active").first()
    if active_trip:
        raise HTTPException(status_code=400, detail="Cannot delete a bus that is currently on an active trip")

    # Unassign students
    db.query(Student).filter(Student.bus_id == bus.id).update({"bus_id": None})
    bus_num = bus.bus_number
    db.delete(bus)
    db.commit()
    log_admin_activity(db, current_user["user_id"], "DELETE_BUS", "bus", str(bus_id), f"Deleted Bus {bus_num}")
    return {"message": f"Bus {bus_num} deleted successfully", "bus_id": bus_id}


@app.post("/admin/buses/{bus_id}/assign-driver")
def admin_bus_assign_driver(
    bus_id: int,
    data: AdminAssignBusDriverRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    bus = db.query(Bus).filter(Bus.id == bus_id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="Bus not found")

    if data.driver_id is not None:
        driver = db.query(Driver).filter(Driver.id == data.driver_id).first()
        if not driver:
            raise HTTPException(status_code=404, detail="Driver not found")
        # Clear other bus assigned to this driver
        other_bus = db.query(Bus).filter(Bus.driver_id == data.driver_id).first()
        if other_bus and other_bus.id != bus.id:
            other_bus.driver_id = None
        bus.driver_id = driver.id
    else:
        bus.driver_id = None

    db.commit()
    db.refresh(bus)
    log_admin_activity(db, current_user["user_id"], "ASSIGN_DRIVER", "bus", str(bus.id), f"Assigned driver {data.driver_id} to Bus {bus.bus_number}")
    return admin_bus_payload(bus, db)


@app.post("/admin/buses/{bus_id}/assign-route")
def admin_bus_assign_route(
    bus_id: int,
    data: AdminAssignBusRouteRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    bus = db.query(Bus).filter(Bus.id == bus_id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="Bus not found")

    if data.route_id is not None:
        route = db.query(Route).filter(Route.id == data.route_id).first()
        if not route:
            raise HTTPException(status_code=404, detail="Route not found")
        bus.route_id = route.id
    else:
        bus.route_id = None

    db.commit()
    db.refresh(bus)
    log_admin_activity(db, current_user["user_id"], "ASSIGN_ROUTE", "bus", str(bus.id), f"Assigned route {data.route_id} to Bus {bus.bus_number}")
    return admin_bus_payload(bus, db)


# ------------------------------------------------------------
# 3. DRIVER MANAGEMENT
# ------------------------------------------------------------

@app.get("/admin/drivers")
def list_admin_drivers(db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    drivers = db.query(Driver).order_by(Driver.driver_code.asc()).all()
    return {"drivers": [admin_driver_payload(d, db) for d in drivers]}


@app.get("/admin/drivers/{driver_id}")
def get_admin_driver_details(driver_id: int, db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    driver = db.query(Driver).filter(Driver.id == driver_id).first()
    if not driver:
        # Fallback check if user_id was passed
        driver = db.query(Driver).filter(Driver.user_id == driver_id).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    payload = admin_driver_payload(driver, db)

    # Trip history
    trips = db.query(Trip).filter(Trip.driver_id == driver.id).order_by(Trip.started_at.desc()).limit(15).all()
    payload["trip_history"] = [
        {
            "trip_id": t.id,
            "status": t.status,
            "started_at": t.started_at.isoformat() + "Z" if t.started_at else None,
            "ended_at": t.ended_at.isoformat() + "Z" if t.ended_at else None,
            "wait_budget_used": t.wait_budget_used
        }
        for t in trips
    ]

    # Complaints
    complaints = db.query(DriverComplaint).filter(DriverComplaint.driver_id == driver.id).order_by(DriverComplaint.created_at.desc()).limit(10).all()
    payload["complaints"] = [
        {
            "id": c.id,
            "reason": c.reason,
            "description": c.description,
            "status": c.status,
            "created_at": c.created_at.isoformat() + "Z" if c.created_at else None
        }
        for c in complaints
    ]

    return payload


@app.post("/admin/drivers", status_code=201)
def admin_create_driver(
    data: AdminDriverCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    if db.query(User).filter(User.phone == data.phone).first():
        raise HTTPException(status_code=400, detail="Phone number already registered")
    if db.query(Driver).filter(Driver.driver_code == data.driver_code).first():
        raise HTTPException(status_code=400, detail="Driver code already exists")

    user = User(name=data.name, phone=data.phone, password_hash=hash_password(data.password), role="driver")
    db.add(user)
    db.commit()
    db.refresh(user)

    driver = Driver(user_id=user.id, driver_code=data.driver_code, license_number=data.license_number)
    db.add(driver)
    db.commit()
    db.refresh(driver)

    log_admin_activity(db, current_user["user_id"], "CREATE_DRIVER", "driver", str(driver.id), f"Created driver {driver.driver_code} ({user.name})")
    return admin_driver_payload(driver, db)


@app.patch("/admin/drivers/{driver_id}")
def admin_update_driver(
    driver_id: int,
    data: AdminDriverUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    driver = db.query(Driver).filter(Driver.id == driver_id).first()
    if not driver:
        driver = db.query(Driver).filter(Driver.user_id == driver_id).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    user = db.query(User).filter(User.id == driver.user_id).first()

    if data.name:
        user.name = data.name
    if data.phone and data.phone != user.phone:
        if db.query(User).filter(User.phone == data.phone, User.id != user.id).first():
            raise HTTPException(status_code=400, detail="Phone number already in use")
        user.phone = data.phone
    if data.password:
        user.password_hash = hash_password(data.password)
    if data.driver_code and data.driver_code != driver.driver_code:
        if db.query(Driver).filter(Driver.driver_code == data.driver_code, Driver.id != driver.id).first():
            raise HTTPException(status_code=400, detail="Driver code already in use")
        driver.driver_code = data.driver_code
    if data.license_number is not None:
        driver.license_number = data.license_number

    db.commit()
    db.refresh(driver)
    log_admin_activity(db, current_user["user_id"], "UPDATE_DRIVER", "driver", str(driver.id), f"Updated driver {driver.driver_code}")
    return admin_driver_payload(driver, db)


@app.delete("/admin/drivers/{driver_id}")
def admin_delete_driver(driver_id: int, db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    driver = db.query(Driver).filter(Driver.id == driver_id).first()
    if not driver:
        driver = db.query(Driver).filter(Driver.user_id == driver_id).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    # Unassign from any bus
    db.query(Bus).filter(Bus.driver_id == driver.id).update({"driver_id": None})
    user = db.query(User).filter(User.id == driver.user_id).first()
    code = driver.driver_code
    db.delete(driver)
    if user:
        db.delete(user)
    db.commit()
    log_admin_activity(db, current_user["user_id"], "DELETE_DRIVER", "driver", str(driver_id), f"Deleted driver {code}")
    return {"message": f"Driver {code} removed successfully", "driver_id": driver_id}


# ------------------------------------------------------------
# 4. STUDENT MANAGEMENT
# ------------------------------------------------------------

@app.get("/admin/students")
def get_all_students(
    search: str | None = None,
    bus_id: int | None = None,
    route_id: int | None = None,
    stop_id: int | None = None,
    travelling: str | None = None,  # "all", "true", "false"
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    query = db.query(Student)

    if bus_id is not None:
        query = query.filter(Student.bus_id == bus_id)
    if stop_id is not None:
        query = query.filter(Student.stop_id == stop_id)

    students = query.all()
    results = [admin_student_payload(s, db) for s in students]

    # Additional filtering in Python for composite fields
    if route_id is not None:
        results = [r for r in results if r.get("route_id") == route_id]
    if search:
        s_lower = search.lower().strip()
        results = [
            r for r in results
            if (r.get("name") and s_lower in r["name"].lower())
            or (r.get("roll_number") and s_lower in r["roll_number"].lower())
            or (r.get("phone") and s_lower in r["phone"].lower())
            or (r.get("department") and s_lower in r["department"].lower())
            or (r.get("stop_name") and s_lower in r["stop_name"].lower())
        ]
    if travelling == "true":
        results = [r for r in results if r.get("travelling_today") is True]
    elif travelling == "false":
        results = [r for r in results if r.get("travelling_today") is False]

    return {"students": results}


@app.get("/admin/students/{student_id}")
def get_admin_student_details(student_id: int, db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    student = db.query(Student).filter(Student.id == student_id).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")
    return admin_student_payload(student, db)


@app.post("/admin/students", status_code=201)
def admin_create_student(
    data: AdminStudentCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    if db.query(User).filter(User.phone == data.phone).first():
        raise HTTPException(status_code=400, detail="Phone number already registered")
    if db.query(Student).filter(Student.roll_number == data.roll_number).first():
        raise HTTPException(status_code=400, detail="Roll number already registered")

    if data.bus_id and not db.query(Bus).filter(Bus.id == data.bus_id).first():
        raise HTTPException(status_code=404, detail="Bus not found")
    if data.stop_id and not db.query(Stop).filter(Stop.id == data.stop_id).first():
        raise HTTPException(status_code=404, detail="Stop not found")

    user = User(name=data.name, phone=data.phone, password_hash=hash_password(data.password), role="student")
    db.add(user)
    db.commit()
    db.refresh(user)

    student = Student(
        user_id=user.id,
        roll_number=data.roll_number,
        department=data.department,
        bus_id=data.bus_id,
        stop_id=data.stop_id
    )
    db.add(student)
    db.commit()
    db.refresh(student)

    log_admin_activity(db, current_user["user_id"], "CREATE_STUDENT", "student", str(student.id), f"Created student {student.roll_number} ({user.name})")
    return admin_student_payload(student, db)


@app.patch("/admin/students/{student_id}")
def admin_update_student(
    student_id: int,
    data: AdminStudentUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    student = db.query(Student).filter(Student.id == student_id).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")
    user = db.query(User).filter(User.id == student.user_id).first()

    if data.name:
        user.name = data.name
    if data.phone and data.phone != user.phone:
        if db.query(User).filter(User.phone == data.phone, User.id != user.id).first():
            raise HTTPException(status_code=400, detail="Phone number already registered")
        user.phone = data.phone
    if data.password:
        user.password_hash = hash_password(data.password)
    if data.roll_number and data.roll_number != student.roll_number:
        if db.query(Student).filter(Student.roll_number == data.roll_number, Student.id != student.id).first():
            raise HTTPException(status_code=400, detail="Roll number already registered")
        student.roll_number = data.roll_number
    if data.department is not None:
        student.department = data.department
    if data.bus_id is not None:
        if data.bus_id != 0 and not db.query(Bus).filter(Bus.id == data.bus_id).first():
            raise HTTPException(status_code=404, detail="Bus not found")
        student.bus_id = data.bus_id if data.bus_id != 0 else None
    if data.stop_id is not None:
        if data.stop_id != 0 and not db.query(Stop).filter(Stop.id == data.stop_id).first():
            raise HTTPException(status_code=404, detail="Stop not found")
        student.stop_id = data.stop_id if data.stop_id != 0 else None

    db.commit()
    db.refresh(student)
    log_admin_activity(db, current_user["user_id"], "UPDATE_STUDENT", "student", str(student.id), f"Updated student {student.roll_number}")
    return admin_student_payload(student, db)


@app.delete("/admin/students/{student_id}")
def admin_delete_student(student_id: int, db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    student = db.query(Student).filter(Student.id == student_id).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")

    user = db.query(User).filter(User.id == student.user_id).first()
    roll = student.roll_number
    db.delete(student)
    if user:
        db.delete(user)
    db.commit()
    log_admin_activity(db, current_user["user_id"], "DELETE_STUDENT", "student", str(student_id), f"Deleted student {roll}")
    return {"message": f"Student {roll} deleted successfully", "student_id": student_id}


@app.patch("/admin/students/{student_id}/bus")
def admin_assign_student_bus(
    student_id: int,
    data: AdminStudentBusUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    student = db.query(Student).filter(Student.id == student_id).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")
    if data.bus_id is not None and not db.query(Bus).filter(Bus.id == data.bus_id).first():
        raise HTTPException(status_code=404, detail="Bus not found")

    student.bus_id = data.bus_id
    if student.stop_id is not None and data.bus_id is not None:
        stop = db.query(Stop).filter(Stop.id == student.stop_id).first()
        bus = db.query(Bus).filter(Bus.id == data.bus_id).first()
        if not stop or not bus or stop.route_id != bus.route_id:
            student.stop_id = None
    db.commit()
    log_admin_activity(db, current_user["user_id"], "ASSIGN_STUDENT_BUS", "student", str(student.id), f"Assigned student {student.roll_number} to Bus ID {data.bus_id}")
    return {"message": "Student bus assignment updated", "student_id": student.id, "bus_id": student.bus_id}


@app.patch("/admin/students/{student_id}/stop")
def admin_assign_student_stop(
    student_id: int,
    data: AdminStudentStopUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    student = db.query(Student).filter(Student.id == student_id).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")
    if data.stop_id is None:
        student.stop_id = None
    else:
        stop = db.query(Stop).filter(Stop.id == data.stop_id).first()
        if not stop:
            raise HTTPException(status_code=404, detail="Stop not found")
        bus = db.query(Bus).filter(Bus.id == student.bus_id).first() if student.bus_id else None
        if not bus or bus.route_id != stop.route_id:
            raise HTTPException(status_code=400, detail="Stop must belong to the student's assigned bus route")
        student.stop_id = stop.id
    db.commit()
    log_admin_activity(db, current_user["user_id"], "ASSIGN_STUDENT_STOP", "student", str(student.id), f"Assigned student {student.roll_number} to Stop ID {data.stop_id}")
    return {"message": "Student stop assignment updated", "student_id": student.id, "stop_id": student.stop_id}


# ------------------------------------------------------------
# 5. STOP MANAGEMENT
# ------------------------------------------------------------

def admin_stop_payload(stop: Stop, db: Session):
    route = db.query(Route).filter(Route.id == stop.route_id).first()
    student_count = db.query(Student).filter(Student.stop_id == stop.id).count()
    return {
        "stop_id": stop.id,
        "route_id": stop.route_id,
        "route_name": route.name if route else None,
        "name": stop.name,
        "latitude": stop.latitude,
        "longitude": stop.longitude,
        "stop_order": stop.stop_order,
        "student_count": student_count
    }


@app.get("/admin/stops")
def list_admin_stops(
    route_id: int | None = None,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    query = db.query(Stop)
    if route_id is not None:
        query = query.filter(Stop.route_id == route_id)
    stops = query.order_by(Stop.route_id.asc(), Stop.stop_order.asc()).all()
    return {"stops": [admin_stop_payload(s, db) for s in stops]}


@app.get("/admin/stops/{stop_id}")
def get_admin_stop_details(stop_id: int, db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    stop = db.query(Stop).filter(Stop.id == stop_id).first()
    if not stop:
        raise HTTPException(status_code=404, detail="Stop not found")
    payload = admin_stop_payload(stop, db)
    students = db.query(Student).filter(Student.stop_id == stop.id).all()
    payload["students"] = [admin_student_payload(s, db) for s in students]
    return payload


@app.post("/admin/routes/{route_id}/stops", status_code=201)
def admin_create_route_stop(
    route_id: int,
    data: AdminStopCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    route = db.query(Route).filter(Route.id == route_id).first()
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")

    existing = db.query(Stop).filter(Stop.route_id == route_id, Stop.stop_order == data.stop_order).first()
    if existing:
        # Shift existing stops forward
        db.query(Stop).filter(Stop.route_id == route_id, Stop.stop_order >= data.stop_order).update(
            {Stop.stop_order: Stop.stop_order + 1}
        )

    stop = Stop(
        route_id=route_id,
        name=data.name,
        latitude=data.latitude,
        longitude=data.longitude,
        stop_order=data.stop_order
    )
    db.add(stop)
    db.commit()
    db.refresh(stop)
    log_admin_activity(db, current_user["user_id"], "CREATE_STOP", "stop", str(stop.id), f"Created stop {stop.name} in route {route.name}")
    return admin_stop_payload(stop, db)


@app.patch("/admin/stops/{stop_id}")
def admin_update_stop(
    stop_id: int,
    data: AdminStopUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    stop = db.query(Stop).filter(Stop.id == stop_id).first()
    if not stop:
        raise HTTPException(status_code=404, detail="Stop not found")

    if data.name:
        stop.name = data.name
    if data.latitude is not None:
        stop.latitude = data.latitude
    if data.longitude is not None:
        stop.longitude = data.longitude
    if data.stop_order is not None:
        stop.stop_order = data.stop_order
    if data.route_id is not None:
        if not db.query(Route).filter(Route.id == data.route_id).first():
            raise HTTPException(status_code=404, detail="Target route not found")
        stop.route_id = data.route_id

    db.commit()
    db.refresh(stop)
    log_admin_activity(db, current_user["user_id"], "UPDATE_STOP", "stop", str(stop.id), f"Updated stop {stop.name}")
    return admin_stop_payload(stop, db)


@app.delete("/admin/stops/{stop_id}")
def admin_delete_stop(stop_id: int, db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    stop = db.query(Stop).filter(Stop.id == stop_id).first()
    if not stop:
        raise HTTPException(status_code=404, detail="Stop not found")

    # Unassign students
    db.query(Student).filter(Student.stop_id == stop.id).update({"stop_id": None})
    stop_name = stop.name
    route_id = stop.route_id
    deleted_order = stop.stop_order
    db.delete(stop)

    # Reorder remaining stops
    db.query(Stop).filter(Stop.route_id == route_id, Stop.stop_order > deleted_order).update(
        {Stop.stop_order: Stop.stop_order - 1}
    )
    db.commit()
    log_admin_activity(db, current_user["user_id"], "DELETE_STOP", "stop", str(stop_id), f"Deleted stop {stop_name}")
    return {"message": f"Stop {stop_name} deleted successfully", "stop_id": stop_id}


# ------------------------------------------------------------
# 5b. DRIVER COMPLAINTS MANAGEMENT
# ------------------------------------------------------------

@app.get("/admin/complaints")
def list_admin_complaints(
    driver_id: int | None = None,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    query = db.query(DriverComplaint)
    if driver_id is not None:
        query = query.filter(DriverComplaint.driver_id == driver_id)

    complaints = query.order_by(DriverComplaint.created_at.desc()).all()

    # Calculate per-driver complaint counts for summary
    all_complaints = db.query(DriverComplaint).all()
    driver_totals = {}
    for c in all_complaints:
        driver_totals[c.driver_id] = driver_totals.get(c.driver_id, 0) + 1

    payload_list = []
    for c in complaints:
        driver = db.query(Driver).filter(Driver.id == c.driver_id).first()
        driver_user = db.query(User).filter(User.id == driver.user_id).first() if driver else None
        student = db.query(Student).filter(Student.id == c.student_id).first()
        student_user = db.query(User).filter(User.id == student.user_id).first() if student else None
        bus = db.query(Bus).filter(Bus.id == c.bus_id).first()

        verifications = db.query(ComplaintVerification).filter(ComplaintVerification.complaint_id == c.id).all()
        yes_count = sum(1 for v in verifications if v.response == "yes")
        no_count = sum(1 for v in verifications if v.response == "no")
        total_votes = len(verifications)

        is_corroborated = (
            total_votes >= COMPLAINT_MIN_VOTES
            and (yes_count / total_votes) >= COMPLAINT_MIN_YES_RATIO
            if total_votes > 0 else False
        )

        payload_list.append({
            "complaint_id": c.id,
            "reason": c.reason,
            "description": c.description,
            "status": c.status,
            "created_at": to_utc_iso(c.created_at),
            "driver_id": c.driver_id,
            "driver_name": driver_user.name if driver_user else f"Driver #{c.driver_id}",
            "driver_code": driver.driver_code if driver else None,
            "driver_total_complaints": driver_totals.get(c.driver_id, 0),
            "bus_id": c.bus_id,
            "bus_number": bus.bus_number if bus else None,
            "student_id": c.student_id,
            "student_name": student_user.name if student_user else f"Student #{c.student_id}",
            "roll_number": student.roll_number if student else None,
            "corroboration": {
                "yes_count": yes_count,
                "no_count": no_count,
                "total_votes": total_votes,
                "is_corroborated": is_corroborated
            }
        })

    return {
        "complaints": payload_list,
        "total_complaints": len(all_complaints),
        "driver_counts": driver_totals
    }


@app.post("/admin/routes/{route_id}/reorder-stops")
def admin_reorder_route_stops(
    route_id: int,
    data: AdminReorderStopsRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    route = db.query(Route).filter(Route.id == route_id).first()
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")

    for order_idx, s_id in enumerate(data.stop_ids, start=1):
        db.query(Stop).filter(Stop.id == s_id, Stop.route_id == route_id).update({"stop_order": order_idx})

    db.commit()
    log_admin_activity(db, current_user["user_id"], "REORDER_STOPS", "route", str(route_id), f"Reordered {len(data.stop_ids)} stops for route {route.name}")
    stops = db.query(Stop).filter(Stop.route_id == route_id).order_by(Stop.stop_order.asc()).all()
    return {"message": "Stops reordered successfully", "stops": [admin_stop_payload(s, db) for s in stops]}


# ------------------------------------------------------------
# 6. ROUTE MANAGEMENT
# ------------------------------------------------------------

def admin_route_payload(route: Route, db: Session):
    stops = db.query(Stop).filter(Stop.route_id == route.id).order_by(Stop.stop_order.asc()).all()
    buses = db.query(Bus).filter(Bus.route_id == route.id).all()
    student_count = db.query(Student).filter(Student.bus_id.in_([b.id for b in buses])).count() if buses else 0

    return {
        "route_id": route.id,
        "name": route.name,
        "description": route.description,
        "stops_count": len(stops),
        "buses_count": len(buses),
        "student_count": student_count,
        "stops": [
            {
                "stop_id": s.id,
                "name": s.name,
                "latitude": s.latitude,
                "longitude": s.longitude,
                "stop_order": s.stop_order
            }
            for s in stops
        ],
        "buses": [
            {
                "bus_id": b.id,
                "bus_number": b.bus_number,
                "status": b.status
            }
            for b in buses
        ]
    }


@app.get("/admin/routes")
def list_admin_routes(db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    routes = db.query(Route).order_by(Route.name.asc()).all()
    return {"routes": [admin_route_payload(r, db) for r in routes]}


@app.get("/admin/routes/{route_id}")
def get_admin_route_details(route_id: int, db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    route = db.query(Route).filter(Route.id == route_id).first()
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")
    return admin_route_payload(route, db)


@app.post("/admin/routes", status_code=201)
def admin_create_route(
    data: AdminRouteCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    route = Route(**data.model_dump())
    db.add(route)
    db.commit()
    db.refresh(route)
    log_admin_activity(db, current_user["user_id"], "CREATE_ROUTE", "route", str(route.id), f"Created route {route.name}")
    return admin_route_payload(route, db)


@app.patch("/admin/routes/{route_id}")
def admin_update_route(
    route_id: int,
    data: AdminRouteUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    route = db.query(Route).filter(Route.id == route_id).first()
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")
    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(route, field, value)
    db.commit()
    db.refresh(route)
    log_admin_activity(db, current_user["user_id"], "UPDATE_ROUTE", "route", str(route.id), f"Updated route {route.name}")
    return admin_route_payload(route, db)


@app.delete("/admin/routes/{route_id}")
def admin_delete_route(route_id: int, db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    route = db.query(Route).filter(Route.id == route_id).first()
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")

    # Unassign from buses
    db.query(Bus).filter(Bus.route_id == route.id).update({"route_id": None})
    # Delete stops
    db.query(Stop).filter(Stop.route_id == route.id).delete()
    name = route.name
    db.delete(route)
    db.commit()
    log_admin_activity(db, current_user["user_id"], "DELETE_ROUTE", "route", str(route_id), f"Deleted route {name}")
    return {"message": f"Route {name} deleted successfully", "route_id": route_id}


# ------------------------------------------------------------
# 7. LIVE FLEET MONITORING
# ------------------------------------------------------------

COLLEGE_COORDINATES = {
    "latitude": 18.054145359568437,
    "longitude": 79.53558731724873,
    "name": "KITS Warangal (College)",
    "geofence_radius": 300
}

@app.get("/admin/live-tracking")
def get_admin_live_tracking(db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    buses = db.query(Bus).all()
    result = []
    for bus in buses:
        payload = admin_bus_payload(bus, db)
        # Add stops along bus route
        if bus.route_id:
            stops = db.query(Stop).filter(Stop.route_id == bus.route_id).order_by(Stop.stop_order.asc()).all()
            payload["route_stops"] = [
                {
                    "stop_id": s.id,
                    "name": s.name,
                    "latitude": s.latitude,
                    "longitude": s.longitude,
                    "stop_order": s.stop_order
                }
                for s in stops
            ]
        else:
            payload["route_stops"] = []
        result.append(payload)

    return {
        "college": COLLEGE_COORDINATES,
        "buses": result
    }


# ------------------------------------------------------------
# 8. ANNOUNCEMENT & NOTIFICATION CENTER
# ------------------------------------------------------------

def get_affected_students(target_type: str, target_id: int | None, db: Session):
    if target_type == "all" or target_id is None:
        return db.query(Student).all()
    elif target_type == "bus":
        return db.query(Student).filter(Student.bus_id == target_id).all()
    elif target_type == "route":
        buses = db.query(Bus).filter(Bus.route_id == target_id).all()
        bus_ids = [b.id for b in buses]
        return db.query(Student).filter(Student.bus_id.in_(bus_ids)).all() if bus_ids else []
    elif target_type == "stop":
        return db.query(Student).filter(Student.stop_id == target_id).all()
    return []


@app.post("/admin/notifications/calculate-recipients")
def calculate_announcement_recipients(
    data: AdminCalculateRecipientsRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    students = get_affected_students(data.target_type, data.target_id, db)
    return {
        "target_type": data.target_type,
        "target_id": data.target_id,
        "recipient_count": len(students),
        "sample_recipients": [s.roll_number for s in students[:5]]
    }


@app.post("/admin/notifications/broadcast", status_code=201)
def broadcast_admin_announcement(
    data: AdminBroadcastAnnouncementRequest,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    students = get_affected_students(data.target_type, data.target_id, db)
    recipients_count = len(students)

    for st in students:
        send_notification(
            db=db,
            user_id=st.user_id,
            title=data.title,
            message=data.message,
            notification_type="announcement",
            data={
                "template_type": data.template_type,
                "target_type": data.target_type,
                "target_id": data.target_id
            }
        )
        try:
            notification_manager.push_notification_sync(st.user_id, {
                "type": "announcement",
                "title": data.title,
                "message": data.message,
                "template_type": data.template_type
            })
        except Exception as ws_err:
            logger.warning(f"Announcement WS push failed for user {st.user_id}: {ws_err}")

    # Record announcement history
    history = AnnouncementHistory(
        sender_id=current_user["user_id"],
        template_type=data.template_type,
        title=data.title,
        message=data.message,
        target_type=data.target_type,
        target_id=data.target_id,
        recipient_count=recipients_count
    )
    db.add(history)
    db.commit()

    log_admin_activity(db, current_user["user_id"], "SEND_ANNOUNCEMENT", "announcement", str(history.id), f"Sent '{data.title}' to {recipients_count} students")
    return {
        "success": True,
        "message": f"Announcement broadcasted successfully to {recipients_count} students.",
        "recipient_count": recipients_count,
        "history_id": history.id
    }


@app.get("/admin/notifications/history")
def get_announcement_history(
    limit: int = 30,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    records = db.query(AnnouncementHistory).order_by(AnnouncementHistory.created_at.desc()).limit(limit).all()
    result = []
    for r in records:
        sender = db.query(User).filter(User.id == r.sender_id).first()
        result.append({
            "id": r.id,
            "template_type": r.template_type,
            "title": r.title,
            "message": r.message,
            "target_type": r.target_type,
            "target_id": r.target_id,
            "recipient_count": r.recipient_count,
            "sender_name": sender.name if sender else "Admin",
            "created_at": r.created_at.isoformat() + "Z" if r.created_at else None
        })
    return {"announcements": result}


# ------------------------------------------------------------
# 9. ALERT CENTER
# ------------------------------------------------------------

@app.get("/admin/alerts")
def get_admin_alerts(db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    alerts = db.query(Notification).filter(
        Notification.type.in_(["emergency_sos", "detour_alert", "driver_complaint_poll", "trip_started", "trip_ended"])
    ).order_by(Notification.created_at.desc()).limit(50).all()

    result = []
    for a in alerts:
        bus = db.query(Bus).filter(Bus.id == a.related_bus_id).first() if a.related_bus_id else None
        driver = db.query(Driver).filter(Driver.id == bus.driver_id).first() if bus and bus.driver_id else None
        driver_user = db.query(User).filter(User.id == driver.user_id).first() if driver else None
        result.append({
            "id": a.id,
            "type": a.type,
            "title": a.title,
            "message": a.message,
            "is_read": a.is_read,
            "bus_id": a.related_bus_id,
            "bus_number": bus.bus_number if bus else None,
            "driver_name": driver_user.name if driver_user else None,
            "driver_phone": driver_user.phone if driver_user else None,
            "created_at": a.created_at.isoformat() + "Z" if a.created_at else None
        })
    return {"alerts": result}


@app.post("/admin/alerts/{notification_id}/acknowledge")
def acknowledge_alert(notification_id: int, db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    notif = db.query(Notification).filter(Notification.id == notification_id).first()
    if not notif:
        raise HTTPException(status_code=404, detail="Alert not found")
    notif.is_read = 1
    db.commit()
    log_admin_activity(db, current_user["user_id"], "ACKNOWLEDGE_ALERT", "notification", str(notification_id), f"Acknowledged alert {notif.title}")
    return {"message": "Alert acknowledged", "notification_id": notification_id}


# ------------------------------------------------------------
# 10. TODAY'S OPERATIONS
# ------------------------------------------------------------

@app.get("/admin/today-operations")
def get_today_operations(db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    today = date.today()
    buses = db.query(Bus).all()

    # Bus operational breakdown
    active_buses = []
    offline_buses = []
    not_started_buses = []

    for bus in buses:
        payload = admin_bus_payload(bus, db)
        if payload["trip_status"] == "active":
            active_buses.append(payload)
        elif bus.status == "active":
            not_started_buses.append(payload)
        else:
            offline_buses.append(payload)

    # Student travelling breakdown
    total_students = db.query(Student).count()
    travel_records = db.query(TravelStatus).filter(TravelStatus.date == today).all()
    not_travelling_count = sum(1 for r in travel_records if r.status == "not_travelling")
    travelling_count = total_students - not_travelling_count

    # Trip breakdown
    today_start = datetime.combine(today, datetime.min.time())
    today_trips = db.query(Trip).filter(Trip.started_at >= today_start).all()
    active_trips_count = sum(1 for t in today_trips if t.status == "active")
    completed_trips_count = sum(1 for t in today_trips if t.status == "completed")

    return {
        "date": today.isoformat(),
        "buses_summary": {
            "total": len(buses),
            "active_now": len(active_buses),
            "not_started": len(not_started_buses),
            "offline": len(offline_buses)
        },
        "students_summary": {
            "total_registered": total_students,
            "travelling_today": travelling_count,
            "not_travelling_today": not_travelling_count
        },
        "trips_summary": {
            "active_now": active_trips_count,
            "completed_today": completed_trips_count
        },
        "active_buses": active_buses,
        "not_started_buses": not_started_buses,
        "offline_buses": offline_buses
    }


# ------------------------------------------------------------
# 11. GLOBAL SEARCH
# ------------------------------------------------------------

@app.get("/admin/search")
def admin_global_search(q: str, db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    query = q.strip().lower()
    if not query:
        return {"buses": [], "drivers": [], "students": [], "routes": [], "stops": []}

    # Search Buses
    buses = db.query(Bus).filter(Bus.bus_number.ilike(f"%{query}%")).all()

    # Search Drivers
    drivers = db.query(Driver).join(User, Driver.user_id == User.id).filter(
        (Driver.driver_code.ilike(f"%{query}%")) |
        (User.name.ilike(f"%{query}%")) |
        (User.phone.ilike(f"%{query}%"))
    ).all()

    # Search Students
    students = db.query(Student).join(User, Student.user_id == User.id).filter(
        (Student.roll_number.ilike(f"%{query}%")) |
        (User.name.ilike(f"%{query}%")) |
        (User.phone.ilike(f"%{query}%")) |
        (Student.department.ilike(f"%{query}%"))
    ).limit(30).all()

    # Search Routes
    routes = db.query(Route).filter(Route.name.ilike(f"%{query}%")).all()

    # Search Stops
    stops = db.query(Stop).filter(Stop.name.ilike(f"%{query}%")).all()

    return {
        "buses": [admin_bus_payload(b, db) for b in buses],
        "drivers": [admin_driver_payload(d, db) for d in drivers],
        "students": [admin_student_payload(s, db) for s in students],
        "routes": [admin_route_payload(r, db) for r in routes],
        "stops": [admin_stop_payload(s, db) for s in stops]
    }


# ------------------------------------------------------------
# 12. ADMIN ACTIVITY LOGS
# ------------------------------------------------------------

@app.get("/admin/activity-logs")
def get_admin_activity_logs(
    limit: int = 50,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    logs = db.query(AdminActivityLog).order_by(AdminActivityLog.created_at.desc()).limit(limit).all()
    result = []
    for l in logs:
        admin_user = db.query(User).filter(User.id == l.admin_user_id).first()
        result.append({
            "id": l.id,
            "action": l.action,
            "entity_type": l.entity_type,
            "entity_id": l.entity_id,
            "details": l.details,
            "admin_name": admin_user.name if admin_user else "Admin",
            "created_at": l.created_at.isoformat() + "Z" if l.created_at else None
        })
    return {"logs": result}


@app.post("/admin/create")
def create_admin(name: str, phone: str, password: str, db: Session = Depends(get_db)):
    existing_user = db.query(User).filter(User.phone == phone).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Phone number already registered")

    admin = User(name=name, phone=phone, password_hash=hash_password(password), role="admin")
    db.add(admin)
    db.commit()
    db.refresh(admin)

    return {
        "message": "Admin created successfully",
        "admin_id": admin.id,
        "name": admin.name,
        "phone": admin.phone,
        "role": admin.role
    }


@app.get("/driver/trip-status")
def get_driver_trip_status(
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver),
):
    driver = db.query(Driver).filter(Driver.user_id == current_driver["user_id"]).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")
    trip = (
        db.query(Trip)
        .filter(Trip.driver_id == driver.id, Trip.status == "active")
        .order_by(Trip.started_at.desc())
        .first()
    )
    return {
        "active": trip is not None,
        "trip_id": trip.id if trip else None,
        "bus_id": trip.bus_id if trip else None,
        "started_at": to_utc_iso(trip.started_at) if trip else None,
    }


@app.post("/driver/start-trip")
def start_driver_trip(
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver),
):
    driver = db.query(Driver).filter(Driver.user_id == current_driver["user_id"]).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")

    bus = db.query(Bus).filter(Bus.driver_id == driver.id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="No bus assigned to this driver")

    existing_trip = (
        db.query(Trip)
        .filter(Trip.driver_id == driver.id, Trip.status == "active")
        .first()
    )

    if existing_trip:
        return {
            "message": "Trip already active",
            "trip_id": existing_trip.id,
            "bus_id": existing_trip.bus_id,
            "route_id": existing_trip.route_id,
            "status": existing_trip.status,
        }

    trip = Trip(
        bus_id=bus.id,
        driver_id=driver.id,
        route_id=bus.route_id,
        status="active",
        wait_budget_total=10,
        wait_budget_used=0,
    )
    db.add(trip)
    db.commit()
    db.refresh(trip)

    for student in db.query(Student).filter(Student.bus_id == bus.id).all():
        send_notification(
            db,
            student.user_id,
            "Bus Trip Started",
            f"{bus.bus_number} has started its trip.",
            "trip_started",
            {"trip_id": trip.id, "bus_id": bus.id},
            related_bus_id=bus.id,
            related_trip_id=trip.id
        )

    send_notification(
        db,
        driver.user_id,
        "Trip Started",
        "Your trip is active and GPS sharing has started.",
        "trip_started",
        {"trip_id": trip.id},
        related_bus_id=bus.id,
        related_trip_id=trip.id
    )
    db.commit()

    return {
        "message": "Trip started successfully",
        "trip_id": trip.id,
        "bus_id": bus.id,
        "route_id": bus.route_id,
        "status": trip.status,
        "started_at": to_utc_iso(trip.started_at),
    }


@app.get("/student/my-bus")
def get_student_my_bus(
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    student = db.query(Student).filter(Student.user_id == current_student["user_id"]).first()
    if not student:
        raise HTTPException(status_code=404, detail="Student profile not found")

    if student.bus_id is None:
        raise HTTPException(status_code=404, detail="No bus assigned to this student")

    user = db.query(User).filter(User.id == student.user_id).first()
    regular_bus = db.query(Bus).filter(Bus.id == student.bus_id).first()

    # Check if student has an active alternative bus allotment for this journey
    active_allotment = get_active_missed_bus_allotment(db, student.id)
    if active_allotment:
        alt_bus = db.query(Bus).filter(Bus.id == active_allotment.alternative_bus_id).first()
        if alt_bus:
            alt_route = db.query(Route).filter(Route.id == alt_bus.route_id).first() if alt_bus.route_id else None
            alt_driver = db.query(Driver).filter(Driver.id == alt_bus.driver_id).first() if alt_bus.driver_id else None
            alt_driver_user = db.query(User).filter(User.id == alt_driver.user_id).first() if alt_driver else None
            alt_active_trip = (
                db.query(Trip)
                .filter(Trip.bus_id == alt_bus.id, Trip.status == "active")
                .order_by(Trip.started_at.desc())
                .first()
            )
            alt_location = (
                db.query(BusLocation)
                .filter(BusLocation.bus_id == alt_bus.id)
                .order_by(BusLocation.timestamp.desc())
                .first()
            )
            return {
                "student_id": student.id,
                "student_name": user.name if user else None,
                "roll_number": student.roll_number,
                "bus_id": alt_bus.id,
                "bus_number": alt_bus.bus_number,
                "route_id": alt_bus.route_id,
                "route_name": alt_route.name if alt_route else None,
                "driver_name": alt_driver_user.name if alt_driver_user else None,
                "driver_phone": alt_driver_user.phone if alt_driver_user else None,
                "registration_number": alt_bus.registration_number,
                "active_trip": alt_active_trip is not None,
                "alternative_bus": True,
                "allotment_id": active_allotment.id,
                "original_bus_id": active_allotment.original_bus_id,
                "original_bus_number": regular_bus.bus_number if regular_bus else None,
                "location": (
                    {
                        "latitude": alt_location.latitude,
                        "longitude": alt_location.longitude,
                        "speed": alt_location.speed,
                        "timestamp": to_utc_iso(alt_location.timestamp)
                    }
                    if alt_location else None
                )
            }

    if not regular_bus:
        raise HTTPException(status_code=404, detail="Assigned bus not found")

    route = db.query(Route).filter(Route.id == regular_bus.route_id).first() if regular_bus.route_id else None
    driver = db.query(Driver).filter(Driver.id == regular_bus.driver_id).first() if regular_bus.driver_id else None
    driver_user = db.query(User).filter(User.id == driver.user_id).first() if driver else None

    active_trip = (
        db.query(Trip)
        .filter(Trip.bus_id == regular_bus.id, Trip.status == "active")
        .order_by(Trip.started_at.desc())
        .first()
    )

    location = (
        db.query(BusLocation)
        .filter(BusLocation.bus_id == regular_bus.id)
        .order_by(BusLocation.timestamp.desc())
        .first()
    )

    return {
        "student_id": student.id,
        "student_name": user.name if user else None,
        "roll_number": student.roll_number,
        "bus_id": regular_bus.id,
        "bus_number": regular_bus.bus_number,
        "route_id": regular_bus.route_id,
        "route_name": route.name if route else None,
        "driver_name": driver_user.name if driver_user else None,
        "driver_phone": driver_user.phone if driver_user else None,
        "registration_number": regular_bus.registration_number,
        "active_trip": active_trip is not None,
        "alternative_bus": False,
        "location": (
            {
                "latitude": location.latitude,
                "longitude": location.longitude,
                "speed": location.speed,
                "timestamp": to_utc_iso(location.timestamp)
            }
            if location else None
        )
    }


@app.post("/driver/end-trip")
def end_driver_trip(
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver),
):
    driver = db.query(Driver).filter(Driver.user_id == current_driver["user_id"]).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")

    trip = (
        db.query(Trip)
        .filter(Trip.driver_id == driver.id, Trip.status == "active")
        .order_by(Trip.started_at.desc())
        .first()
    )

    if not trip:
        raise HTTPException(status_code=404, detail="No active trip found")

    trip.ended_at = datetime.utcnow()
    trip.status = "completed"
    db.commit()
    db.refresh(trip)

    bus = db.query(Bus).filter(Bus.id == trip.bus_id).first()

    for student in db.query(Student).filter(Student.bus_id == trip.bus_id).all():
        send_notification(
            db,
            student.user_id,
            "Trip Ended",
            f"{bus.bus_number if bus else 'Your bus'} has ended its trip.",
            "trip_ended",
            {"trip_id": trip.id, "bus_id": trip.bus_id},
            related_bus_id=trip.bus_id,
            related_trip_id=trip.id
        )

    send_notification(
        db,
        driver.user_id,
        "Trip Ended",
        "Your trip has ended and GPS sharing has stopped.",
        "trip_ended",
        {"trip_id": trip.id},
        related_bus_id=trip.bus_id,
        related_trip_id=trip.id
    )
    db.commit()

    return {
        "message": "Trip ended successfully",
        "trip_id": trip.id,
        "status": trip.status,
        "started_at": to_utc_iso(trip.started_at),
        "ended_at": to_utc_iso(trip.ended_at),
    }


@app.post("/driver/wait-request/{request_id}/skip")
def skip_wait_request(
    request_id: int,
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver)
):
    driver = db.query(Driver).filter(Driver.user_id == current_driver["user_id"]).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")

    bus = db.query(Bus).filter(Bus.driver_id == driver.id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="No bus assigned to this driver")

    trip = get_active_trip_for_bus(db, bus.id)
    if not trip:
        raise HTTPException(status_code=400, detail="No active trip")

    request = db.query(WaitRequest).filter(
        WaitRequest.id == request_id,
        WaitRequest.bus_id == bus.id,
        WaitRequest.trip_id == trip.id,
        WaitRequest.status == "pending"
    ).first()

    if not request:
        raise HTTPException(status_code=404, detail="Pending wait request not found")

    if request.stop_id is None:
        raise HTTPException(status_code=400, detail="Wait request has no assigned stop")

    grouped_requests = db.query(WaitRequest).filter(
        WaitRequest.trip_id == trip.id,
        WaitRequest.stop_id == request.stop_id,
        WaitRequest.status == "pending"
    ).all()

    if not grouped_requests:
        raise HTTPException(status_code=400, detail="No pending requests in this group")

    now = datetime.utcnow()
    cooldown_until = now + timedelta(minutes=WAIT_SKIP_COOLDOWN_MINUTES)
    stop = db.query(Stop).filter(Stop.id == request.stop_id).first()
    stop_name = stop.name if stop else "your stop"

    affected_students = []
    for grouped_request in grouped_requests:
        grouped_request.status = "rejected"
        grouped_request.skipped_at = now
        grouped_request.cooldown_until = cooldown_until

        student = db.query(Student).filter(Student.id == grouped_request.student_id).first()
        if student:
            affected_students.append(student.id)
            send_notification(
                db,
                student.user_id,
                "Wait Request Skipped",
                f"The driver skipped the wait request at {stop_name}. You may request again after the cooldown.",
                "wait_rejected",
                {
                    "wait_request_id": grouped_request.id,
                    "trip_id": trip.id,
                    "stop_id": request.stop_id,
                    "cooldown_until": to_utc_iso(cooldown_until)
                },
                related_bus_id=bus.id,
                related_trip_id=trip.id,
                related_wait_request_id=grouped_request.id
            )

    db.commit()

    return {
        "message": "Wait request group skipped",
        "trip_id": trip.id,
        "stop_id": request.stop_id,
        "stop_name": stop_name,
        "students_affected": len(affected_students),
        "status": "rejected",
        "cooldown_until": cooldown_until
    }


def reconcile_wait_requests(db: Session, trip: Trip):
    now = datetime.utcnow()
    pending_requests = (
        db.query(WaitRequest)
        .filter(
            WaitRequest.trip_id == trip.id,
            WaitRequest.bus_id == trip.bus_id,
            WaitRequest.status == "pending"
        )
        .order_by(WaitRequest.created_at.asc())
        .all()
    )

    if not pending_requests:
        return

    grouped = {}
    for request in pending_requests:
        if request.stop_id is None:
            continue
        key = (request.trip_id, request.stop_id)
        grouped.setdefault(key, []).append(request)

    changed = False
    for key, group in grouped.items():
        deadline = max((r.auto_accept_at for r in group if r.auto_accept_at), default=None)
        if deadline is not None and deadline > now:
            continue

        stop_id = key[1]
        stop = db.query(Stop).filter(Stop.id == stop_id).first()
        if not stop:
            continue

        if has_passed_stop(db, trip, stop):
            for request in group:
                request.status = "rejected"
                request.skipped_at = now
                request.cooldown_until = now + timedelta(minutes=WAIT_SKIP_COOLDOWN_MINUTES)
                student = db.query(Student).filter(Student.id == request.student_id).first()
                if student:
                    send_notification(
                        db,
                        student.user_id,
                        "Wait Request Cancelled",
                        "The bus has already passed your stop.",
                        "wait_rejected",
                        {"wait_request_id": request.id, "trip_id": trip.id, "stop_id": stop.id},
                        related_bus_id=trip.bus_id,
                        related_trip_id=trip.id,
                        related_wait_request_id=request.id
                    )
                changed = True
            continue

        requested_minutes = max(r.minutes for r in group)
        remaining_budget = trip.wait_budget_total - trip.wait_budget_used

        if remaining_budget <= 0 or requested_minutes > remaining_budget:
            for request in group:
                request.status = "rejected"
                student = db.query(Student).filter(Student.id == request.student_id).first()
                if student:
                    send_notification(
                        db,
                        student.user_id,
                        "Wait unavailable",
                        "The bus can't wait further today.",
                        "wait_budget_exhausted",
                        {"wait_request_id": request.id, "trip_id": trip.id, "stop_id": stop.id, "remaining_budget": max(0, remaining_budget)},
                        related_bus_id=trip.bus_id,
                        related_trip_id=trip.id,
                        related_wait_request_id=request.id
                    )
                changed = True
            continue

        trip.wait_budget_used += requested_minutes
        wait_until = now + timedelta(minutes=requested_minutes)

        for request in group:
            request.status = "accepted"
            request.wait_until = wait_until
            student = db.query(Student).filter(Student.id == request.student_id).first()
            if student:
                send_notification(
                    db,
                    student.user_id,
                    "Wait Request Accepted",
                    f"The bus will wait approximately {requested_minutes} minute{'s' if requested_minutes != 1 else ''} at {stop.name}.",
                    "wait_accepted",
                    {"wait_request_id": request.id, "trip_id": trip.id, "stop_id": request.stop_id, "minutes": requested_minutes, "wait_until": to_utc_iso(wait_until)},
                    related_bus_id=trip.bus_id,
                    related_trip_id=trip.id,
                    related_wait_request_id=request.id
                )
            changed = True

    if changed:
        db.commit()


# ============================================================
# DRIVER STUDENT PASS VERIFICATION
# ============================================================

@app.post("/driver/verify-pass")
def verify_student_pass(
    data: VerifyPassRequest,
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver)
):
    driver = db.query(Driver).filter(Driver.user_id == current_driver["user_id"]).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")

    bus = db.query(Bus).filter(Bus.driver_id == driver.id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="No bus assigned to this driver")

    query_str = (data.query or "").strip().upper()
    if not query_str:
        raise HTTPException(status_code=400, detail="Student roll number or QR code query is required")

    import re
    match = re.search(r"([0-9]{2}[A-Z]{3,4}[0-9]{3,4})", query_str, re.IGNORECASE)
    roll_number = match.group(1).upper() if match else query_str

    student = db.query(Student).filter(Student.roll_number.ilike(roll_number)).first()
    if not student:
        raise HTTPException(status_code=404, detail=f"No student record found with roll number: {roll_number}")

    user = db.query(User).filter(User.id == student.user_id).first()
    student_name = user.name if user else "Student"

    if student.bus_id != bus.id:
        assigned_bus = db.query(Bus).filter(Bus.id == student.bus_id).first() if student.bus_id else None
        bus_name = f"Bus {assigned_bus.bus_number}" if assigned_bus else "another bus"
        raise HTTPException(
            status_code=400,
            detail=f"Pass Invalid for this bus: {student_name} ({roll_number}) is assigned to {bus_name}, not Bus {bus.bus_number}."
        )

    is_travelling = student_is_travelling_today(db, student.id)
    stop = db.query(Stop).filter(Stop.id == student.stop_id).first() if student.stop_id else None
    stop_name = stop.name if stop else "Route Stop"

    active_trip = get_active_trip_for_bus(db, bus.id)
    try:
        entry_log = BusEntryLog(
            bus_id=bus.id,
            trip_id=active_trip.id if active_trip else 0,
            latitude=data.latitude or (stop.latitude if stop else 0.0),
            longitude=data.longitude or (stop.longitude if stop else 0.0)
        )
        db.add(entry_log)
        db.commit()
    except Exception as e:
        logger.warning(f"Failed to log bus entry: {e}")
        db.rollback()

    return {
        "valid": True,
        "student_id": student.id,
        "student_name": student_name,
        "roll_number": student.roll_number,
        "department": student.department or "General",
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "stop_id": stop.id if stop else None,
        "stop_name": stop_name,
        "travelling_today": is_travelling,
        "status": "active" if is_travelling else "warning_not_travelling",
        "message": "Student pass verified successfully" if is_travelling else "Student marked as Not Travelling today",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }


# ============================================================
# DRIVER ROUTE DETOUR REPORTING
# ============================================================

@app.post("/driver/report-detour")
def report_driver_detour(
    data: DriverDetourCreate,
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver)
):
    driver = db.query(Driver).filter(Driver.user_id == current_driver["user_id"]).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")

    bus = db.query(Bus).filter(Bus.driver_id == driver.id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="No bus assigned to this driver")

    active_trip = get_active_trip_for_bus(db, bus.id)
    reason = (data.reason or "Road Work").strip()
    delay = data.delay_minutes or 10
    message = f"Bus {bus.bus_number} has reported a route detour ({reason}). Estimated delay: ~{delay} mins."

    students = db.query(Student).filter(Student.bus_id == bus.id).all()
    for st in students:
        send_notification(
            db=db,
            user_id=st.user_id,
            title="⚠️ Route Detour Alert",
            message=message,
            notification_type="detour_alert",
            related_bus_id=bus.id,
            related_trip_id=active_trip.id if active_trip else None,
            payload=json.dumps({"reason": reason, "delay_minutes": delay, "bus_number": bus.bus_number})
        )
        try:
            notification_manager.push_notification_sync(st.user_id, {
                "type": "detour_alert",
                "title": "⚠️ Route Detour Alert",
                "message": message,
                "reason": reason,
                "delay_minutes": delay,
                "bus_id": bus.id,
                "bus_number": bus.bus_number
            })
        except Exception as ws_err:
            logger.warning(f"Detour WS push failed for user {st.user_id}: {ws_err}")

    admins = db.query(User).filter(User.role == "admin").all()
    for adm in admins:
        send_notification(
            db=db,
            user_id=adm.id,
            title=f"⚠️ Detour: Bus {bus.bus_number}",
            message=f"Driver reported detour ({reason}, ~{delay} min delay) for Bus {bus.bus_number}.",
            notification_type="detour_alert",
            related_bus_id=bus.id,
            related_trip_id=active_trip.id if active_trip else None,
            payload=json.dumps({"reason": reason, "delay_minutes": delay, "bus_number": bus.bus_number})
        )
        try:
            notification_manager.push_notification_sync(adm.id, {
                "type": "detour_alert",
                "title": f"⚠️ Detour: Bus {bus.bus_number}",
                "message": f"Driver reported detour ({reason}, ~{delay} min delay) for Bus {bus.bus_number}.",
                "reason": reason,
                "delay_minutes": delay,
                "bus_id": bus.id,
                "bus_number": bus.bus_number
            })
        except Exception as ws_err:
            logger.warning(f"Detour WS push failed for admin {adm.id}: {ws_err}")

    db.commit()
    return {
        "success": True,
        "message": f"Detour alert ({reason}, +{delay}m delay) broadcasted to {len(students)} students and admin.",
        "bus_id": bus.id,
        "reason": reason,
        "delay_minutes": delay
    }


# ============================================================
# DRIVER EMERGENCY SOS & BREAKDOWN
# ============================================================

@app.post("/driver/emergency-sos")
def report_driver_emergency_sos(
    data: DriverEmergencySosCreate,
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver)
):
    driver = db.query(Driver).filter(Driver.user_id == current_driver["user_id"]).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver profile not found")

    bus = db.query(Bus).filter(Bus.driver_id == driver.id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="No bus assigned to this driver")

    driver_user = db.query(User).filter(User.id == driver.user_id).first()
    driver_name = driver_user.name if driver_user else "Driver"
    driver_phone = driver_user.phone if driver_user else "Unknown"

    active_trip = get_active_trip_for_bus(db, bus.id)
    incident = (data.incident_type or "Emergency Breakdown").strip()

    location = get_latest_location(db, bus.id, active_trip.id if active_trip else None)
    loc_str = f"near coordinates ({data.latitude or (location.latitude if location else 'N/A')}, {data.longitude or (location.longitude if location else 'N/A')})"

    sos_msg = f"EMERGENCY SOS: Bus {bus.bus_number} ({driver_name}, Ph: {driver_phone}) reported '{incident}' {loc_str}. Immediate transport assistance requested."

    students = db.query(Student).filter(Student.bus_id == bus.id).all()
    for st in students:
        send_notification(
            db=db,
            user_id=st.user_id,
            title="🚨 Emergency SOS Alert",
            message=f"Bus {bus.bus_number} driver has reported an incident: {incident}. College transport authority is coordinating assistance.",
            notification_type="emergency_sos",
            related_bus_id=bus.id,
            related_trip_id=active_trip.id if active_trip else None,
            payload=json.dumps({"incident": incident, "bus_number": bus.bus_number})
        )
        try:
            notification_manager.push_notification_sync(st.user_id, {
                "type": "emergency_sos",
                "title": "🚨 Emergency SOS Alert",
                "message": f"Bus {bus.bus_number} reported: {incident}. Assistance is underway.",
                "incident": incident,
                "bus_id": bus.id
            })
        except Exception as ws_err:
            logger.warning(f"SOS WS push failed for student {st.user_id}: {ws_err}")

    admins = db.query(User).filter(User.role == "admin").all()
    for adm in admins:
        send_notification(
            db=db,
            user_id=adm.id,
            title=f"🚨 URGENT SOS: Bus {bus.bus_number}",
            message=sos_msg,
            notification_type="emergency_sos",
            related_bus_id=bus.id,
            related_trip_id=active_trip.id if active_trip else None,
            payload=json.dumps({
                "incident": incident,
                "bus_id": bus.id,
                "bus_number": bus.bus_number,
                "driver_name": driver_name,
                "driver_phone": driver_phone,
                "latitude": data.latitude or (location.latitude if location else None),
                "longitude": data.longitude or (location.longitude if location else None)
            })
        )
        try:
            notification_manager.push_notification_sync(adm.id, {
                "type": "emergency_sos",
                "title": f"🚨 URGENT SOS: Bus {bus.bus_number}",
                "message": sos_msg,
                "incident": incident,
                "bus_id": bus.id
            })
        except Exception as ws_err:
            logger.warning(f"SOS WS push failed for admin {adm.id}: {ws_err}")

    db.commit()
    return {
        "success": True,
        "message": f"Emergency SOS ({incident}) logged and transmitted to college administration and {len(students)} students.",
        "bus_id": bus.id,
        "incident": incident
    }