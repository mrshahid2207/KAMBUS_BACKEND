from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, status
from sqlalchemy.orm import Session
from sqlalchemy import inspect, text
from datetime import datetime, timedelta, date
from math import asin, cos, radians, sin, sqrt, atan2
import json
import logging
import asyncio

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
    ComplaintVerification
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
    DeviceTokenCreate,
    DriverComplaintCreate,
    ComplaintVerificationCreate
)
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
    from database import engine
    from models import Trip

    Trip.__table__.create(bind=engine, checkfirst=True)

    inspector = inspect(engine)
    columns = {
        column["name"]
        for column in inspector.get_columns("bus_locations")
    }

    if "trip_id" not in columns:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    ALTER TABLE bus_locations
                    ADD COLUMN trip_id INTEGER
                    REFERENCES trips(id)
                    """
                )
            )

    student_columns = {column["name"] for column in inspector.get_columns("students")}
    if "stop_id" not in student_columns:
        with engine.begin() as connection:
            connection.execute(text("ALTER TABLE students ADD COLUMN stop_id INTEGER REFERENCES stops(id)"))
    Notification.__table__.create(bind=engine, checkfirst=True)
    DeviceToken.__table__.create(bind=engine, checkfirst=True)
    DriverComplaint.__table__.create(bind=engine, checkfirst=True)
    ComplaintVerification.__table__.create(bind=engine, checkfirst=True)


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

    return nearest_stop.stop_order > stop.stop_order


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

    token = create_access_token(user.id, user.role)

    return {
        "access_token": token,
        "token_type": "bearer",
        "role": user.role,
        "user_id": user.id
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

    stops = db.query(Stop).filter(Stop.route_id == bus.route_id).order_by(Stop.stop_order.asc()).all()

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

    bus = db.query(Bus).filter(Bus.id == student.bus_id).first()
    stop = db.query(Stop).filter(Stop.id == student.stop_id).first()
    if not bus or not stop:
        raise HTTPException(status_code=404, detail="Assigned stop or bus not found")
    if bus.route_id is None or stop.route_id != bus.route_id:
        raise HTTPException(status_code=409, detail="Assigned stop does not belong to the student's bus route")

    return {
        "stop_id": stop.id,
        "stop_name": stop.name,
        "latitude": stop.latitude,
        "longitude": stop.longitude,
        "stop_order": stop.stop_order,
        "route_id": stop.route_id,
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
    }


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

    if student.bus_id is None:
        raise HTTPException(status_code=400, detail="No bus assigned to this student")

    if student.stop_id is None:
        raise HTTPException(status_code=400, detail="No stop assigned to this student")

    if not student_is_travelling_today(db, student.id):
        raise HTTPException(status_code=400, detail="You marked yourself as not travelling today")

    trip = get_active_trip_for_bus(db, student.bus_id)
    if not trip:
        raise HTTPException(status_code=400, detail="No active bus trip right now")

    stop = db.query(Stop).filter(Stop.id == student.stop_id).first()
    if not stop:
        raise HTTPException(status_code=404, detail="Assigned stop not found")

    if has_passed_stop(db, trip, stop):
        raise HTTPException(status_code=400, detail="The bus has already passed your stop")

    eta_minutes = get_eta_minutes_to_stop(db, student.bus_id, trip.id, stop)
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
        bus_id=student.bus_id,
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

    bus = db.query(Bus).filter(Bus.id == student.bus_id).first()
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
            related_bus_id=student.bus_id,
            related_trip_id=trip.id,
            related_wait_request_id=wait_request.id
        )

    db.commit()

    return {
        "message": "Wait request created",
        "request_id": wait_request.id,
        "student_id": student.id,
        "bus_id": student.bus_id,
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

    stops = db.query(Stop).filter(Stop.route_id == bus.route_id).order_by(Stop.stop_order.asc()).all()

    # Query all students assigned to this driver's bus
    today = date.today()
    students = db.query(Student).filter(Student.bus_id == bus.id).all()
    student_ids = [s.id for s in students]

    # Find students who marked 'not_travelling' for today
    not_travelling_ids = set()
    if student_ids:
        not_travelling_rows = db.query(TravelStatus.student_id).filter(
            TravelStatus.student_id.in_(student_ids),
            TravelStatus.date == today,
            TravelStatus.status == "not_travelling"
        ).all()
        not_travelling_ids = {row[0] for row in not_travelling_rows}

    # Aggregate active students per stop and total for today
    stop_student_counts = {}
    total_expected_today = 0
    for s in students:
        if s.id not in not_travelling_ids:
            total_expected_today += 1
            if s.stop_id:
                stop_student_counts[s.stop_id] = stop_student_counts.get(s.stop_id, 0) + 1

    return {
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "route_id": bus.route_id,
        "total_stops": len(stops),
        "total_students_today": total_expected_today,
        "total_assigned_students": len(students),
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
                "student_count": stop_student_counts.get(stop.id, 0)
            }
            for stop in stops
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


@app.get("/admin/students")
def get_all_students(db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    students = db.query(Student).all()
    return {
        "students": [
            {
                "student_id": s.id,
                "user_id": s.user_id,
                "name": db.query(User).filter(User.id == s.user_id).first().name if db.query(User).filter(User.id == s.user_id).first() else None,
                "roll_number": s.roll_number,
                "department": s.department,
                "bus_id": s.bus_id,
                "bus_number": db.query(Bus).filter(Bus.id == s.bus_id).first().bus_number if s.bus_id and db.query(Bus).filter(Bus.id == s.bus_id).first() else None
            }
            for s in students
        ]
    }


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


def admin_bus_payload(bus: Bus, db: Session):
    route = db.query(Route).filter(Route.id == bus.route_id).first() if bus.route_id else None
    driver = db.query(Driver).filter(Driver.id == bus.driver_id).first() if bus.driver_id else None
    driver_user = db.query(User).filter(User.id == driver.user_id).first() if driver else None
    location = db.query(BusLocation).filter(BusLocation.bus_id == bus.id).order_by(BusLocation.timestamp.desc()).first()

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


@app.get("/admin/dashboard")
def get_admin_dashboard(db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    buses = db.query(Bus).order_by(Bus.bus_number.asc()).all()
    students = db.query(Student).count()
    active_buses = [b for b in buses if b.status == "active"]
    live_buses = [b for b in buses if db.query(BusLocation).filter(BusLocation.bus_id == b.id).first()]

    return {
        "total_buses": len(buses),
        "registered_students": students,
        "active_buses": len(active_buses),
        "live_buses": len(live_buses),
        "system_status": "operational",
        "buses": [admin_bus_payload(bus, db) for bus in buses]
    }


@app.get("/admin/buses")
def list_admin_buses(db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    buses = db.query(Bus).order_by(Bus.bus_number.asc()).all()
    return {"buses": [admin_bus_payload(bus, db) for bus in buses]}


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
    return admin_bus_payload(bus, db)


@app.get("/admin/drivers")
def list_admin_drivers(db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    drivers = db.query(Driver).order_by(Driver.driver_code.asc()).all()
    result = []
    for driver in drivers:
        user = db.query(User).filter(User.id == driver.user_id).first()
        bus = db.query(Bus).filter(Bus.driver_id == driver.id).first()
        result.append({
            "driver_id": driver.id,
            "driver_code": driver.driver_code,
            "license_number": driver.license_number,
            "name": user.name if user else None,
            "phone": user.phone if user else None,
            "bus_id": bus.id if bus else None,
            "bus_number": bus.bus_number if bus else None
        })
    return {"drivers": result}


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
    return {"message": "Student stop assignment updated", "student_id": student.id, "stop_id": student.stop_id}


@app.get("/admin/routes")
def list_admin_routes(db: Session = Depends(get_db), current_user: dict = Depends(require_admin)):
    routes = db.query(Route).order_by(Route.name.asc()).all()
    return {
        "routes": [
            {
                "route_id": route.id,
                "name": route.name,
                "description": route.description,
                "stops": [
                    {
                        "stop_id": stop.id,
                        "name": stop.name,
                        "latitude": stop.latitude,
                        "longitude": stop.longitude,
                        "stop_order": stop.stop_order
                    }
                    for stop in db.query(Stop).filter(Stop.route_id == route.id).order_by(Stop.stop_order.asc()).all()
                ]
            }
            for route in routes
        ]
    }


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
    return {"route_id": route.id, "name": route.name, "description": route.description}


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
    return {"route_id": route.id, "name": route.name, "description": route.description}


@app.post("/admin/routes/{route_id}/stops", status_code=201)
def admin_create_route_stop(
    route_id: int,
    data: AdminStopCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    if not db.query(Route).filter(Route.id == route_id).first():
        raise HTTPException(status_code=404, detail="Route not found")
    if db.query(Stop).filter(Stop.route_id == route_id, Stop.stop_order == data.stop_order).first():
        raise HTTPException(status_code=400, detail="Stop order already exists for this route")

    stop = Stop(route_id=route_id, **data.model_dump())
    db.add(stop)
    db.commit()
    db.refresh(stop)
    return {"stop_id": stop.id, "route_id": stop.route_id, "name": stop.name, "stop_order": stop.stop_order}


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

    bus = db.query(Bus).filter(Bus.id == student.bus_id).first()
    if not bus:
        raise HTTPException(status_code=404, detail="Assigned bus not found")

    user = db.query(User).filter(User.id == student.user_id).first()
    route = db.query(Route).filter(Route.id == bus.route_id).first() if bus.route_id else None
    driver = db.query(Driver).filter(Driver.id == bus.driver_id).first() if bus.driver_id else None
    driver_user = db.query(User).filter(User.id == driver.user_id).first() if driver else None

    active_trip = (
        db.query(Trip)
        .filter(Trip.bus_id == bus.id, Trip.status == "active")
        .order_by(Trip.started_at.desc())
        .first()
    )

    location = (
        db.query(BusLocation)
        .filter(BusLocation.bus_id == bus.id)
        .order_by(BusLocation.timestamp.desc())
        .first()
    )

    return {
        "student_id": student.id,
        "student_name": user.name if user else None,
        "roll_number": student.roll_number,
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "route_id": bus.route_id,
        "route_name": route.name if route else None,
        "driver_name": driver_user.name if driver_user else None,
        "driver_phone": driver_user.phone if driver_user else None,
        "registration_number": bus.registration_number,
        "active_trip": active_trip is not None,
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