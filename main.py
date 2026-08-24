from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import inspect, text
from datetime import date,datetime
from math import asin, cos, radians, sin, sqrt
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
    DeviceToken
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
    AdminStopCreate
    ,DeviceTokenCreate
)
from notification_service import send_notification
import models
from auth import (
    hash_password,
    verify_password,
    create_access_token,
    get_current_user,
    require_driver,
    require_admin,
    require_student
)
app = FastAPI(title="KAMBUS API")

Base.metadata.create_all(bind=engine)

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

@app.get("/")
def root():
    return {
        "message": "KAMBUS Backend is running 🚍"
    }


@app.get("/health")
def health():
    try:
        with engine.connect():
            return {
                "status": "database connected"
            }
    except Exception as e:
        return {
            "status": "database connection failed",
            "error": str(e)
        }


def get_db():
    db = SessionLocal()

    try:
        yield db
    finally:
        db.close()


@app.post("/notifications/device-token")
def register_device_token(data: DeviceTokenCreate, db: Session = Depends(get_db), current_user: dict = Depends(get_current_user)):
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
def list_notifications(limit: int = 30, offset: int = 0, db: Session = Depends(get_db), current_user: dict = Depends(get_current_user)):
    limit = max(1, min(limit, 100)); offset = max(0, offset)
    query = db.query(Notification).filter(Notification.user_id == current_user["user_id"])
    notifications = query.order_by(Notification.created_at.desc()).offset(offset).limit(limit).all()
    return {"notifications": [{"id": n.id, "title": n.title, "message": n.message, "type": n.type, "is_read": bool(n.is_read), "created_at": n.created_at} for n in notifications], "unread_count": query.filter(Notification.is_read == 0).count()}


@app.patch("/notifications/{notification_id}/read")
def mark_notification_read(notification_id: int, db: Session = Depends(get_db), current_user: dict = Depends(get_current_user)):
    notification = db.query(Notification).filter(Notification.id == notification_id, Notification.user_id == current_user["user_id"]).first()
    if not notification: raise HTTPException(status_code=404, detail="Notification not found")
    notification.is_read = 1; db.commit()
    return {"message": "Notification marked as read", "notification_id": notification.id}


@app.patch("/notifications/read-all")
def mark_all_notifications_read(db: Session = Depends(get_db), current_user: dict = Depends(get_current_user)):
    db.query(Notification).filter(Notification.user_id == current_user["user_id"], Notification.is_read == 0).update({Notification.is_read: 1})
    db.commit(); return {"message": "Notifications marked as read"}
@app.post("/auth/login", response_model=LoginResponse)
def login(
    data: LoginRequest,
    db: Session = Depends(get_db)
):

    user = None

    # =========================
    # STUDENT LOGIN
    # =========================

    if data.role == "student":

        student = (
            db.query(Student)
            .filter(Student.roll_number == data.identifier)
            .first()
        )

        if student:
            user = (
                db.query(User)
                .filter(User.id == student.user_id)
                .first()
            )


    # =========================
    # DRIVER LOGIN
    # =========================


    elif data.role == "driver":
     driver = (
        db.query(Driver)
        .filter(Driver.driver_code == data.identifier)
        .first()
            )

     if driver:
        user = (
            db.query(User)
            .filter(User.id == driver.user_id)
            .first()
        )
    # =========================
    # ADMIN LOGIN
    # =========================

    elif data.role == "admin":

        try:
            admin_id = int(data.identifier)

        except ValueError:
            raise HTTPException(
                status_code=401,
                detail="Invalid admin ID or password"
            )

        user = (
            db.query(User)
            .filter(
                User.id == admin_id,
                User.role == "admin"
            )
            .first()
        )


    else:

        raise HTTPException(
            status_code=400,
            detail="Invalid role"
        )


    # =========================
    # USER NOT FOUND
    # =========================

    if not user:

        raise HTTPException(
            status_code=401,
            detail="Invalid ID or password"
        )


    # =========================
    # PASSWORD CHECK
    # =========================

    if not verify_password(
        data.password,
        user.password_hash
    ):

        raise HTTPException(
            status_code=401,
            detail="Invalid ID or password"
        )


    # =========================
    # CREATE JWT
    # =========================

    token = create_access_token(
        user.id,
        user.role
    )


    return {
        "access_token": token,
        "token_type": "bearer",
        "role": user.role,
        "user_id": user.id
    }
@app.post("/routes")
def create_route(
    data: RouteCreate,
    db: Session = Depends(get_db)
):
    route = Route(
        name=data.name,
        description=data.description
    )

    db.add(route)
    db.commit()
    db.refresh(route)

    return {
        "message": "Route created successfully",
        "route_id": route.id,
        "name": route.name
    }


@app.post("/buses")
def create_bus(
    data: BusCreate,
    db: Session = Depends(get_db)
):
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

    return {
        "message": "Bus created successfully",
        "bus_id": bus.id,
        "bus_number": bus.bus_number
    }


@app.post("/routes/{route_id}/stops")
def create_stop(
    route_id: int,
    data: StopCreate,
    db: Session = Depends(get_db)
):
    route = (
        db.query(Route)
        .filter(Route.id == route_id)
        .first()
    )

    if not route:
        raise HTTPException(
            status_code=404,
            detail="Route not found"
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

    return {
        "message": "Stop created successfully",
        "stop_id": stop.id,
        "route_id": route_id,
        "name": stop.name
    }


@app.post("/students")
def create_student(
    data: StudentCreate,
    db: Session = Depends(get_db)
):
    existing_user = (
        db.query(User)
        .filter(User.phone == data.phone)
        .first()
    )

    if existing_user:
        raise HTTPException(
            status_code=400,
            detail="Phone number already registered"
        )

    existing_student = (
        db.query(Student)
        .filter(Student.roll_number == data.roll_number)
        .first()
    )

    if existing_student:
        raise HTTPException(
            status_code=400,
            detail="Roll number already registered"
        )

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
    # Find driver using JWT
    driver = (
        db.query(Driver)
        .filter(
            Driver.user_id == current_driver["user_id"]
        )
        .first()
    )

    if not driver:
        raise HTTPException(
            status_code=404,
            detail="Driver profile not found"
        )

    # Find driver's assigned bus
    bus = (
        db.query(Bus)
        .filter(
            Bus.id == bus_id
        )
        .first()
    )

    if not bus:
        raise HTTPException(
            status_code=404,
            detail="Bus not found"
        )

    # Security check
    if bus.driver_id != driver.id:
        raise HTTPException(
            status_code=403,
            detail="You are not assigned to this bus"
        )

    active_trip = (
        db.query(Trip)
        .filter(Trip.driver_id == driver.id, Trip.bus_id == bus.id, Trip.status == "active")
        .order_by(Trip.started_at.desc())
        .first()
    )
    if not active_trip:
        raise HTTPException(status_code=409, detail="Start a trip before publishing location")

    # Save GPS location
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

    # Notify once when a bus enters a student's stop approach zone during a trip.
    for student in db.query(Student).filter(Student.bus_id == bus.id, Student.stop_id.isnot(None)).all():
        stop = db.query(Stop).filter(Stop.id == student.stop_id).first()
        if not stop:
            continue
        lat_delta, lng_delta = radians(stop.latitude - location.latitude), radians(stop.longitude - location.longitude)
        a = sin(lat_delta / 2) ** 2 + cos(radians(location.latitude)) * cos(radians(stop.latitude)) * sin(lng_delta / 2) ** 2
        metres = 6371000 * 2 * asin(sqrt(a))
        already_notified = db.query(Notification).filter(Notification.user_id == student.user_id, Notification.type == "bus_approaching", Notification.related_trip_id == active_trip.id, Notification.message.like(f"%{stop.name}%")).first()
        if metres <= 150 and not already_notified:
            send_notification(db, student.user_id, "Bus Approaching", f"{bus.bus_number} is approaching your stop, {stop.name}.", "bus_approaching", {"stop_id": stop.id, "trip_id": active_trip.id}, related_bus_id=bus.id, related_trip_id=active_trip.id)
    db.commit()

    return {
        "message": "Bus location updated",
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "driver_code": driver.driver_code,
        "latitude": location.latitude,
        "longitude": location.longitude,
        "speed": location.speed,
        "location_id": location.id
    }

@app.get("/buses/{bus_id}/location")
def get_bus_location(
    bus_id: int,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    # Students may read only the live location of their assigned bus. Drivers
    # may read only their own bus; admins retain operational visibility.
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
    location = (
        db.query(BusLocation)
        .filter(BusLocation.bus_id == bus_id)
        .order_by(BusLocation.timestamp.desc())
        .first()
    )

    if not location:
        raise HTTPException(
            status_code=404,
            detail="No location found for this bus"
        )

    return {
        "bus_id": bus_id,
        "latitude": location.latitude,
        "longitude": location.longitude,
        "speed": location.speed,
        "timestamp": location.timestamp
    }
@app.post("/drivers")
def create_driver(
    data: DriverCreate,
    db: Session = Depends(get_db)
):

    # Check Driver ID
    existing_driver = (
        db.query(Driver)
        .filter(Driver.driver_code == data.driver_code)
        .first()
    )

    if existing_driver:
        raise HTTPException(
            status_code=400,
            detail="Driver ID already registered"
        )

    # Create User
    user = User(
        name=data.name,
        phone=data.phone,
        password_hash=hash_password(data.password),
        role="driver"
    )

    db.add(user)
    db.commit()
    db.refresh(user)

    # Create Driver
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
def assign_driver(
    bus_id: int,
    data: AssignDriverRequest,
    db: Session = Depends(get_db)
):
    # Find bus
    bus = (
        db.query(Bus)
        .filter(Bus.id == bus_id)
        .first()
    )

    if not bus:
        raise HTTPException(
            status_code=404,
            detail="Bus not found"
        )

    # Find driver
    driver = (
        db.query(Driver)
        .filter(Driver.driver_code == data.driver_code)
        .first()
    )

    if not driver:
        raise HTTPException(
            status_code=404,
            detail="Driver not found"
        )

    # Check if driver already assigned to another bus
    existing_bus = (
        db.query(Bus)
        .filter(
            Bus.driver_id == driver.id,
            Bus.id != bus_id
        )
        .first()
    )

    if existing_bus:
        raise HTTPException(
            status_code=400,
            detail="Driver already assigned to another bus"
        )

    # Assign driver
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
    # 1. JWT se driver ka user_id
    user_id = current_driver["user_id"]

    # 2. User ID se driver find karo
    driver = (
        db.query(Driver)
        .filter(Driver.user_id == user_id)
        .first()
    )

    if not driver:
        raise HTTPException(
            status_code=404,
            detail="Driver profile not found"
        )

    # 3. Driver ki assigned bus find karo
    bus = (
        db.query(Bus)
        .filter(Bus.driver_id == driver.id)
        .first()
    )

    if not bus:
        raise HTTPException(
            status_code=404,
            detail="No bus assigned to this driver"
        )

    # 4. Bus ke paas route hona zaroori hai
    if bus.route_id is None:
        raise HTTPException(
            status_code=400,
            detail="Bus is not assigned to a route"
        )

    # 5. Route ke last stop ko find karo
    last_stop = (
        db.query(Stop)
        .filter(Stop.route_id == bus.route_id)
        .order_by(Stop.stop_order.desc())
        .first()
    )

    # 6. Next stop number
    if last_stop:
        next_stop_order = last_stop.stop_order + 1
    else:
        next_stop_order = 1

    # 7. Automatically create Stop 1, Stop 2, Stop 3...
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

    # 8. Response
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
    driver = (
        db.query(Driver)
        .filter(Driver.user_id == current_driver["user_id"])
        .first()
    )

    if not driver:
        raise HTTPException(
            status_code=404,
            detail="Driver profile not found"
        )

    user = (
        db.query(User)
        .filter(User.id == driver.user_id)
        .first()
    )

    bus = (
        db.query(Bus)
        .filter(Bus.driver_id == driver.id)
        .first()
    )

    if not bus:
        raise HTTPException(
            status_code=404,
            detail="No bus assigned to this driver"
        )

    route = (
        db.query(Route)
        .filter(Route.id == bus.route_id)
        .first()
    )

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
    student = (
        db.query(Student)
        .filter(Student.user_id == current_student["user_id"])
        .first()
    )

    if not student:
        raise HTTPException(
            status_code=404,
            detail="Student profile not found"
        )

    if student.bus_id is None:
        raise HTTPException(
            status_code=404,
            detail="No bus assigned to this student"
        )

    bus = (
        db.query(Bus)
        .filter(Bus.id == student.bus_id)
        .first()
    )

    if not bus:
        raise HTTPException(
            status_code=404,
            detail="Assigned bus not found"
        )

    if bus.route_id is None:
        raise HTTPException(
            status_code=404,
            detail="No route assigned to this bus"
        )

    stops = (
        db.query(Stop)
        .filter(Stop.route_id == bus.route_id)
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
@app.get("/student/my-stop")
def get_student_my_stop(
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    """Return the authenticated student's assigned stop, never all route stops."""
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


@app.post("/student/wait-request")
def create_wait_request(
    data: WaitRequestCreate,
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    if data.minutes <= 0:
        raise HTTPException(
            status_code=400,
            detail="Wait time must be greater than 0"
        )

    student = (
        db.query(Student)
        .filter(
            Student.user_id == current_student["user_id"]
        )
        .first()
    )

    if not student:
        raise HTTPException(
            status_code=404,
            detail="Student profile not found"
        )

    if student.bus_id is None:
        raise HTTPException(
            status_code=400,
            detail="No bus assigned to this student"
        )

    # Prevent multiple pending requests
    existing_request = (
        db.query(WaitRequest)
        .filter(
            WaitRequest.student_id == student.id,
            WaitRequest.bus_id == student.bus_id,
            WaitRequest.status == "pending"
        )
        .first()
    )

    if existing_request:
        raise HTTPException(
            status_code=400,
            detail="You already have a pending wait request"
        )

    wait_request = WaitRequest(
        student_id=student.id,
        bus_id=student.bus_id,
        minutes=data.minutes,
        status="pending"
    )

    db.add(wait_request)
    db.commit()
    db.refresh(wait_request)
    bus = db.query(Bus).filter(Bus.id == student.bus_id).first()
    driver = db.query(Driver).filter(Driver.id == bus.driver_id).first() if bus and bus.driver_id else None
    if driver:
        send_notification(db, driver.user_id, "New Wait Request", f"A student requested a {wait_request.minutes} minute wait.", "wait_request", {"wait_request_id": wait_request.id}, related_bus_id=student.bus_id, related_wait_request_id=wait_request.id)
        db.commit()

    return {
        "message": "Wait request sent successfully",
        "request_id": wait_request.id,
        "student_id": student.id,
        "bus_id": student.bus_id,
        "minutes": wait_request.minutes,
        "status": wait_request.status
    }
@app.get("/student/wait-request/status")
def get_wait_request_status(
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    student = (
        db.query(Student)
        .filter(
            Student.user_id == current_student["user_id"]
        )
        .first()
    )

    if not student:
        raise HTTPException(
            status_code=404,
            detail="Student profile not found"
        )

    wait_request = (
        db.query(WaitRequest)
        .filter(
            WaitRequest.student_id == student.id
        )
        .order_by(WaitRequest.created_at.desc())
        .first()
    )

    if not wait_request:
        raise HTTPException(
            status_code=404,
            detail="No wait request found"
        )

    return {
        "request_id": wait_request.id,
        "minutes": wait_request.minutes,
        "status": wait_request.status,
        "created_at": wait_request.created_at
    }
@app.get("/driver/wait-requests")
def get_driver_wait_requests(
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver)
):
    driver = (
        db.query(Driver)
        .filter(Driver.user_id == current_driver["user_id"])
        .first()
    )

    if not driver:
        raise HTTPException(
            status_code=404,
            detail="Driver profile not found"
        )

    bus = (
        db.query(Bus)
        .filter(Bus.driver_id == driver.id)
        .first()
    )

    if not bus:
        raise HTTPException(
            status_code=404,
            detail="No bus assigned to this driver"
        )

    requests = (
        db.query(WaitRequest)
        .filter(
            WaitRequest.bus_id == bus.id,
            WaitRequest.status == "pending"
        )
        .order_by(WaitRequest.created_at.asc())
        .all()
    )

    result = []

    for request in requests:

        student = (
            db.query(Student)
            .filter(Student.id == request.student_id)
            .first()
        )

        result.append({
            "request_id": request.id,
            "student_id": request.student_id,
            "roll_number": student.roll_number if student else None,
            "minutes": request.minutes,
            "status": request.status,
            "created_at": request.created_at
        })

    return {
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "requests": result
    }
@app.post("/driver/wait-request/{request_id}/accept")
def accept_wait_request(
    request_id: int,
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver)
):
    driver = (
        db.query(Driver)
        .filter(Driver.user_id == current_driver["user_id"])
        .first()
    )

    if not driver:
        raise HTTPException(
            status_code=404,
            detail="Driver profile not found"
        )

    bus = (
        db.query(Bus)
        .filter(Bus.driver_id == driver.id)
        .first()
    )

    if not bus:
        raise HTTPException(
            status_code=404,
            detail="No bus assigned to this driver"
        )

    request = (
        db.query(WaitRequest)
        .filter(
            WaitRequest.id == request_id,
            WaitRequest.bus_id == bus.id
        )
        .first()
    )

    if not request:
        raise HTTPException(
            status_code=404,
            detail="Wait request not found"
        )

    if request.status != "pending":
        raise HTTPException(
            status_code=400,
            detail="Request is no longer pending"
        )

    request.status = "accepted"

    db.commit()
    db.refresh(request)
    student = db.query(Student).filter(Student.id == request.student_id).first()
    if student:
        send_notification(db, student.user_id, "Wait Request Accepted", "Your driver has accepted your wait request.", "wait_accepted", {"wait_request_id": request.id}, related_bus_id=request.bus_id, related_wait_request_id=request.id)
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
    driver = (
        db.query(Driver)
        .filter(Driver.user_id == current_driver["user_id"])
        .first()
    )

    if not driver:
        raise HTTPException(
            status_code=404,
            detail="Driver profile not found"
        )

    bus = (
        db.query(Bus)
        .filter(Bus.driver_id == driver.id)
        .first()
    )

    if not bus:
        raise HTTPException(
            status_code=404,
            detail="No bus assigned to this driver"
        )

    request = (
        db.query(WaitRequest)
        .filter(
            WaitRequest.id == request_id,
            WaitRequest.bus_id == bus.id
        )
        .first()
    )

    if not request:
        raise HTTPException(
            status_code=404,
            detail="Wait request not found"
        )

    if request.status != "pending":
        raise HTTPException(
            status_code=400,
            detail="Request is no longer pending"
        )

    request.status = "rejected"

    db.commit()
    db.refresh(request)
    student = db.query(Student).filter(Student.id == request.student_id).first()
    if student:
        send_notification(db, student.user_id, "Wait Request Rejected", "Your driver could not accept your wait request.", "wait_rejected", {"wait_request_id": request.id}, related_bus_id=request.bus_id, related_wait_request_id=request.id)
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
    driver = (
        db.query(Driver)
        .filter(Driver.user_id == current_driver["user_id"])
        .first()
    )

    if not driver:
        raise HTTPException(
            status_code=404,
            detail="Driver profile not found"
        )

    bus = (
        db.query(Bus)
        .filter(Bus.driver_id == driver.id)
        .first()
    )

    if not bus:
        raise HTTPException(
            status_code=404,
            detail="No bus assigned to this driver"
        )

    if bus.route_id is None:
        raise HTTPException(
            status_code=400,
            detail="No route assigned to this bus"
        )

    stops = (
        db.query(Stop)
        .filter(Stop.route_id == bus.route_id)
        .order_by(Stop.stop_order.asc())
        .all()
    )

    return {
        "bus_id": bus.id,
        "bus_number": bus.bus_number,
        "route_id": bus.route_id,
        "total_stops": len(stops),
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
@app.post("/student/travel-status")
def update_travel_status(
    data: TravelStatusCreate,
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    student = (
        db.query(Student)
        .filter(
            Student.user_id == current_student["user_id"]
        )
        .first()
    )

    if not student:
        raise HTTPException(
            status_code=404,
            detail="Student profile not found"
        )

    allowed_statuses = [
        "travelling",
        "not_travelling"
    ]

    if data.status not in allowed_statuses:
        raise HTTPException(
            status_code=400,
            detail="Invalid travel status"
        )

    today = date.today()

    travel_status = (
        db.query(TravelStatus)
        .filter(
            TravelStatus.student_id == student.id,
            TravelStatus.date == today
        )
        .first()
    )

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
    student = (
        db.query(Student)
        .filter(
            Student.user_id == current_student["user_id"]
        )
        .first()
    )

    if not student:
        raise HTTPException(
            status_code=404,
            detail="Student profile not found"
        )

    travel_status = (
        db.query(TravelStatus)
        .filter(
            TravelStatus.student_id == student.id,
            TravelStatus.date == date.today()
        )
        .first()
    )

    return {
        "student_id": student.id,
        "status": (
            travel_status.status
            if travel_status
            else "not_travelling"
        ),
        "date": date.today()
    }
@app.get("/admin/students")
def get_all_students(
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    students = db.query(Student).all()

    return {
        "students": [
            {
                "student_id": student.id,
                "user_id": student.user_id,
                "name": (
                    db.query(User).filter(User.id == student.user_id).first().name
                    if db.query(User).filter(User.id == student.user_id).first()
                    else None
                ),
                "roll_number": student.roll_number,
                "department": student.department,
                "bus_id": student.bus_id,
                "bus_number": (
                    db.query(Bus).filter(Bus.id == student.bus_id).first().bus_number
                    if student.bus_id and db.query(Bus).filter(Bus.id == student.bus_id).first()
                    else None
                )
            }
            for student in students
        ]
    }
@app.post("/admin/create")
def create_admin(
    name: str,
    phone: str,
    password: str,
    db: Session = Depends(get_db)
):
    existing_user = (
        db.query(User)
        .filter(User.phone == phone)
        .first()
    )

    if existing_user:
        raise HTTPException(
            status_code=400,
            detail="Phone number already registered"
        )

    admin = User(
        name=name,
        phone=phone,
        password_hash=hash_password(password),
        role="admin"
    )

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


# =========================
# ADMIN MANAGEMENT
# =========================

def admin_bus_payload(bus: Bus, db: Session):
    route = db.query(Route).filter(Route.id == bus.route_id).first() if bus.route_id else None
    driver = db.query(Driver).filter(Driver.id == bus.driver_id).first() if bus.driver_id else None
    driver_user = db.query(User).filter(User.id == driver.user_id).first() if driver else None
    location = (
        db.query(BusLocation)
        .filter(BusLocation.bus_id == bus.id)
        .order_by(BusLocation.timestamp.desc())
        .first()
    )

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
def get_admin_dashboard(
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
    buses = db.query(Bus).order_by(Bus.bus_number.asc()).all()
    students = db.query(Student).count()
    active_buses = [bus for bus in buses if bus.status == "active"]
    live_buses = [bus for bus in buses if db.query(BusLocation).filter(BusLocation.bus_id == bus.id).first()]

    return {
        "total_buses": len(buses),
        "registered_students": students,
        "active_buses": len(active_buses),
        "live_buses": len(live_buses),
        "system_status": "operational",
        "buses": [admin_bus_payload(bus, db) for bus in buses]
    }


@app.get("/admin/buses")
def list_admin_buses(
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
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

    validate_bus_links(
        db,
        changes.get("route_id", bus.route_id),
        changes.get("driver_id", bus.driver_id),
        bus.id
    )
    for field, value in changes.items():
        setattr(bus, field, value)

    db.commit()
    db.refresh(bus)
    return admin_bus_payload(bus, db)


@app.get("/admin/drivers")
def list_admin_drivers(
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
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
        raise HTTPException(status_code=404, detail="Student not found")
    if data.bus_id is not None and not db.query(Bus).filter(Bus.id == data.bus_id).first():
        raise HTTPException(status_code=404, detail="Bus not found")

    student.bus_id = data.bus_id
    # A stop belonging to the previous route is no longer a valid assignment.
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
        raise HTTPException(status_code=404, detail="Student not found")
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
def list_admin_routes(
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_admin)
):
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
    trip = (db.query(Trip).filter(Trip.driver_id == driver.id, Trip.status == "active")
            .order_by(Trip.started_at.desc()).first())
    return {
        "active": trip is not None,
        "trip_id": trip.id if trip else None,
        "bus_id": trip.bus_id if trip else None,
        "started_at": trip.started_at if trip else None,
    }


@app.post("/driver/start-trip")
def start_driver_trip(
    db: Session = Depends(get_db),
    current_driver: dict = Depends(require_driver),
):
    driver = (
        db.query(Driver)
        .filter(Driver.user_id == current_driver["user_id"])
        .first()
    )

    if not driver:
        raise HTTPException(
            status_code=404,
            detail="Driver profile not found"
        )

    bus = (
        db.query(Bus)
        .filter(Bus.driver_id == driver.id)
        .first()
    )

    if not bus:
        raise HTTPException(
            status_code=404,
            detail="No bus assigned to this driver"
        )

    existing_trip = (
        db.query(Trip)
        .filter(
            Trip.driver_id == driver.id,
            Trip.status == "active"
        )
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
    )

    db.add(trip)
    db.commit()
    db.refresh(trip)
    for student in db.query(Student).filter(Student.bus_id == bus.id).all():
        send_notification(db, student.user_id, "Bus Trip Started", f"{bus.bus_number} has started its trip.", "trip_started", {"trip_id": trip.id, "bus_id": bus.id}, related_bus_id=bus.id, related_trip_id=trip.id)
    send_notification(db, driver.user_id, "Trip Started", "Your trip is active and GPS sharing has started.", "trip_started", {"trip_id": trip.id}, related_bus_id=bus.id, related_trip_id=trip.id)
    db.commit()

    return {
        "message": "Trip started successfully",
        "trip_id": trip.id,
        "bus_id": bus.id,
        "route_id": bus.route_id,
        "status": trip.status,
        "started_at": trip.started_at,
    }
@app.get("/student/my-bus")
def get_student_my_bus(
    db: Session = Depends(get_db),
    current_student: dict = Depends(require_student)
):
    # Find student profile
    student = (
        db.query(Student)
        .filter(
            Student.user_id == current_student["user_id"]
        )
        .first()
    )

    if not student:
        raise HTTPException(
            status_code=404,
            detail="Student profile not found"
        )

    # Check assigned bus
    if student.bus_id is None:
        raise HTTPException(
            status_code=404,
            detail="No bus assigned to this student"
        )

    # Find bus
    bus = (
        db.query(Bus)
        .filter(Bus.id == student.bus_id)
        .first()
    )

    if not bus:
        raise HTTPException(
            status_code=404,
            detail="Assigned bus not found"
        )

    user = db.query(User).filter(
        User.id == student.user_id
    ).first()

    route = (
        db.query(Route)
        .filter(Route.id == bus.route_id)
        .first()
        if bus.route_id
        else None
    )

    driver = (
        db.query(Driver)
        .filter(Driver.id == bus.driver_id)
        .first()
        if bus.driver_id
        else None
    )

    driver_user = (
        db.query(User)
        .filter(User.id == driver.user_id)
        .first()
        if driver
        else None
    )

    # Check whether this bus currently has an active trip
    active_trip = (
        db.query(Trip)
        .filter(
            Trip.bus_id == bus.id,
            Trip.status == "active"
        )
        .order_by(
            Trip.started_at.desc()
        )
        .first()
    )

    # Latest GPS location
    location = (
        db.query(BusLocation)
        .filter(
            BusLocation.bus_id == bus.id
        )
        .order_by(
            BusLocation.timestamp.desc()
        )
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

        # NEW: tells frontend whether the bus currently has an active trip
        "active_trip": active_trip is not None,

        "location": (
            {
                "latitude": location.latitude,
                "longitude": location.longitude,
                "speed": location.speed,
                "timestamp": location.timestamp
            }
            if location else None
        )
    }