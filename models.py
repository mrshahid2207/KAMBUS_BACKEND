from datetime import datetime, date

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Date,
    ForeignKey,
)

from database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    phone = Column(String(20), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(20), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class Student(Base):
    __tablename__ = "students"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    roll_number = Column(String(50), unique=True, nullable=False)
    department = Column(String(100))
    bus_id = Column(Integer, ForeignKey("buses.id"), nullable=True)
    stop_id = Column(Integer, ForeignKey("stops.id"), nullable=True)


class Driver(Base):
    __tablename__ = "drivers"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    driver_code = Column(String(20), unique=True, nullable=False, index=True)
    license_number = Column(String(100))

class Route(Base):
    __tablename__ = "routes"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    description = Column(String(255))


class Bus(Base):
    __tablename__ = "buses"

    id = Column(Integer, primary_key=True, index=True)
    bus_number = Column(String(50), unique=True, nullable=False)
    route_id = Column(Integer, ForeignKey("routes.id"), nullable=True)
    driver_id = Column(Integer, ForeignKey("drivers.id"), nullable=True)
    registration_number = Column(String(50))
    status = Column(String(30), default="active")


class Stop(Base):
    __tablename__ = "stops"

    id = Column(Integer, primary_key=True, index=True)
    route_id = Column(Integer, ForeignKey("routes.id"), nullable=False)
    name = Column(String(100), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    stop_order = Column(Integer, nullable=False)

class Trip(Base):
    __tablename__ = "trips"

    id = Column(Integer, primary_key=True, index=True)

    bus_id = Column(Integer, ForeignKey("buses.id"), nullable=False)
    driver_id = Column(Integer, ForeignKey("drivers.id"), nullable=False)
    route_id = Column(Integer, ForeignKey("routes.id"), nullable=True)

    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    ended_at = Column(DateTime, nullable=True)

    status = Column(String(20), default="active", nullable=False)
    
class BusLocation(Base):
    __tablename__ = "bus_locations"

    id = Column(Integer, primary_key=True, index=True)
    bus_id = Column(Integer, ForeignKey("buses.id"), nullable=False)
    trip_id = Column(Integer, ForeignKey("trips.id"), nullable=True)

    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    speed = Column(Float, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)

class WaitRequest(Base):
    __tablename__ = "wait_requests"

    id = Column(Integer, primary_key=True, index=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    bus_id = Column(Integer, ForeignKey("buses.id"), nullable=False)
    minutes = Column(Integer, nullable=False)
    status = Column(String(20), default="pending")
    created_at = Column(DateTime, default=datetime.utcnow)


class TravelStatus(Base):
    __tablename__ = "travel_status"

    id = Column(Integer, primary_key=True, index=True)
    student_id = Column(Integer, ForeignKey("students.id"), nullable=False)
    date = Column(Date, default=date.today)
    status = Column(String(30), default="not_travelling")
    created_at = Column(DateTime, default=datetime.utcnow)


class Notification(Base):
    __tablename__ = "notifications"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    title = Column(String(160), nullable=False)
    message = Column(String(500), nullable=False)
    type = Column(String(40), nullable=False, default="system")
    is_read = Column(Integer, nullable=False, default=0)
    payload = Column(String, nullable=True)
    related_bus_id = Column(Integer, ForeignKey("buses.id"), nullable=True)
    related_trip_id = Column(Integer, ForeignKey("trips.id"), nullable=True)
    related_wait_request_id = Column(Integer, ForeignKey("wait_requests.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class DeviceToken(Base):
    __tablename__ = "device_tokens"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    token = Column(String(512), unique=True, nullable=False)
    platform = Column(String(30), nullable=False, default="android")
    is_active = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
driver_code = Column(
    String(20),
    unique=True,
    nullable=False,
    index=True
)
