from datetime import date
from pydantic import BaseModel
class LoginRequest(BaseModel):
    identifier: str
    password: str
    role: str
class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    role: str
    user_id: int

class RouteCreate(BaseModel):
    name: str
    description: str | None = None

class BusCreate(BaseModel):
    bus_number: str
    route_id: int | None = None
    driver_id: int | None = None
    registration_number: str | None = None
    status: str = "active"


class StopCreate(BaseModel):
    name: str
    latitude: float
    longitude: float
    stop_order: int
class StudentCreate(BaseModel):
    name: str
    phone: str
    password: str
    roll_number: str
    department: str | None = None
    bus_id: int | None = None
    stop_id: int | None = None
class LocationCreate(BaseModel):
    latitude: float
    longitude: float
    speed: float | None = None
class DriverCreate(BaseModel):
    name: str
    driver_code: str
    phone: str
    password: str
    license_number: str | None = None
class AssignDriverRequest(BaseModel):
    driver_code: str
class DriverStopCreate(BaseModel):
    latitude: float
    longitude: float
class WaitRequestCreate(BaseModel):
    minutes: int
class TravelStatusCreate(BaseModel):
    status: str = "not_travelling"


class AdminBusCreate(BaseModel):
    bus_number: str
    registration_number: str | None = None
    route_id: int | None = None
    driver_id: int | None = None
    status: str = "active"


class AdminBusUpdate(BaseModel):
    bus_number: str | None = None
    registration_number: str | None = None
    route_id: int | None = None
    driver_id: int | None = None
    status: str | None = None


class AdminStudentBusUpdate(BaseModel):
    bus_id: int | None = None


class AdminStudentStopUpdate(BaseModel):
    stop_id: int | None = None


class AdminRouteCreate(BaseModel):
    name: str
    description: str | None = None


class AdminRouteUpdate(BaseModel):
    name: str | None = None
    description: str | None = None


class AdminStopCreate(BaseModel):
    name: str
    latitude: float
    longitude: float
    stop_order: int


class DeviceTokenCreate(BaseModel):
    token: str
    platform: str = "android"
class DriverComplaintCreate(BaseModel):
    reason: str
    description: str | None = None
class ComplaintVerificationCreate(BaseModel):
    complaint_id: int
    response: str
class VerifyPassRequest(BaseModel):
    query: str
    latitude: float | None = None
    longitude: float | None = None
class DriverDetourCreate(BaseModel):
    reason: str
    delay_minutes: int = 10
    notes: str | None = None
class DriverEmergencySosCreate(BaseModel):
    incident_type: str
    latitude: float | None = None
    longitude: float | None = None
    notes: str | None = None


class AdminDriverCreate(BaseModel):
    name: str
    phone: str
    password: str
    driver_code: str
    license_number: str | None = None


class AdminDriverUpdate(BaseModel):
    name: str | None = None
    phone: str | None = None
    driver_code: str | None = None
    license_number: str | None = None
    password: str | None = None


class AdminStudentCreate(BaseModel):
    name: str
    phone: str
    password: str
    roll_number: str
    department: str | None = None
    bus_id: int | None = None
    stop_id: int | None = None


class AdminStudentUpdate(BaseModel):
    name: str | None = None
    phone: str | None = None
    roll_number: str | None = None
    department: str | None = None
    bus_id: int | None = None
    stop_id: int | None = None
    password: str | None = None


class AdminStopUpdate(BaseModel):
    name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    stop_order: int | None = None
    route_id: int | None = None


class AdminReorderStopsRequest(BaseModel):
    stop_ids: list[int]


class AdminAssignBusDriverRequest(BaseModel):
    driver_id: int | None = None


class AdminAssignBusRouteRequest(BaseModel):
    route_id: int | None = None


class AdminCalculateRecipientsRequest(BaseModel):
    target_type: str  # "all", "bus", "route", "stop"
    target_id: int | None = None


class AdminBroadcastAnnouncementRequest(BaseModel):
    template_type: str
    title: str
    message: str
    target_type: str = "all"
    target_id: int | None = None


class StudentSignupRequest(BaseModel):
    name: str
    roll_number: str
    email: str
    phone: str
    password: str
    department: str | None = None


class StudentVerifyOtpRequest(BaseModel):
    email: str
    otp_code: str


class StudentResendOtpRequest(BaseModel):
    email: str


class StudentSelectStopRequest(BaseModel):
    stop_id: int

class TemporaryStopChangeCreate(BaseModel):
    temporary_stop_id: int
    start_date: date
    end_date: date


class MissedBusAllotmentRequest(BaseModel):
    latitude: float | None = None
    longitude: float | None = None
