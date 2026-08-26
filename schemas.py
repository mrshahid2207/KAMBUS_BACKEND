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