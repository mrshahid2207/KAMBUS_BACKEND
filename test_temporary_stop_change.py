"""
Comprehensive Automated Test Suite for Temporary Stop Change Feature.
Includes Unit Tests, API Integration Tests, and Regression Tests.
"""
import pytest
from datetime import date, datetime, timedelta
import math

# Import module functions and models
from main import (
    app,
    ROUTE_MATCH_TOLERANCE_M,
    point_to_segment_distance_m,
    min_distance_to_route_m,
    is_point_on_route,
    get_effective_student_stop,
    get_active_missed_bus_allotment,
    _POLYLINE_CACHE,
)
from database import SessionLocal, Base, engine
from models import (
    User, Student, Driver, Route, Bus, Stop, Trip, BusLocation, TemporaryStopChange,
    MissedBusAllotment, Notification, TravelStatus
)
from auth import hash_password, create_access_token
from fastapi.testclient import TestClient

client = TestClient(app)


# =====================================================================
# 1. ROUTE MATCHING & PROXIMITY UNIT TESTS
# =====================================================================

def test_point_to_segment_distance_hand_computed():
    """
    Test point-to-segment distance against hand-computed expected values.
    
    Setup:
    A = (17.98000, 79.53000)
    B = (17.98000, 79.54000)  -- Segment runs along latitude 17.98deg E-W.
    
    1 degree of longitude at lat 17.98deg:
    cos(radians(17.98)) * 111,320m = 0.951185 * 111,320m = 105,885.8m
    So 0.01 deg lon = 1,058.86m.
    
    Point P = (17.98090, 79.53500) -- Midpoint along segment, offset North by 0.0009 deg lat.
    0.0009 deg lat * 111,320m/deg = 100.188m.
    
    Hand-computed perpendicular distance is ~100.19m.
    """
    a_lat, a_lon = 17.98000, 79.53000
    b_lat, b_lon = 17.98000, 79.54000
    p_lat, p_lon = 17.98090, 79.53500

    dist_m = point_to_segment_distance_m(p_lat, p_lon, a_lat, a_lon, b_lat, b_lon)
    
    # Expected: ~100.188 meters (within 2 meters tolerance)
    expected_m = 0.0009 * 111320.0
    assert abs(dist_m - expected_m) < 2.0, f"Expected ~{expected_m:.2f}m, got {dist_m:.2f}m"


def test_route_match_tolerance_boundary_conditions():
    """
    Explicit boundary condition testing for ROUTE_MATCH_TOLERANCE_M = 30.0m.
    
    - 15.0m   --> matches (<= 30.0m)
    - 30.00m  --> matches (explicitly testing exact boundary <= condition)
    - 30.01m  --> does NOT match (> 30.0m)
    - 40.0m   --> does NOT match (> 30.0m)
    - 50.0m   --> does NOT match (> 30.0m)
    """
    assert ROUTE_MATCH_TOLERANCE_M == 30.0

    a_lat, a_lon = 17.98000, 79.53000
    b_lat, b_lon = 17.98000, 79.54000
    # midpoint longitude = 79.53500

    # 1 deg lat = 111,320 meters
    delta_lat_15m = 15.0 / 111320.0
    delta_lat_30m = 30.0 / 111320.0
    delta_lat_30_01m = 30.01 / 111320.0
    delta_lat_40m = 40.0 / 111320.0
    delta_lat_50m = 50.0 / 111320.0

    # 15m offset (within 30m tolerance)
    dist_15m = point_to_segment_distance_m(17.98000 + delta_lat_15m, 79.53500, a_lat, a_lon, b_lat, b_lon)
    assert dist_15m <= ROUTE_MATCH_TOLERANCE_M, f"15m ({dist_15m:.2f}m) should match tolerance 30m"

    # Exactly 30.0m offset
    dist_30m = point_to_segment_distance_m(17.98000 + delta_lat_30m, 79.53500, a_lat, a_lon, b_lat, b_lon)
    assert round(dist_30m, 4) <= ROUTE_MATCH_TOLERANCE_M, f"Exactly 30.0m ({dist_30m:.2f}m) must satisfy <= {ROUTE_MATCH_TOLERANCE_M}"

    # 30.01m offset
    dist_30_01m = point_to_segment_distance_m(17.98000 + delta_lat_30_01m, 79.53500, a_lat, a_lon, b_lat, b_lon)
    assert dist_30_01m > ROUTE_MATCH_TOLERANCE_M, f"30.01m ({dist_30_01m:.2f}m) must exceed 30m tolerance"

    # 40m offset
    dist_40m = point_to_segment_distance_m(17.98000 + delta_lat_40m, 79.53500, a_lat, a_lon, b_lat, b_lon)
    assert dist_40m > ROUTE_MATCH_TOLERANCE_M, f"40m ({dist_40m:.2f}m) must exceed 30m tolerance"

    # 50m offset
    dist_50m = point_to_segment_distance_m(17.98000 + delta_lat_50m, 79.53500, a_lat, a_lon, b_lat, b_lon)
    assert dist_50m > ROUTE_MATCH_TOLERANCE_M, f"50m ({dist_50m:.2f}m) must exceed 30m tolerance"


def test_min_distance_to_route_polyline():
    """Test min_distance_to_route_m over multi-segment polyline with 30m tolerance."""
    polyline = [
        (17.98000, 79.53000),
        (17.98000, 79.54000),
        (17.99000, 79.54000),
    ]
    # Point near 2nd segment, within ~10.5m: 10m / 105885m/deg = ~0.000095 deg lon
    p_lat, p_lon = 17.98500, 79.54009
    dist_m = min_distance_to_route_m(p_lat, p_lon, polyline)
    assert dist_m < 15.0
    assert dist_m <= ROUTE_MATCH_TOLERANCE_M


# =====================================================================
# FIXTURES & SEED DATA FOR API INTEGRATION TESTS
# =====================================================================

@pytest.fixture
def db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def setup_test_environment(db_session):
    """
    Creates test data:
    - Route 101: Stops A1 (17.980, 79.530), A2 (17.980, 79.540) -> Bus 101
    - Route 102: Stops B1 (17.950, 79.500), B2 (17.950, 79.510) -> Bus 102
    - Route 103: Stops C1 (17.950, 79.505), C2 (17.950, 79.515) -> Bus 103
    - Student 1 assigned to Bus 101 & Stop A1
    - Admin user for approval tests
    """
    # Route IDs can be reused by SQLite between tests; never retain a cached
    # OSRM geometry for a previous fixture's route.
    _POLYLINE_CACHE.clear()
    # Clean up old test records if present
    from models import ComplaintVerification, DriverComplaint
    db_session.query(BusLocation).delete()
    db_session.query(Trip).delete()
    db_session.query(Notification).delete()
    db_session.query(TravelStatus).delete()
    db_session.query(ComplaintVerification).delete()
    db_session.query(DriverComplaint).delete()
    db_session.query(MissedBusAllotment).delete()
    db_session.query(TemporaryStopChange).filter(TemporaryStopChange.selected_address.like("%Test%")).delete()
    db_session.query(Student).filter(Student.roll_number.like("TEST%")).delete()
    db_session.query(Bus).filter(Bus.bus_number.like("TEST%")).delete()
    db_session.query(Driver).filter(Driver.driver_code.like("TEST%")).delete()
    db_session.query(Stop).filter(Stop.name.like("TEST%")).delete()
    db_session.query(Route).filter(Route.name.like("TEST%")).delete()
    db_session.query(User).filter(User.email.like("test_%@kambus.test")).delete()
    db_session.commit()

    # Create Users
    u_student = User(name="Test Student 1", email="test_student1@kambus.test", phone="9999900001", password_hash=hash_password("pass123"), role="student", is_verified=True)
    u_student2 = User(name="Test Student 2", email="test_student2@kambus.test", phone="9999900004", password_hash=hash_password("pass123"), role="student", is_verified=True)
    u_driver = User(name="Test Driver 1", email="test_driver1@kambus.test", phone="9999900003", password_hash=hash_password("pass123"), role="driver", is_verified=True)
    u_admin = User(name="Test Admin", email="test_admin@kambus.test", phone="9999900002", password_hash=hash_password("pass123"), role="admin", is_verified=True)
    u_super_admin = User(name="Test Super Admin", email="test_super_admin@kambus.test", phone="9999900005", password_hash=hash_password("pass123"), role="super_admin", is_verified=True)
    db_session.add_all([u_student, u_student2, u_driver, u_admin, u_super_admin])
    db_session.commit()

    # Create Driver
    driver1 = Driver(user_id=u_driver.id, driver_code="TEST_DRV_1", license_number="LIC123")
    db_session.add(driver1)
    db_session.commit()

    # Create Routes
    r1 = Route(name="TEST Route 101", description="Route 101")
    r2 = Route(name="TEST Route 102", description="Route 102")
    r3 = Route(name="TEST Route 103", description="Route 103")
    r4 = Route(name="TEST Route 104", description="Route 104")
    db_session.add_all([r1, r2, r3, r4])
    db_session.commit()

    # Create Stops
    stop_a1 = Stop(route_id=r1.id, name="TEST Stop A1", latitude=17.98000, longitude=79.53000, stop_order=1, is_active=True)
    stop_a2 = Stop(route_id=r1.id, name="TEST Stop A2", latitude=17.98000, longitude=79.54000, stop_order=2, is_active=True)
    stop_b1 = Stop(route_id=r2.id, name="TEST Stop B1", latitude=17.95000, longitude=79.50000, stop_order=1, is_active=True)
    stop_b2 = Stop(route_id=r2.id, name="TEST Stop B2", latitude=17.95000, longitude=79.51000, stop_order=2, is_active=True)
    stop_c1 = Stop(route_id=r3.id, name="TEST Stop C1", latitude=17.95000, longitude=79.50500, stop_order=1, is_active=True)
    stop_c2 = Stop(route_id=r3.id, name="TEST Stop C2", latitude=17.95000, longitude=79.51500, stop_order=2, is_active=True)
    stop_d1 = Stop(route_id=r4.id, name="TEST Stop D1", latitude=17.95000, longitude=79.50600, stop_order=1, is_active=True)
    stop_d2 = Stop(route_id=r4.id, name="TEST Stop D2", latitude=17.95000, longitude=79.51600, stop_order=2, is_active=True)
    db_session.add_all([stop_a1, stop_a2, stop_b1, stop_b2, stop_c1, stop_c2, stop_d1, stop_d2])
    db_session.commit()

    # Create Buses
    bus1 = Bus(bus_number="TEST Bus 101", route_id=r1.id, driver_id=driver1.id, status="active")
    bus2 = Bus(bus_number="TEST Bus 102", route_id=r2.id, status="active")
    bus3 = Bus(bus_number="TEST Bus 103", route_id=r3.id, status="active")
    bus4 = Bus(bus_number="TEST Bus 104", route_id=r4.id, status="active")
    db_session.add_all([bus1, bus2, bus3, bus4])
    db_session.commit()

    # Create Student
    student1 = Student(user_id=u_student.id, roll_number="TEST_ROLL_101", department="CSE", bus_id=bus1.id, stop_id=stop_a1.id)
    student2 = Student(user_id=u_student2.id, roll_number="TEST_ROLL_102", department="ECE", bus_id=bus2.id, stop_id=stop_b1.id)
    db_session.add_all([student1, student2])
    db_session.commit()

    # Tokens
    t_student = create_access_token(u_student.id, "student")
    t_student2 = create_access_token(u_student2.id, "student")
    t_driver = create_access_token(u_driver.id, "driver")
    t_admin = create_access_token(u_admin.id, "admin")
    t_super_admin = create_access_token(u_super_admin.id, "super_admin")

    yield {
        "user_student": u_student,
        "user_student2": u_student2,
        "user_driver": u_driver,
        "user_admin": u_admin,
        "user_super_admin": u_super_admin,
        "student": student1,
        "student2": student2,
        "driver": driver1,
        "token_student": t_student,
        "token_student2": t_student2,
        "token_driver": t_driver,
        "token_admin": t_admin,
        "token_super_admin": t_super_admin,
        "headers_student": {"Authorization": f"Bearer {t_student}"},
        "headers_student2": {"Authorization": f"Bearer {t_student2}"},
        "headers_driver": {"Authorization": f"Bearer {t_driver}"},
        "headers_admin": {"Authorization": f"Bearer {t_admin}"},
        "headers_super_admin": {"Authorization": f"Bearer {t_super_admin}"},
        "bus1": bus1, "bus2": bus2, "bus3": bus3, "bus4": bus4,
        "stop_a1": stop_a1, "stop_a2": stop_a2,
        "stop_b1": stop_b1, "stop_b2": stop_b2, "stop_c1": stop_c1,
    }

    # Cleanup after test run
    from models import ComplaintVerification, DriverComplaint
    db_session.query(BusLocation).delete()
    db_session.query(Trip).delete()
    db_session.query(Notification).delete()
    db_session.query(TravelStatus).delete()
    db_session.query(ComplaintVerification).delete()
    db_session.query(DriverComplaint).delete()
    db_session.query(MissedBusAllotment).delete()
    db_session.query(TemporaryStopChange).filter(TemporaryStopChange.student_id.in_([student1.id, student2.id])).delete()
    db_session.query(Student).filter(Student.id.in_([student1.id, student2.id])).delete()
    db_session.query(Bus).filter(Bus.id.in_([bus1.id, bus2.id, bus3.id, bus4.id])).delete()
    db_session.query(Driver).filter(Driver.id == driver1.id).delete()
    db_session.query(Stop).filter(Stop.id.in_([stop_a1.id, stop_a2.id, stop_b1.id, stop_b2.id, stop_c1.id, stop_c2.id, stop_d1.id, stop_d2.id])).delete()
    db_session.query(Route).filter(Route.id.in_([r1.id, r2.id, r3.id, r4.id])).delete()
    db_session.query(User).filter(User.id.in_([u_student.id, u_student2.id, u_driver.id, u_admin.id, u_super_admin.id])).delete()
    db_session.commit()


# =====================================================================
# 2. API INTEGRATION TESTS: check-route ENDPOINT
# =====================================================================

def test_check_route_on_route_registered_stop(setup_test_environment):
    env = setup_test_environment
    resp = client.post(
        "/student/temporary-stop-change/check-route",
        json={"stop_id": env["stop_a2"].id},
        headers=env["headers_student"]
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["on_route"] is True
    assert data["message"] == "Selected stop is on your assigned bus route"


def test_check_route_off_route_registered_stop(setup_test_environment):
    env = setup_test_environment
    resp = client.post(
        "/student/temporary-stop-change/check-route",
        json={"stop_id": env["stop_b1"].id},
        headers=env["headers_student"]
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["on_route"] is False
    assert len(data["candidate_buses"]) >= 1
    bus_ids = [c["bus_id"] for c in data["candidate_buses"]]
    assert env["bus2"].id in bus_ids


def test_check_route_arbitrary_point_current_route(setup_test_environment):
    env = setup_test_environment
    # Point within 10m of Stop A1 (17.98000, 79.53000)
    lat = 17.98000 + (10.0 / 111320.0)
    lon = 79.53000
    resp = client.post(
        "/student/temporary-stop-change/check-route",
        json={"latitude": lat, "longitude": lon},
        headers=env["headers_student"]
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["on_route"] is True
    assert data["candidate_buses"] == []


def test_check_route_arbitrary_point_another_bus(setup_test_environment, monkeypatch):
    env = setup_test_environment
    polylines = {
        env["bus1"].route_id: [(17.980, 79.530), (17.980, 79.540)],
        env["bus2"].route_id: [(17.950, 79.500), (17.950, 79.510)],
    }
    monkeypatch.setattr("main.get_route_polyline_points", lambda _db, route_id: polylines.get(route_id, []))

    # Point within 10m of Stop B1 (17.95000, 79.50000)
    lat = 17.95000 + (10.0 / 111320.0)
    lon = 79.50000
    resp = client.post(
        "/student/temporary-stop-change/check-route",
        json={"latitude": lat, "longitude": lon},
        headers=env["headers_student"]
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["on_route"] is False
    assert len(data["candidate_buses"]) >= 1
    assert data["candidate_buses"][0]["bus_id"] == env["bus2"].id


def test_check_route_point_30m_to_50m_off_route_not_matched(setup_test_environment, monkeypatch):
    """
    Points beyond 30m off the route (e.g. adjacent parallel street or building setback)
    must NOT be falsely matched under the 30m tolerance.
    """
    env = setup_test_environment
    polylines = {
        env["bus1"].route_id: [(17.980, 79.530), (17.980, 79.540)],
        env["bus2"].route_id: [(17.950, 79.500), (17.950, 79.510)],
        env["bus3"].route_id: [(17.950, 79.505), (17.950, 79.515)],
        env["bus4"].route_id: [(17.950, 79.506), (17.950, 79.516)],
    }
    monkeypatch.setattr("main.get_route_polyline_points", lambda _db, route_id: polylines.get(route_id, []))

    # 45 meters off Stop A1: exceeds 30m tolerance, so should not match Bus 1 or any other bus
    lat_45m_off = 17.98000 + (45.0 / 111320.0)
    lon = 79.53000
    resp = client.post(
        "/student/temporary-stop-change/check-route",
        json={"latitude": lat_45m_off, "longitude": lon},
        headers=env["headers_student"]
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["on_route"] is False
    assert data["candidate_buses"] == []
    assert data["message"] == "No bus is currently travelling through this route."


def test_check_route_no_bus_matches(setup_test_environment):
    env = setup_test_environment
    # Point in the middle of nowhere (e.g. lat=0, lon=0)
    resp = client.post(
        "/student/temporary-stop-change/check-route",
        json={"latitude": 0.0, "longitude": 0.0},
        headers=env["headers_student"]
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["on_route"] is False
    assert data["message"] == "No bus is currently travelling through this route."
    assert data["candidate_buses"] == []


def test_check_route_error_both_inputs_provided(setup_test_environment):
    env = setup_test_environment
    resp = client.post(
        "/student/temporary-stop-change/check-route",
        json={"stop_id": env["stop_a1"].id, "latitude": 17.980, "longitude": 79.530},
        headers=env["headers_student"]
    )
    assert resp.status_code == 400
    assert "Provide either stop_id OR latitude/longitude, not both" in resp.json()["detail"]


def test_check_route_error_neither_input_provided(setup_test_environment):
    env = setup_test_environment
    resp = client.post(
        "/student/temporary-stop-change/check-route",
        json={},
        headers=env["headers_student"]
    )
    assert resp.status_code == 400
    assert "Either stop_id OR latitude and longitude must be provided" in resp.json()["detail"]


# =====================================================================
# 3. API INTEGRATION TESTS: create temporary-stop-change
# =====================================================================

def test_create_temp_stop_on_route_auto_scheduled(setup_test_environment):
    env = setup_test_environment
    today = date.today()
    resp = client.post(
        "/student/temporary-stop-change",
        json={
            "stop_id": env["stop_a2"].id,
            "start_date": str(today),
            "end_date": str(today + timedelta(days=2))
        },
        headers=env["headers_student"]
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert data["status"] in ("active", "scheduled")
    assert data["target_bus_id"] == env["bus1"].id

    # Clean up
    client.delete("/student/temporary-stop-change", headers=env["headers_student"])


def test_create_temp_stop_off_route_auto_approved(setup_test_environment):
    env = setup_test_environment
    today = date.today()
    resp = client.post(
        "/student/temporary-stop-change",
        json={
            "stop_id": env["stop_b1"].id,
            "target_bus_id": env["bus2"].id,
            "start_date": str(today),
            "end_date": str(today + timedelta(days=2))
        },
        headers=env["headers_student"]
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    # Auto-approval: Status is directly active/scheduled (no pending gate)
    assert data["status"] in ("active", "scheduled")
    assert data["target_bus_id"] == env["bus2"].id

    # Clean up
    client.delete("/student/temporary-stop-change", headers=env["headers_student"])


def test_create_temp_stop_candidate_ranking_and_recommendation(setup_test_environment, monkeypatch):
    env = setup_test_environment
    # Three routes share this road section. Distinct live positions/speeds
    # produce deterministic, different ETAs for the requested point.
    lat, lon = 17.95000, 79.50800
    polylines = {
        env["bus1"].route_id: [(17.980, 79.530), (17.980, 79.540)],
        env["bus2"].route_id: [(17.950, 79.500), (17.950, 79.510)],
        env["bus3"].route_id: [(17.950, 79.505), (17.950, 79.515)],
        env["bus4"].route_id: [(17.950, 79.506), (17.950, 79.516)],
    }
    monkeypatch.setattr("main.get_route_polyline_points", lambda _db, route_id: polylines[route_id])
    db = SessionLocal()
    try:
        trips = [Trip(bus_id=bus.id, driver_id=1, route_id=bus.route_id, status="active", started_at=datetime.utcnow())
                 for bus in (env["bus2"], env["bus3"], env["bus4"])]
        db.add_all(trips); db.commit()
        db.add_all([
            BusLocation(bus_id=env["bus2"].id, trip_id=trips[0].id, latitude=lat, longitude=79.500, speed=20),
            BusLocation(bus_id=env["bus3"].id, trip_id=trips[1].id, latitude=lat, longitude=79.505, speed=30),
            BusLocation(bus_id=env["bus4"].id, trip_id=trips[2].id, latitude=lat, longitude=79.507, speed=10),
        ]); db.commit()
    finally:
        db.close()
    resp = client.post(
        "/student/temporary-stop-change/check-route",
        json={"latitude": lat, "longitude": lon},
        headers=env["headers_student"]
    )
    assert resp.status_code == 200
    data = resp.json()
    candidates = data["candidate_buses"]
    assert len(candidates) >= 3

    # Assert top candidate is tagged as recommended
    assert candidates[0]["is_recommended"] is True
    # Assert ETAs are sorted ascending
    etas = [c["eta_minutes"] for c in candidates]
    assert etas == sorted(etas), "Candidate ETAs must be sorted in ascending order"


def test_create_temp_stop_no_bus_matches_rejection(setup_test_environment):
    env = setup_test_environment
    today = date.today()
    resp = client.post(
        "/student/temporary-stop-change",
        json={
            "latitude": 0.0,
            "longitude": 0.0,
            "start_date": str(today),
            "end_date": str(today + timedelta(days=1))
        },
        headers=env["headers_student"]
    )
    assert resp.status_code == 400
    assert resp.json()["detail"] == "No bus is currently travelling through this route."


# =====================================================================
# 4. AUTO-APPROVAL & AUDIT LOG INTEGRATION TEST
# =====================================================================

def test_auto_approval_and_audit_log(setup_test_environment, db_session):
    env = setup_test_environment
    today = date.today()

    # Step 1: Student submits off-route temp stop change
    sub_resp = client.post(
        "/student/temporary-stop-change",
        json={
            "stop_id": env["stop_b1"].id,
            "target_bus_id": env["bus2"].id,
            "start_date": str(today),
            "end_date": str(today + timedelta(days=2))
        },
        headers=env["headers_student"]
    )
    assert sub_resp.status_code == 200
    req_id = sub_resp.json()["request_id"]
    assert sub_resp.json()["status"] in ("active", "scheduled")

    # Step 2: Admin views audit logs
    admin_list = client.get("/admin/temporary-stop-requests", headers=env["headers_admin"])
    assert admin_list.status_code == 200
    req_ids = [r["request_id"] for r in admin_list.json()]
    assert req_id in req_ids

    # Step 3: Verify student effective stop immediately reflects change
    st_obj = db_session.query(Student).filter(Student.id == env["student"].id).first()
    eff_stop, eff_change = get_effective_student_stop(db_session, st_obj)
    assert eff_stop is not None
    assert eff_stop.id == env["stop_b1"].id
    assert eff_change.id == req_id
    assert eff_change.target_bus_id == env["bus2"].id

    # Clean up
    client.delete("/student/temporary-stop-change", headers=env["headers_student"])


# =====================================================================
# 5. BUS MISS REQUEST (REPLACEMENT BUS) INTEGRATION TEST
# =====================================================================

def _create_missed_bus_trips(db, env, passed=True, alternative_active=True, original_active=True):
    original_trip = Trip(
        bus_id=env["bus1"].id,
        driver_id=env["driver"].id,
        route_id=env["bus1"].route_id,
        status="active" if original_active else "completed",
        started_at=datetime.utcnow(),
        ended_at=None if original_active else datetime.utcnow(),
    )
    alternative_trip = Trip(
        bus_id=env["bus2"].id,
        driver_id=env["driver"].id,
        route_id=env["bus2"].route_id,
        status="active" if alternative_active else "completed",
        started_at=datetime.utcnow(),
        ended_at=None if alternative_active else datetime.utcnow(),
    )
    db.add_all([original_trip, alternative_trip])
    db.commit()
    if original_active:
        location_stop = env["stop_a2"] if passed else env["stop_a1"]
        if passed:
            # Trip direction is inferred from GPS history: the bus started at A1 and has moved on to A2.
            db.add(BusLocation(
                bus_id=env["bus1"].id,
                trip_id=original_trip.id,
                latitude=env["stop_a1"].latitude,
                longitude=env["stop_a1"].longitude,
                speed=20,
                timestamp=datetime.utcnow() - timedelta(minutes=5),
            ))
        db.add(BusLocation(
            bus_id=env["bus1"].id,
            trip_id=original_trip.id,
            latitude=location_stop.latitude,
            longitude=location_stop.longitude,
            speed=20,
        ))
        db.commit()
    if alternative_active:
        # Replacement bus must have a GPS fix; B1 is before the student's stop on this route.
        db.add(BusLocation(
            bus_id=env["bus2"].id,
            trip_id=alternative_trip.id,
            latitude=env["stop_b1"].latitude,
            longitude=env["stop_b1"].longitude,
            speed=20,
        ))
        db.commit()
    return original_trip, alternative_trip


def _mock_missed_bus_routes(monkeypatch, env):
    polylines = {
        env["bus1"].route_id: [(17.980, 79.530), (17.980, 79.540)],
        env["bus2"].route_id: [(17.980, 79.530), (17.980, 79.540)],
    }
    monkeypatch.setattr("main.get_route_polyline_points", lambda _db, route_id: polylines.get(route_id, []))


def test_missed_bus_allotment_matching_and_fallback(setup_test_environment, db_session, monkeypatch):
    env = setup_test_environment
    _mock_missed_bus_routes(monkeypatch, env)
    _create_missed_bus_trips(db_session, env)

    # Successful Missed Bus Allotment
    resp = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert data["alternative_bus_id"] == env["bus2"].id
    assert get_active_missed_bus_allotment(db_session, env["student"].id).status == "active"

    # Verify fallback when no alternative bus matches
    alternative_trip = db_session.query(Trip).filter(Trip.bus_id == env["bus2"].id, Trip.status == "active").first()
    db_session.add(BusLocation(bus_id=env["bus2"].id, trip_id=alternative_trip.id,
                               latitude=env["stop_b2"].latitude, longitude=env["stop_b2"].longitude, speed=20))
    db_session.commit()
    monkeypatch.setattr("main.get_route_polyline_points", lambda _db, route_id: [])
    fail_resp = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student2"])
    assert fail_resp.status_code == 400
    assert fail_resp.json()["detail"] == "No bus is currently travelling through this route."


def test_missed_bus_rejects_when_original_bus_has_not_reached_stop(setup_test_environment, db_session, monkeypatch):
    env = setup_test_environment
    _mock_missed_bus_routes(monkeypatch, env)
    _create_missed_bus_trips(db_session, env, passed=False)
    response = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert response.status_code == 400
    assert response.json()["detail"] == "Your bus has not reached your stop yet."


def test_missed_bus_rejects_without_original_trip_today(setup_test_environment):
    env = setup_test_environment
    response = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert response.status_code == 400
    assert response.json()["detail"] == "Your bus hasn't started yet."


def test_missed_bus_rejects_ended_original_trip_today(setup_test_environment, db_session, monkeypatch):
    env = setup_test_environment
    _mock_missed_bus_routes(monkeypatch, env)
    _create_missed_bus_trips(db_session, env, original_active=False)
    response = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert response.status_code == 400
    assert response.json()["detail"] == "Your bus hasn't started yet."


def test_missed_bus_rejects_candidate_without_active_trip(setup_test_environment, db_session, monkeypatch):
    env = setup_test_environment
    _mock_missed_bus_routes(monkeypatch, env)
    _create_missed_bus_trips(db_session, env, alternative_active=False)
    response = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert response.status_code == 400
    assert response.json()["detail"] == "No bus is currently travelling through this route."
    assert db_session.query(MissedBusAllotment).filter(MissedBusAllotment.student_id == env["student"].id).count() == 0


def test_missed_bus_repeat_post_reuses_allotment_and_notification(setup_test_environment, db_session, monkeypatch):
    env = setup_test_environment
    _mock_missed_bus_routes(monkeypatch, env)
    _create_missed_bus_trips(db_session, env)
    first = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    second = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert first.status_code == second.status_code == 200
    assert first.json()["allotment_id"] == second.json()["allotment_id"]
    assert db_session.query(MissedBusAllotment).filter(MissedBusAllotment.student_id == env["student"].id).count() == 1
    assert db_session.query(Notification).filter(Notification.user_id == env["user_student"].id, Notification.type == "alternative_bus_allotted").count() == 1


def test_missed_bus_rejects_not_travelling_student(setup_test_environment, db_session):
    env = setup_test_environment
    db_session.add(TravelStatus(student_id=env["student"].id, date=date.today(), status="not_travelling"))
    db_session.commit()
    response = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert response.status_code == 400
    assert response.json()["detail"] == "You marked yourself as not travelling today"


def test_missed_bus_request_coordinates_require_a_pair(setup_test_environment):
    env = setup_test_environment
    invalid = client.post("/student/missed-bus/allot", json={"latitude": 17.98}, headers=env["headers_student"])
    accepted = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert invalid.status_code == 422
    assert accepted.status_code == 400
    assert accepted.json()["detail"] == "Your bus hasn't started yet."


def test_missed_bus_rejects_out_of_range_coordinates(setup_test_environment):
    env = setup_test_environment
    invalid = client.post("/student/missed-bus/allot", json={"latitude": 91, "longitude": 0}, headers=env["headers_student"])
    accepted = client.post("/student/missed-bus/allot", json={"latitude": 13.0, "longitude": 80.0}, headers=env["headers_student"])
    assert invalid.status_code == 422
    assert accepted.status_code == 400
    assert accepted.json()["detail"] == "Your bus hasn't started yet."


def test_missed_bus_response_contract_for_student_frontend(setup_test_environment, db_session, monkeypatch):
    env = setup_test_environment
    _mock_missed_bus_routes(monkeypatch, env)
    _create_missed_bus_trips(db_session, env)
    post_response = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    get_response = client.get("/student/missed-bus/allotment", headers=env["headers_student"])
    bus_response = client.get("/student/my-bus", headers=env["headers_student"])
    assert post_response.status_code == get_response.status_code == bus_response.status_code == 200
    post_data = post_response.json()
    get_data = get_response.json()
    bus_data = bus_response.json()
    assert {"message", "alternative_bus_id", "alternative_bus_number", "eta_minutes", "stop_id", "stop_name", "original_bus_id", "original_bus_number"}.issubset(post_data)
    assert {"active", "alternative_bus_id", "alternative_bus_number", "stop_id", "stop_name", "trip_active", "original_bus_id"}.issubset(get_data)
    assert bus_data["alternative_bus"] is True
    assert bus_data["bus_id"] == post_data["alternative_bus_id"] == get_data["alternative_bus_id"]
    assert bus_data["original_bus_number"] == post_data["original_bus_number"]


def test_missed_bus_uses_active_custom_temporary_stop(setup_test_environment, db_session, monkeypatch):
    env = setup_test_environment
    _mock_missed_bus_routes(monkeypatch, env)
    _create_missed_bus_trips(db_session, env)
    custom_stop = Stop(route_id=env["bus1"].route_id, name="TEST Custom Stop", latitude=env["stop_a1"].latitude,
                       longitude=env["stop_a1"].longitude, stop_order=99, is_custom=True, is_active=True,
                       created_by_student_id=env["student"].id)
    db_session.add(custom_stop)
    db_session.commit()
    db_session.add(TemporaryStopChange(student_id=env["student"].id, original_stop_id=env["stop_a1"].id,
                                       temporary_stop_id=custom_stop.id, start_date=date.today(), end_date=date.today(), status="active"))
    db_session.commit()
    response = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert response.status_code == 200
    assert response.json()["stop_id"] == custom_stop.id


def test_missed_bus_rejects_active_original_trip_without_gps(setup_test_environment, db_session, monkeypatch):
    env = setup_test_environment
    _mock_missed_bus_routes(monkeypatch, env)
    original_trip = Trip(bus_id=env["bus1"].id, driver_id=env["driver"].id, route_id=env["bus1"].route_id,
                         status="active", started_at=datetime.utcnow())
    alternative_trip = Trip(bus_id=env["bus2"].id, driver_id=env["driver"].id, route_id=env["bus2"].route_id,
                            status="active", started_at=datetime.utcnow())
    db_session.add_all([original_trip, alternative_trip])
    db_session.commit()
    response = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert response.status_code == 400
    assert response.json()["detail"] == "Your bus has not reached your stop yet."


def test_missed_bus_active_trip_filter_does_not_change_temporary_stop_preview(setup_test_environment, monkeypatch):
    env = setup_test_environment
    _mock_missed_bus_routes(monkeypatch, env)
    response = client.post("/student/temporary-stop-change/check-route", json={"stop_id": env["stop_b1"].id}, headers=env["headers_student"])
    assert response.status_code == 200
    assert env["bus2"].id in [candidate["bus_id"] for candidate in response.json()["candidate_buses"]]


# =====================================================================
# 6. DRIVER ROUTE STOPS PASSENGER COUNT WITH TRANSFERS
# =====================================================================

def test_driver_route_stops_pickup_counts(setup_test_environment):
    env = setup_test_environment
    today = date.today()

    # Move Student 1 to Stop B1 on Bus 2
    sub_resp = client.post(
        "/student/temporary-stop-change",
        json={
            "stop_id": env["stop_b1"].id,
            "target_bus_id": env["bus2"].id,
            "start_date": str(today),
            "end_date": str(today)
        },
        headers=env["headers_student"]
    )
    assert sub_resp.status_code == 200

    # Driver 1 (Bus 1) route stops -> student 1 is transferred away, count at Stop A1 should not include student 1
    resp_drv = client.get("/driver/route-stops", headers=env["headers_driver"])
    assert resp_drv.status_code == 200
    stops_data = resp_drv.json().get("stops", [])
    stop_a1_entry = next((s for s in stops_data if s["stop_id"] == env["stop_a1"].id), None)
    assert stop_a1_entry is not None
    assert stop_a1_entry["student_count"] == 0

    # Clean up
    client.delete("/student/temporary-stop-change", headers=env["headers_student"])


# =====================================================================
# 7. SUPER-ADMIN COMPLAINT DETAIL RBAC & VOTER BREAKDOWN
# =====================================================================

def test_super_admin_complaint_detail_rbac(setup_test_environment, db_session):
    env = setup_test_environment
    from models import DriverComplaint, ComplaintVerification

    # Create a complaint
    complaint = DriverComplaint(
        student_id=env["student"].id,
        driver_id=env["driver"].id,
        bus_id=env["bus1"].id,
        reason="rash_driving",
        description="Driver was overspeeding on the highway.",
        status="pending"
    )
    db_session.add(complaint)
    db_session.commit()

    # Add a peer vote
    vote = ComplaintVerification(
        complaint_id=complaint.id,
        student_id=env["student2"].id,
        response="yes"
    )
    db_session.add(vote)
    db_session.commit()

    # Regular admin should be Forbidden (403)
    reg_resp = client.get(f"/admin/complaints/{complaint.id}/detail", headers=env["headers_admin"])
    assert reg_resp.status_code == 403

    # Super admin should be Authorized (200) with full complainant identity & voter details
    super_resp = client.get(f"/admin/complaints/{complaint.id}/detail", headers=env["headers_super_admin"])
    assert super_resp.status_code == 200
    data = super_resp.json()
    assert data["complainant"]["roll_number"] == "TEST_ROLL_101"
    assert data["complainant"]["email"] == "test_student1@kambus.test"
    assert data["corroboration"]["yes_count"] == 1
    assert len(data["voters"]) == 1
    assert data["voters"][0]["roll_number"] == "TEST_ROLL_102"
    assert data["voters"][0]["vote"] == "yes"

    # Regular admin complaints list should mask complainant
    list_resp = client.get("/admin/complaints", headers=env["headers_admin"])
    assert list_resp.status_code == 200
    complaints_list = list_resp.json().get("complaints", [])
    comp_item = next((c for c in complaints_list if c["complaint_id"] == complaint.id), None)
    assert comp_item is not None
    assert comp_item["student_name"] == "Student (Anonymous)"
    assert comp_item["roll_number"] is None


# =====================================================================
# 8. REGRESSION TEST FOR EXISTING FLOW
# =====================================================================

def test_regression_existing_temporary_stop_flow(setup_test_environment):
    env = setup_test_environment
    today = date.today()

    # Verify student get_temporary_stop_change when no change exists
    get_resp = client.get("/student/temporary-stop-change", headers=env["headers_student"])
    assert get_resp.status_code == 200
    assert get_resp.json()["active"] is False

    # Create change on current route using lat/lng (existing flow)
    lat, lon = env["stop_a1"].latitude, env["stop_a1"].longitude
    create_resp = client.post(
        "/student/temporary-stop-change",
        json={
            "latitude": lat,
            "longitude": lon,
            "address": "Test Regular Address",
            "start_date": str(today),
            "end_date": str(today + timedelta(days=1))
        },
        headers=env["headers_student"]
    )
    assert create_resp.status_code == 200
    assert create_resp.json()["status"] in ("active", "scheduled")

    # Cancel change
    del_resp = client.delete("/student/temporary-stop-change", headers=env["headers_student"])
    assert del_resp.status_code == 200
    assert del_resp.json()["success"] is True


# =====================================================================
# 9. OWN-BUS CANDIDATE PRIORITIZATION & ALL BUS ROUTES REFERENCE
# =====================================================================

def test_temporary_stop_own_bus_candidate_ranking_and_exclusion(setup_test_environment, db_session, monkeypatch):
    """
    BUG FIX VERIFICATION:
    When a student's own bus already covers the selected stop/location within tolerance:
    - Their own bus must be included AND ranked first (is_recommended: True, is_own_bus: True).
    - Other candidate buses must be excluded (no need to switch to another bus).
    - If the own bus does NOT cover the location, other candidate buses are returned.
    """
    from main import find_candidate_buses_for_location
    env = setup_test_environment

    bus1 = env["bus1"]  # Route 101: Stop A1 (17.98000, 79.53000), Stop A2 (17.98000, 79.54000)
    bus2 = env["bus2"]  # Route 102: Stop B1 (17.95000, 79.50000), Stop B2 (17.95000, 79.51000)
    bus3 = env["bus3"]  # Route 103: Stop C1 (17.95000, 79.50500), Stop C2 (17.95000, 79.51500)
    bus4 = env["bus4"]  # Route 104: Stop D1 (17.95000, 79.50600), Stop D2 (17.95000, 79.51600)

    polylines = {
        bus1.route_id: [(17.980, 79.530), (17.980, 79.540)],
        bus2.route_id: [(17.950, 79.500), (17.950, 79.510)],
        bus3.route_id: [(17.950, 79.505), (17.950, 79.515)],
        bus4.route_id: [(17.950, 79.506), (17.950, 79.516)],
    }
    monkeypatch.setattr("main.get_route_polyline_points", lambda _db, route_id: polylines[route_id])

    # 1. Location near Stop B1/C1/D1 corridor:
    # (17.95000, 79.50800) is directly on routes for Bus 2, Bus 3, and Bus 4 (dist = 0m).
    test_lat, test_lng = 17.95000, 79.50800

    # Student 2 is assigned to Bus 2.
    # When student_bus_id=bus2.id is supplied:
    candidates, is_approx, _ = find_candidate_buses_for_location(
        db_session,
        lat=test_lat,
        lng=test_lng,
        student_bus_id=bus2.id
    )

    assert len(candidates) >= 1, "Must find at least one candidate"
    assert candidates[0]["bus_id"] == bus2.id, "Student's own bus must be ranked first"
    assert candidates[0]["is_recommended"] is True
    assert candidates[0]["is_own_bus"] is True
    # Other buses (bus3, bus4) must NOT be included since bus2 covers the location
    candidate_bus_ids = [c["bus_id"] for c in candidates]
    assert bus3.id not in candidate_bus_ids, "Other bus 3 should not appear when own bus covers location"
    assert bus4.id not in candidate_bus_ids, "Other bus 4 should not appear when own bus covers location"

    # 2. When student_bus_id is Bus 1 (Route 101, up at 17.98000), Bus 1 is far away (~3.3km).
    # Bus 1 does NOT cover (17.95000, 79.50800).
    candidates_other, _, _ = find_candidate_buses_for_location(
        db_session,
        lat=test_lat,
        lng=test_lng,
        student_bus_id=bus1.id
    )
    # Bus 1 should NOT be in candidates, and other buses (bus2, bus3, bus4) should be returned
    cand_ids_other = [c["bus_id"] for c in candidates_other]
    assert bus1.id not in cand_ids_other, "Bus 1 does not cover this location and should not appear"
    assert len(candidates_other) >= 1, "Other matching buses should appear when own bus does not cover location"
    assert any(c["bus_id"] in [bus2.id, bus3.id, bus4.id] for c in candidates_other)

    # 3. Test check-route endpoint for Student 1 (assigned to Bus 1) checking a stop on Route 102:
    check_resp = client.post(
        "/student/temporary-stop-change/check-route",
        json={"latitude": 17.95000, "longitude": 79.50800},
        headers=env["headers_student"]  # Student 1 is on Bus 1
    )
    assert check_resp.status_code == 200
    data = check_resp.json()
    assert data["on_route"] is False
    assert len(data["candidate_buses"]) >= 1
    # Student 1's bus (Bus 1) does not cover here, so candidate buses from other routes appear
    assert all(c["bus_id"] != bus1.id for c in data["candidate_buses"])


def test_student_all_bus_routes_reference_endpoint(setup_test_environment):
    """
    FEATURE VERIFICATION:
    GET /student/all-bus-routes:
    - Accessible by student role.
    - Returns all active buses with route names and ordered stops list.
    - Contains read-only metadata (bus_id, bus_number, route_id, route_name, stops).
    """
    env = setup_test_environment

    resp = client.get("/student/all-bus-routes", headers=env["headers_student"])
    assert resp.status_code == 200
    data = resp.json()
    assert "buses" in data
    buses = data["buses"]
    assert len(buses) >= 4, "Must list all configured test buses"

    # Find Bus 101 entry
    b1_entry = next((b for b in buses if b["bus_id"] == env["bus1"].id), None)
    assert b1_entry is not None
    assert b1_entry["bus_number"] == "TEST Bus 101"
    assert b1_entry["route_name"] == "TEST Route 101"
    assert len(b1_entry["stops"]) >= 2

    # Check stops are ordered by stop_order
    orders = [s["stop_order"] for s in b1_entry["stops"]]
    assert orders == sorted(orders), "Stops must be sorted by stop_order"

    # Verify anonymous access is forbidden
    unauth_resp = client.get("/student/all-bus-routes")
    assert unauth_resp.status_code in (401, 403)


# =====================================================================
# 10. ROUTE CONTINUITY: BEFORE FIRST STOP, AFTER LAST STOP & LONG STRETCHES
# =====================================================================

def test_route_matching_before_first_stop_and_after_last_stop():
    """
    Verify route matching for points along the road:
    1. Just before the first stop (approach corridor) -> matches.
    2. Just past the last stop (departure corridor) -> matches.
    3. Far before/after route (e.g. 500m away) -> rejected.
    4. Off to the side (perpendicular deviation > 30m) -> rejected.
    """
    from main import extend_polyline_endpoints, min_distance_to_route_m, ROUTE_MATCH_TOLERANCE_M

    # Route segment along lat 17.98000 from lon 79.53000 to 79.54000 (~1.05 km)
    base_poly = [(17.98000, 79.53000), (17.98000, 79.54000)]
    extended_poly = extend_polyline_endpoints(base_poly, ext_m=150.0)

    # 1. Point 40m before the first stop (lon 79.53000) along the road:
    # 40m in lon = 40.0 / 105885.8 = ~0.0003777 deg lon
    p_before_on_road = (17.98000, 79.53000 - 0.0003777)
    dist_before = min_distance_to_route_m(p_before_on_road[0], p_before_on_road[1], extended_poly)
    assert round(dist_before, 2) <= ROUTE_MATCH_TOLERANCE_M, f"Point 40m before first stop on road should match ({dist_before:.2f}m <= {ROUTE_MATCH_TOLERANCE_M}m)"

    # 2. Point 40m past the last stop (lon 79.54000) along the road:
    p_after_on_road = (17.98000, 79.54000 + 0.0003777)
    dist_after = min_distance_to_route_m(p_after_on_road[0], p_after_on_road[1], extended_poly)
    assert round(dist_after, 2) <= ROUTE_MATCH_TOLERANCE_M, f"Point 40m past last stop on road should match ({dist_after:.2f}m <= {ROUTE_MATCH_TOLERANCE_M}m)"

    # 3. Point 500m before the first stop (far outside route corridor):
    p_far = (17.98000, 79.53000 - (500.0 / 105885.8))
    dist_far = min_distance_to_route_m(p_far[0], p_far[1], extended_poly)
    assert round(dist_far, 2) > ROUTE_MATCH_TOLERANCE_M, f"Point 500m away must be rejected ({dist_far:.2f}m > {ROUTE_MATCH_TOLERANCE_M}m)"

    # 4. Point 45m off to the side (perpendicular north of approach corridor):
    p_off_side = (17.98000 + (45.0 / 111320.0), 79.53000 - 0.0003777)
    dist_off_side = min_distance_to_route_m(p_off_side[0], p_off_side[1], extended_poly)
    assert round(dist_off_side, 2) > ROUTE_MATCH_TOLERANCE_M, f"Point 45m off to the side must be rejected ({dist_off_side:.2f}m > {ROUTE_MATCH_TOLERANCE_M}m)"


def test_route_matching_along_long_stretch_between_distant_stops():
    """
    Verify route matching throughout a long stretch between two distant stops.
    Confirms matching works at multiple fractional positions (10%, 25%, 50%, 75%, 90%)
    along the segment, not just near the stops or the midpoint.
    """
    from main import point_to_segment_distance_m, min_distance_to_route_m, ROUTE_MATCH_TOLERANCE_M

    # 5 km long stretch between Stop 1 and Stop 2
    a_lat, a_lon = 17.95000, 79.50000
    b_lat, b_lon = 17.95000, 79.55000  # ~5.3 km east
    polyline = [(a_lat, a_lon), (b_lat, b_lon)]

    fractions = [0.10, 0.25, 0.50, 0.75, 0.90]
    for frac in fractions:
        # Point right on the road at `frac` of the distance
        lon_on_road = a_lon + frac * (b_lon - a_lon)
        dist_on = min_distance_to_route_m(a_lat, lon_on_road, polyline)
        assert round(dist_on, 2) <= ROUTE_MATCH_TOLERANCE_M, f"Point at {int(frac*100)}% on road must match (got {dist_on:.2f}m)"

        # Point 45m off to the side (perpendicular north) at `frac` of the distance
        lat_off_road = a_lat + (45.0 / 111320.0)
        dist_off = min_distance_to_route_m(lat_off_road, lon_on_road, polyline)
        assert round(dist_off, 2) > ROUTE_MATCH_TOLERANCE_M, f"Point 45m off road at {int(frac*100)}% must be rejected (got {dist_off:.2f}m)"


# =====================================================================
# 7. SECURITY TESTS: POST /admin/create
# =====================================================================

def test_admin_create_unauthenticated_rejected():
    """
    POST /admin/create with no token must be rejected with 401 or 403.
    The endpoint now requires a super-admin JWT; unauthenticated callers
    must not be able to create accounts.
    """
    resp = client.post(
        "/admin/create",
        params={"name": "Hacker", "phone": "0000000001", "password": "hacked"},
    )
    assert resp.status_code in (401, 403), (
        f"Expected 401 or 403 for unauthenticated request, got {resp.status_code}"
    )


def test_admin_create_regular_admin_rejected(setup_test_environment):
    """
    A regular admin token must NOT be able to call POST /admin/create.
    Only super-admins are authorised.
    """
    env = setup_test_environment
    resp = client.post(
        "/admin/create",
        params={"name": "SomeAdmin", "phone": "0000000002", "password": "pass123"},
        headers=env["headers_admin"],
    )
    assert resp.status_code in (401, 403), (
        f"Expected 401 or 403 for regular admin, got {resp.status_code}"
    )


def test_admin_create_super_admin_creates_admin_role(setup_test_environment, db_session):
    """
    A super-admin token can create a new account with role='admin' (default).
    The created user must have role='admin' in the response.
    """
    env = setup_test_environment
    test_phone = "0000000003"
    # Clean up any leftover record from a previous run
    db_session.query(User).filter(User.phone == test_phone).delete()
    db_session.commit()

    resp = client.post(
        "/admin/create",
        params={"name": "New Admin", "phone": test_phone, "password": "secure123", "role": "admin"},
        headers=env["headers_super_admin"],
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    data = resp.json()
    assert data["role"] == "admin"
    assert data["phone"] == test_phone

    # Cleanup
    db_session.query(User).filter(User.phone == test_phone).delete()
    db_session.commit()


def test_admin_create_super_admin_creates_super_admin_role(setup_test_environment, db_session):
    """
    A super-admin token can create a new account with role='super_admin'.
    The created user must have role='super_admin' in the response.
    """
    env = setup_test_environment
    test_phone = "0000000004"
    db_session.query(User).filter(User.phone == test_phone).delete()
    db_session.commit()

    resp = client.post(
        "/admin/create",
        params={"name": "New Super Admin", "phone": test_phone, "password": "secure456", "role": "super_admin"},
        headers=env["headers_super_admin"],
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    data = resp.json()
    assert data["role"] == "super_admin"
    assert data["phone"] == test_phone

    # Cleanup
    db_session.query(User).filter(User.phone == test_phone).delete()
    db_session.commit()


def test_admin_create_invalid_role_rejected(setup_test_environment):
    """
    Passing an invalid role (e.g. 'driver') to POST /admin/create must be
    rejected with HTTP 400 regardless of the caller's authority.
    """
    env = setup_test_environment
    resp = client.post(
        "/admin/create",
        params={"name": "Bad Role", "phone": "0000000005", "password": "pass123", "role": "driver"},
        headers=env["headers_super_admin"],
    )
    assert resp.status_code == 400, (
        f"Expected 400 for invalid role 'driver', got {resp.status_code}: {resp.text}"
    )
    assert "role" in resp.json()["detail"].lower()


def test_admin_create_default_role_is_admin(setup_test_environment, db_session):
    """
    When no role is specified the endpoint must default to 'admin'.
    """
    env = setup_test_environment
    test_phone = "0000000006"
    db_session.query(User).filter(User.phone == test_phone).delete()
    db_session.commit()

    resp = client.post(
        "/admin/create",
        params={"name": "Default Role Admin", "phone": test_phone, "password": "pass123"},
        headers=env["headers_super_admin"],
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    assert resp.json()["role"] == "admin"

    # Cleanup
    db_session.query(User).filter(User.phone == test_phone).delete()
    db_session.commit()


# =====================================================================
# MISSED BUS: replacement bus must not have passed the stop / capacity optional
# =====================================================================

def _mock_three_route_polylines(monkeypatch, env):
    line = [(17.980, 79.530), (17.980, 79.540)]
    polylines = {env["bus1"].route_id: line, env["bus2"].route_id: line, env["bus3"].route_id: line}
    monkeypatch.setattr("main.get_route_polyline_points", lambda _db, route_id: polylines.get(route_id, []))


def _start_trip_with_gps(db, env, bus, lat=None, lng=None, speed=20, prior=None):
    trip = Trip(bus_id=bus.id, driver_id=env["driver"].id, route_id=bus.route_id,
                status="active", started_at=datetime.utcnow())
    db.add(trip)
    db.commit()
    if prior is not None:
        # earlier GPS point: shows which way the bus is travelling
        db.add(BusLocation(bus_id=bus.id, trip_id=trip.id, latitude=prior[0], longitude=prior[1],
                           speed=speed, timestamp=datetime.utcnow() - timedelta(minutes=5)))
        db.commit()
    if lat is not None:
        db.add(BusLocation(bus_id=bus.id, trip_id=trip.id, latitude=lat, longitude=lng, speed=speed))
        db.commit()
    return trip


def test_missed_bus_skips_replacement_bus_that_already_passed_stop(setup_test_environment, db_session, monkeypatch):
    env = setup_test_environment
    _mock_three_route_polylines(monkeypatch, env)
    # Route 102 also runs along the student's line: X1 sits ~100 m from the student's stop A1, X2 is beyond it.
    x1 = Stop(route_id=env["bus2"].route_id, name="TEST Stop X1", latitude=17.980, longitude=79.529, stop_order=10, is_active=True)
    x2 = Stop(route_id=env["bus2"].route_id, name="TEST Stop X2", latitude=17.980, longitude=79.540, stop_order=11, is_active=True)
    db_session.add_all([x1, x2])
    db_session.commit()

    original_trip = _start_trip_with_gps(db_session, env, env["bus1"], env["stop_a2"].latitude, env["stop_a2"].longitude, prior=(env["stop_a1"].latitude, env["stop_a1"].longitude))
    # bus2 is closest/soonest but is already at X2 (past the student's stop); bus3 is farther but still before it.
    _start_trip_with_gps(db_session, env, env["bus2"], x2.latitude, x2.longitude, prior=(env["stop_b1"].latitude, env["stop_b1"].longitude))
    bus3_trip = _start_trip_with_gps(db_session, env, env["bus3"], env["stop_c1"].latitude, env["stop_c1"].longitude)

    resp = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert resp.status_code == 200, resp.text
    assert resp.json()["alternative_bus_id"] == env["bus3"].id
    assert resp.json()["alternative_trip_id"] == bus3_trip.id

    db_session.query(Stop).filter(Stop.id.in_([x1.id, x2.id])).delete(synchronize_session=False)
    db_session.commit()


def test_missed_bus_rejects_when_only_candidate_already_passed_stop(setup_test_environment, db_session, monkeypatch):
    env = setup_test_environment
    _mock_three_route_polylines(monkeypatch, env)
    x1 = Stop(route_id=env["bus2"].route_id, name="TEST Stop X1", latitude=17.980, longitude=79.529, stop_order=10, is_active=True)
    x2 = Stop(route_id=env["bus2"].route_id, name="TEST Stop X2", latitude=17.980, longitude=79.540, stop_order=11, is_active=True)
    db_session.add_all([x1, x2])
    db_session.commit()

    _start_trip_with_gps(db_session, env, env["bus1"], env["stop_a2"].latitude, env["stop_a2"].longitude, prior=(env["stop_a1"].latitude, env["stop_a1"].longitude))
    _start_trip_with_gps(db_session, env, env["bus2"], x2.latitude, x2.longitude, prior=(env["stop_b1"].latitude, env["stop_b1"].longitude))

    resp = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert resp.status_code == 400
    assert resp.json()["detail"] == "No bus is currently travelling through this route."
    assert db_session.query(MissedBusAllotment).count() == 0

    db_session.query(Stop).filter(Stop.id.in_([x1.id, x2.id])).delete(synchronize_session=False)
    db_session.commit()


def test_missed_bus_skips_replacement_bus_without_gps(setup_test_environment, db_session, monkeypatch):
    env = setup_test_environment
    _mock_three_route_polylines(monkeypatch, env)
    _start_trip_with_gps(db_session, env, env["bus1"], env["stop_a2"].latitude, env["stop_a2"].longitude, prior=(env["stop_a1"].latitude, env["stop_a1"].longitude))
    _start_trip_with_gps(db_session, env, env["bus2"])  # active trip, no GPS fix yet

    resp = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert resp.status_code == 400
    assert resp.json()["detail"] == "No bus is currently travelling through this route."


def test_missed_bus_capacity_is_ignored_when_not_configured(setup_test_environment, db_session, monkeypatch):
    """Pilot behaviour: Bus has no capacity, so a bus with many registered students is still allotted."""
    env = setup_test_environment
    _mock_three_route_polylines(monkeypatch, env)
    assert not hasattr(env["bus2"], "capacity")
    _start_trip_with_gps(db_session, env, env["bus1"], env["stop_a2"].latitude, env["stop_a2"].longitude, prior=(env["stop_a1"].latitude, env["stop_a1"].longitude))
    _start_trip_with_gps(db_session, env, env["bus2"], env["stop_b2"].latitude, env["stop_b2"].longitude)

    resp = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert resp.status_code == 200, resp.text
    assert resp.json()["alternative_bus_id"] == env["bus2"].id


def test_missed_bus_capacity_enforced_once_a_capacity_value_exists(setup_test_environment, db_session, monkeypatch):
    """Future behaviour: when Bus.capacity exists, a full bus is skipped and the next bus with room is used."""
    env = setup_test_environment
    _mock_three_route_polylines(monkeypatch, env)
    monkeypatch.setattr(Bus, "capacity", 1, raising=False)  # simulates the future column
    _start_trip_with_gps(db_session, env, env["bus1"], env["stop_a2"].latitude, env["stop_a2"].longitude, prior=(env["stop_a1"].latitude, env["stop_a1"].longitude))
    # bus2 is soonest but already has 1 registered student (student2) -> full at capacity 1
    _start_trip_with_gps(db_session, env, env["bus2"], env["stop_b2"].latitude, env["stop_b2"].longitude)
    _start_trip_with_gps(db_session, env, env["bus3"], env["stop_c1"].latitude, env["stop_c1"].longitude)

    resp = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert resp.status_code == 200, resp.text
    assert resp.json()["alternative_bus_id"] == env["bus3"].id


# =====================================================================
# ACCESS CONTROL: legacy create endpoints must require an admin token
# =====================================================================

@pytest.mark.parametrize("method,path,body", [
    ("post", "/routes", {"name": "TEST Auth Route"}),
    ("post", "/buses", {"bus_number": "TEST-AUTH-1"}),
    ("post", "/routes/1/stops", {"name": "TEST Auth Stop", "latitude": 17.98, "longitude": 79.53, "stop_order": 1}),
    ("post", "/students", {"name": "TEST Auth", "phone": "9999911111", "password": "pass123", "roll_number": "TEST_AUTH_1"}),
    ("post", "/drivers", {"name": "TEST Auth", "phone": "9999911112", "password": "pass123", "driver_code": "TEST_AUTH_D", "license_number": "L1"}),
    ("post", "/buses/1/assign-driver", {"driver_code": "TEST_AUTH_D"}),
])
def test_legacy_create_endpoints_reject_anonymous_and_non_admin(setup_test_environment, db_session, method, path, body):
    env = setup_test_environment
    before = (db_session.query(Route).count(), db_session.query(Bus).count(), db_session.query(User).count())

    anonymous = getattr(client, method)(path, json=body)
    assert anonymous.status_code in (401, 403)

    as_student = getattr(client, method)(path, json=body, headers=env["headers_student"])
    assert as_student.status_code == 403
    as_driver = getattr(client, method)(path, json=body, headers=env["headers_driver"])
    assert as_driver.status_code == 403

    db_session.expire_all()
    after = (db_session.query(Route).count(), db_session.query(Bus).count(), db_session.query(User).count())
    assert before == after


def test_legacy_create_route_still_works_for_admin(setup_test_environment, db_session):
    env = setup_test_environment
    resp = client.post("/routes", json={"name": "TEST Auth Route OK"}, headers=env["headers_admin"])
    assert resp.status_code == 200, resp.text
    db_session.query(Route).filter(Route.name == "TEST Auth Route OK").delete()
    db_session.commit()


# =====================================================================
# ATTEMPT LIMITS: login lockout and OTP guess limit
# =====================================================================

@pytest.fixture(autouse=True)
def _reset_attempt_limits():
    import main as _main
    _main._FAILED_LOGINS.clear()
    _main._OTP_WRONG_GUESSES.clear()
    yield
    _main._FAILED_LOGINS.clear()
    _main._OTP_WRONG_GUESSES.clear()


def _login(identifier, password, role="student"):
    return client.post("/auth/login", json={"identifier": identifier, "password": password, "role": role})


def test_login_locks_after_five_failures_even_with_correct_password(setup_test_environment):
    for _ in range(5):
        assert _login("TEST_ROLL_101", "wrong-password").status_code == 401
    locked = _login("TEST_ROLL_101", "pass123")
    assert locked.status_code == 429
    assert "Too many failed sign-in attempts" in locked.json()["detail"]
    # a different account is unaffected
    assert _login("TEST_ROLL_102", "pass123").status_code == 200


def test_login_success_clears_failure_count(setup_test_environment):
    for _ in range(4):
        assert _login("TEST_ROLL_101", "wrong-password").status_code == 401
    assert _login("TEST_ROLL_101", "pass123").status_code == 200
    for _ in range(4):
        assert _login("TEST_ROLL_101", "wrong-password").status_code == 401
    assert _login("TEST_ROLL_101", "pass123").status_code == 200


def test_login_lock_expires_after_window(setup_test_environment, monkeypatch):
    import main as _main
    for _ in range(5):
        _login("TEST_ROLL_101", "wrong-password")
    assert _login("TEST_ROLL_101", "pass123").status_code == 429
    real = _main._monotonic
    monkeypatch.setattr(_main, "_monotonic", lambda: real() + _main.LOGIN_FAILURE_WINDOW_SECONDS + 5)
    assert _login("TEST_ROLL_101", "pass123").status_code == 200


def test_admin_login_with_non_numeric_id_counts_toward_lockout(setup_test_environment):
    for _ in range(5):
        assert _login("not-a-number", "x", role="admin").status_code == 401
    assert _login("not-a-number", "x", role="admin").status_code == 429


def _make_unverified_student_with_otp(db, code="123456"):
    from models import StudentOTP
    from main import hash_otp
    db.query(StudentOTP).filter(StudentOTP.email == "test_otp@kambus.test").delete()
    db.query(User).filter(User.email == "test_otp@kambus.test").delete()
    db.commit()
    user = User(name="Test OTP", email="test_otp@kambus.test", phone="9999922222",
                password_hash=hash_password("pass123"), role="student", is_verified=0)
    db.add(user)
    db.commit()
    db.add(StudentOTP(email="test_otp@kambus.test", otp_code=hash_otp(code), user_id=user.id,
                      expires_at=datetime.utcnow() + timedelta(minutes=10), is_used=0))
    db.commit()
    return user


def _cleanup_otp_user(db):
    from models import StudentOTP
    db.query(StudentOTP).filter(StudentOTP.email == "test_otp@kambus.test").delete()
    db.query(User).filter(User.email == "test_otp@kambus.test").delete()
    db.commit()


def _verify(code):
    return client.post("/auth/student/verify-otp", json={"email": "test_otp@kambus.test", "otp_code": code})


def test_otp_is_burned_after_five_wrong_guesses(setup_test_environment, db_session):
    _make_unverified_student_with_otp(db_session)
    for _ in range(4):
        assert _verify("000000").status_code == 400
    fifth = _verify("000000")
    assert fifth.status_code == 429
    assert "request a new code" in fifth.json()["detail"]
    # even the right code no longer works: the code was burned
    after = _verify("123456")
    assert after.status_code == 400
    assert "No active verification code" in after.json()["detail"]
    _cleanup_otp_user(db_session)


def test_otp_correct_code_still_works_after_a_few_wrong_guesses(setup_test_environment, db_session):
    _make_unverified_student_with_otp(db_session)
    for _ in range(4):
        assert _verify("000000").status_code == 400
    ok = _verify("123456")
    assert ok.status_code == 200, ok.text
    assert ok.json()["success"] is True
    _cleanup_otp_user(db_session)


# =====================================================================
# TRIP DIRECTION: evening runs travel the route in reverse stop_order
# =====================================================================

def _direction_setup(db, env, points):
    """Add stop A3 (order 3) to route 101 and start a bus-1 trip with GPS points [(stop, minutes_ago), ...]."""
    a3 = Stop(route_id=env["bus1"].route_id, name="TEST Stop A3", latitude=17.980, longitude=79.550,
              stop_order=3, is_active=True)
    db.add(a3)
    db.commit()
    trip = Trip(bus_id=env["bus1"].id, driver_id=env["driver"].id, route_id=env["bus1"].route_id,
                status="active", started_at=datetime.utcnow())
    db.add(trip)
    db.commit()
    stops = {"A1": env["stop_a1"], "A2": env["stop_a2"], "A3": a3}
    for name, minutes_ago in points:
        db.add(BusLocation(bus_id=env["bus1"].id, trip_id=trip.id, latitude=stops[name].latitude,
                           longitude=stops[name].longitude, speed=20,
                           timestamp=datetime.utcnow() - timedelta(minutes=minutes_ago)))
    db.commit()
    return trip, stops


def _drop_stop(db, stop):
    db.query(Stop).filter(Stop.id == stop.id).delete()
    db.commit()


def test_has_passed_stop_forward_trip(setup_test_environment, db_session):
    from main import has_passed_stop
    env = setup_test_environment
    trip, stops = _direction_setup(db_session, env, [("A1", 10), ("A2", 1)])
    assert has_passed_stop(db_session, trip, stops["A1"]) is True
    assert has_passed_stop(db_session, trip, stops["A3"]) is False
    _drop_stop(db_session, stops["A3"])


def test_has_passed_stop_reverse_evening_trip_after_movement(setup_test_environment, db_session):
    from main import has_passed_stop
    env = setup_test_environment
    trip, stops = _direction_setup(db_session, env, [("A3", 10), ("A2", 1)])
    assert has_passed_stop(db_session, trip, stops["A3"]) is True    # already left A3
    assert has_passed_stop(db_session, trip, stops["A1"]) is False   # still on its way to A1
    _drop_stop(db_session, stops["A3"])


def test_has_passed_stop_reverse_trip_that_has_just_started(setup_test_environment, db_session):
    from main import has_passed_stop
    env = setup_test_environment
    trip, stops = _direction_setup(db_session, env, [("A3", 1)])  # single point at the far end = trip started there
    assert has_passed_stop(db_session, trip, stops["A1"]) is False
    assert has_passed_stop(db_session, trip, stops["A3"]) is False
    _drop_stop(db_session, stops["A3"])


def test_has_passed_stop_forward_trip_that_has_just_started(setup_test_environment, db_session):
    from main import has_passed_stop
    env = setup_test_environment
    trip, stops = _direction_setup(db_session, env, [("A1", 1)])
    assert has_passed_stop(db_session, trip, stops["A3"]) is False
    _drop_stop(db_session, stops["A3"])


def test_has_passed_stop_trip_started_mid_route_uses_movement(setup_test_environment, db_session):
    from main import has_passed_stop
    env = setup_test_environment
    trip, stops = _direction_setup(db_session, env, [("A2", 10), ("A3", 1)])  # started at A2, moving up
    assert has_passed_stop(db_session, trip, stops["A1"]) is True
    assert has_passed_stop(db_session, trip, stops["A2"]) is True
    assert has_passed_stop(db_session, trip, stops["A3"]) is False
    _drop_stop(db_session, stops["A3"])


def test_missed_bus_evening_trip_not_yet_at_students_stop(setup_test_environment, db_session, monkeypatch):
    env = setup_test_environment
    _mock_three_route_polylines(monkeypatch, env)
    trip, stops = _direction_setup(db_session, env, [("A3", 10), ("A2", 1)])
    _start_trip_with_gps(db_session, env, env["bus2"], env["stop_b1"].latitude, env["stop_b1"].longitude)
    # student's stop is A1: the evening bus (A3 -> A2 -> A1) has not reached it yet
    resp = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert resp.status_code == 400
    assert "not reached" in resp.json()["detail"]
    _drop_stop(db_session, stops["A3"])


def test_missed_bus_evening_trip_after_students_stop(setup_test_environment, db_session, monkeypatch):
    env = setup_test_environment
    # replacement route's road line must reach stop A3 (79.550) for it to count as passing through
    line = [(17.980, 79.530), (17.980, 79.555)]
    monkeypatch.setattr("main.get_route_polyline_points", lambda _db, route_id: line)
    trip, stops = _direction_setup(db_session, env, [("A3", 10), ("A2", 1)])
    env["student"].stop_id = stops["A3"].id  # the evening bus left A3 first, so this student has missed it
    db_session.commit()
    _start_trip_with_gps(db_session, env, env["bus2"], env["stop_b1"].latitude, env["stop_b1"].longitude)
    resp = client.post("/student/missed-bus/allot", json={}, headers=env["headers_student"])
    assert resp.status_code == 200, resp.text
    assert resp.json()["alternative_bus_id"] == env["bus2"].id
    _drop_stop(db_session, stops["A3"])
