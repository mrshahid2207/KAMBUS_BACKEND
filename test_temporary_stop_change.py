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
    _POLYLINE_CACHE,
)
from database import SessionLocal, Base, engine
from models import (
    User, Student, Driver, Route, Bus, Stop, Trip, BusLocation, TemporaryStopChange
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
    Explicit boundary condition testing for ROUTE_MATCH_TOLERANCE_M = 150.0m.
    
    - 50.0m   --> matches (<= 150.0m)
    - 150.00m --> matches (explicitly testing exact boundary <= condition)
    - 150.01m --> does NOT match (> 150.0m)
    - 250.0m  --> does NOT match (> 150.0m)
    """
    assert ROUTE_MATCH_TOLERANCE_M == 150.0

    a_lat, a_lon = 17.98000, 79.53000
    b_lat, b_lon = 17.98000, 79.54000
    # midpoint longitude = 79.53500

    # 1 deg lat = 111,320 meters
    delta_lat_50m = 50.0 / 111320.0
    delta_lat_150m = 150.0 / 111320.0
    delta_lat_150_01m = 150.01 / 111320.0
    delta_lat_250m = 250.0 / 111320.0

    # 50m offset
    dist_50m = point_to_segment_distance_m(17.98000 + delta_lat_50m, 79.53500, a_lat, a_lon, b_lat, b_lon)
    assert dist_50m <= ROUTE_MATCH_TOLERANCE_M, f"50m ({dist_50m:.2f}m) should match tolerance 150m"

    # Exactly 150.0m offset
    dist_150m = point_to_segment_distance_m(17.98000 + delta_lat_150m, 79.53500, a_lat, a_lon, b_lat, b_lon)
    # Assert explicit <= behavior
    assert dist_150m <= ROUTE_MATCH_TOLERANCE_M, f"Exactly 150.0m ({dist_150m:.2f}m) must satisfy <= {ROUTE_MATCH_TOLERANCE_M}"

    # 150.01m offset
    dist_150_01m = point_to_segment_distance_m(17.98000 + delta_lat_150_01m, 79.53500, a_lat, a_lon, b_lat, b_lon)
    assert dist_150_01m > ROUTE_MATCH_TOLERANCE_M, f"150.01m ({dist_150_01m:.2f}m) must exceed 150m tolerance"

    # 250m offset
    dist_250m = point_to_segment_distance_m(17.98000 + delta_lat_250m, 79.53500, a_lat, a_lon, b_lat, b_lon)
    assert dist_250m > ROUTE_MATCH_TOLERANCE_M, f"250m ({dist_250m:.2f}m) must exceed 150m tolerance"


def test_min_distance_to_route_polyline():
    """Test min_distance_to_route_m over multi-segment polyline."""
    polyline = [
        (17.98000, 79.53000),
        (17.98000, 79.54000),
        (17.99000, 79.54000),
    ]
    # Point near 2nd segment
    p_lat, p_lon = 17.98500, 79.54050
    dist_m = min_distance_to_route_m(p_lat, p_lon, polyline)
    # 0.0005 deg lon at lat 17.985 is ~53 meters
    assert dist_m < 60.0
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
    db_session.query(TemporaryStopChange).filter(TemporaryStopChange.selected_address.like("%Test%")).delete()
    db_session.query(Student).filter(Student.roll_number.like("TEST%")).delete()
    db_session.query(Bus).filter(Bus.bus_number.like("TEST%")).delete()
    db_session.query(Stop).filter(Stop.name.like("TEST%")).delete()
    db_session.query(Route).filter(Route.name.like("TEST%")).delete()
    db_session.query(User).filter(User.email.like("test_%@kambus.test")).delete()
    db_session.commit()

    # Create Users
    u_student = User(name="Test Student 1", email="test_student1@kambus.test", phone="9999900001", password_hash=hash_password("pass123"), role="student", is_verified=True)
    u_admin = User(name="Test Admin", email="test_admin@kambus.test", phone="9999900002", password_hash=hash_password("pass123"), role="admin", is_verified=True)
    db_session.add_all([u_student, u_admin])
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
    bus1 = Bus(bus_number="TEST Bus 101", route_id=r1.id, status="active")
    bus2 = Bus(bus_number="TEST Bus 102", route_id=r2.id, status="active")
    bus3 = Bus(bus_number="TEST Bus 103", route_id=r3.id, status="active")
    bus4 = Bus(bus_number="TEST Bus 104", route_id=r4.id, status="active")
    db_session.add_all([bus1, bus2, bus3, bus4])
    db_session.commit()

    # Create Student
    student1 = Student(user_id=u_student.id, roll_number="TEST_ROLL_101", department="CSE", bus_id=bus1.id, stop_id=stop_a1.id)
    db_session.add(student1)
    db_session.commit()

    # Tokens
    t_student = create_access_token(u_student.id, "student")
    t_admin = create_access_token(u_admin.id, "admin")

    yield {
        "user_student": u_student,
        "user_admin": u_admin,
        "student": student1,
        "token_student": t_student,
        "token_admin": t_admin,
        "headers_student": {"Authorization": f"Bearer {t_student}"},
        "headers_admin": {"Authorization": f"Bearer {t_admin}"},
        "bus1": bus1, "bus2": bus2, "bus3": bus3, "bus4": bus4,
        "stop_a1": stop_a1, "stop_a2": stop_a2,
        "stop_b1": stop_b1, "stop_b2": stop_b2, "stop_c1": stop_c1,
    }

    # Cleanup after test run
    db_session.query(TemporaryStopChange).filter(TemporaryStopChange.student_id == student1.id).delete()
    db_session.query(Student).filter(Student.id == student1.id).delete()
    db_session.query(Bus).filter(Bus.id.in_([bus1.id, bus2.id, bus3.id, bus4.id])).delete()
    db_session.query(Stop).filter(Stop.id.in_([stop_a1.id, stop_a2.id, stop_b1.id, stop_b2.id, stop_c1.id, stop_c2.id, stop_d1.id, stop_d2.id])).delete()
    db_session.query(Route).filter(Route.id.in_([r1.id, r2.id, r3.id, r4.id])).delete()
    db_session.query(User).filter(User.id.in_([u_student.id, u_admin.id])).delete()
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
    # Point 50m off Stop A1 (17.98000, 79.53000)
    lat = 17.98000 + (50.0 / 111320.0)
    lon = 79.53000
    resp = client.post(
        "/student/temporary-stop-change/check-route",
        json={"latitude": lat, "longitude": lon},
        headers=env["headers_student"]
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["on_route"] is True


def test_check_route_arbitrary_point_another_bus(setup_test_environment):
    env = setup_test_environment
    # Point 50m off Stop B1 (17.95000, 79.50000)
    lat = 17.95000 + (50.0 / 111320.0)
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


def test_create_temp_stop_off_route_pending_approval(setup_test_environment):
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
    assert data["status"] == "pending_admin_approval"
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
# 4. ADMIN APPROVAL & EFFECTIVE STOP INTEGRATION TEST
# =====================================================================

def test_admin_approve_temporary_stop_request(setup_test_environment, db_session):
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
    assert sub_resp.json()["status"] == "pending_admin_approval"

    # Step 2: Admin lists pending requests
    admin_list = client.get("/admin/temporary-stop-requests?status=pending_admin_approval", headers=env["headers_admin"])
    assert admin_list.status_code == 200
    req_ids = [r["request_id"] for r in admin_list.json()]
    assert req_id in req_ids

    # Step 3: Admin approves request
    app_resp = client.post(f"/admin/temporary-stop-requests/{req_id}/approve", headers=env["headers_admin"])
    assert app_resp.status_code == 200
    assert app_resp.json()["status"] in ("active", "scheduled")

    # Step 4: Verify student effective stop reflects approved change
    st_obj = db_session.query(Student).filter(Student.id == env["student"].id).first()
    eff_stop, eff_change = get_effective_student_stop(db_session, st_obj)
    assert eff_stop is not None
    assert eff_stop.id == env["stop_b1"].id
    assert eff_change.id == req_id
    assert eff_change.target_bus_id == env["bus2"].id

    # Clean up
    client.delete("/student/temporary-stop-change", headers=env["headers_student"])


# =====================================================================
# 5. REGRESSION TEST FOR EXISTING FLOW
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
