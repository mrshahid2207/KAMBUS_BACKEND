"""
Wait Request Lifecycle Tests.
Tests the new wait-request state machine:
  pending -> accepted (budget reserved, wait_until=NULL)
  accepted -> waiting (bus arrives at stop)
  waiting -> completed (student boards OR timer expires)
  cancel releases budget
  trip-end force-closes open requests
"""
import pytest
from datetime import date, datetime, timedelta
import math

from main import (
    app,
    is_at_stop,
    maybe_start_wait_countdown,
    WAIT_STOP_ARRIVAL_RADIUS_M,
)
from database import SessionLocal, Base, engine
from models import (
    User, Student, Driver, Route, Bus, Stop, Trip, BusLocation,
    WaitRequest, Notification, TravelStatus,
)
from auth import hash_password, create_access_token
from fastapi.testclient import TestClient

client = TestClient(app)


# =====================================================================
# FIXTURES
# =====================================================================

@pytest.fixture
def db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def wait_env(db_session):
    """
    Isolated fixture for wait-request lifecycle tests.
    Creates:
      - Route WAIT_R1 with stops WAIT_S1 (17.98, 79.53) and WAIT_S2 (17.98, 79.54)
      - Bus WAIT_BUS_1 on WAIT_R1, driven by WAIT_DRV_1
      - Student WAIT_STU_1 assigned to WAIT_BUS_1 / WAIT_S1
      - Active trip with 10-minute wait budget
    """
    db = db_session

    # Cleanup any leftover test data
    db.query(BusLocation).filter(BusLocation.bus_id.in_(
        db.query(Bus.id).filter(Bus.bus_number.like('WAIT_%'))
    )).delete(synchronize_session=False)
    db.query(WaitRequest).filter(WaitRequest.bus_id.in_(
        db.query(Bus.id).filter(Bus.bus_number.like('WAIT_%'))
    )).delete(synchronize_session=False)
    db.query(Notification).filter(Notification.user_id.in_(
        db.query(User.id).filter(User.email.like('wait_%@kambus.test'))
    )).delete(synchronize_session=False)
    db.query(Trip).filter(Trip.bus_id.in_(
        db.query(Bus.id).filter(Bus.bus_number.like('WAIT_%'))
    )).delete(synchronize_session=False)
    db.query(TravelStatus).filter(TravelStatus.student_id.in_(
        db.query(Student.id).filter(Student.roll_number.like('WAIT_%'))
    )).delete(synchronize_session=False)
    db.query(Student).filter(Student.roll_number.like('WAIT_%')).delete()
    db.query(Bus).filter(Bus.bus_number.like('WAIT_%')).delete()
    db.query(Driver).filter(Driver.driver_code.like('WAIT_%')).delete()
    db.query(Stop).filter(Stop.name.like('WAIT_%')).delete()
    db.query(Route).filter(Route.name.like('WAIT_%')).delete()
    db.query(User).filter(User.email.like('wait_%@kambus.test')).delete()
    db.commit()

    # Users
    u_student = User(name='Wait Student', email='wait_stu@kambus.test',
                     phone='8880001111', password_hash=hash_password('pass'),
                     role='student', is_verified=True)
    u_driver = User(name='Wait Driver', email='wait_drv@kambus.test',
                    phone='8880002222', password_hash=hash_password('pass'),
                    role='driver', is_verified=True)
    db.add_all([u_student, u_driver])
    db.commit()

    # Driver
    drv = Driver(user_id=u_driver.id, driver_code='WAIT_DRV_1', license_number='WLIC1')
    db.add(drv)
    db.commit()

    # Route + Stops
    route = Route(name='WAIT_R1', description='Wait test route')
    db.add(route)
    db.commit()

    stop1 = Stop(route_id=route.id, name='WAIT_S1', latitude=17.98000,
                 longitude=79.53000, stop_order=1, is_active=True)
    stop2 = Stop(route_id=route.id, name='WAIT_S2', latitude=17.98000,
                 longitude=79.54000, stop_order=2, is_active=True)
    db.add_all([stop1, stop2])
    db.commit()

    # Bus
    bus = Bus(bus_number='WAIT_BUS_1', route_id=route.id,
              driver_id=drv.id, status='active')
    db.add(bus)
    db.commit()

    # Student
    stu = Student(user_id=u_student.id, roll_number='WAIT_STU_1',
                  department='CSE', bus_id=bus.id, stop_id=stop1.id)
    db.add(stu)
    db.commit()

    # Active Trip
    trip = Trip(bus_id=bus.id, driver_id=drv.id, route_id=route.id,
                status='active', wait_budget_total=10, wait_budget_used=0)
    db.add(trip)
    db.commit()

    # Tokens
    t_stu = create_access_token(u_student.id, 'student')
    t_drv = create_access_token(u_driver.id, 'driver')

    yield {
        'u_student': u_student,
        'u_driver': u_driver,
        'student': stu,
        'driver': drv,
        'route': route,
        'stop1': stop1,
        'stop2': stop2,
        'bus': bus,
        'trip': trip,
        'headers_student': {'Authorization': f'Bearer {t_stu}'},
        'headers_driver': {'Authorization': f'Bearer {t_drv}'},
    }

    # Teardown
    db.query(BusLocation).filter(BusLocation.trip_id == trip.id).delete()
    db.query(WaitRequest).filter(WaitRequest.trip_id == trip.id).delete()
    db.query(Notification).filter(Notification.user_id.in_(
        [u_student.id, u_driver.id]
    )).delete(synchronize_session=False)
    db.query(TravelStatus).filter(TravelStatus.student_id == stu.id).delete()
    db.query(Trip).filter(Trip.id == trip.id).delete()
    db.query(Student).filter(Student.id == stu.id).delete()
    db.query(Bus).filter(Bus.id == bus.id).delete()
    db.query(Driver).filter(Driver.id == drv.id).delete()
    db.query(Stop).filter(Stop.id.in_([stop1.id, stop2.id])).delete()
    db.query(Route).filter(Route.id == route.id).delete()
    db.query(User).filter(User.id.in_([u_student.id, u_driver.id])).delete()
    db.commit()


# =====================================================================
# UNIT TESTS
# =====================================================================

def test_is_at_stop_within_radius():
    """is_at_stop returns True when distance <= 75m."""
    # Same point
    assert is_at_stop(17.98, 79.53, 17.98, 79.53) is True
    # ~55m offset north (0.0005 deg * 111320 = 55.66m < 75m)
    assert is_at_stop(17.9805, 79.53, 17.98, 79.53) is True


def test_is_at_stop_outside_radius():
    """is_at_stop returns False when distance > 75m."""
    # ~111m offset north (0.001 deg * 111320 = 111.32m > 75m)
    assert is_at_stop(17.981, 79.53, 17.98, 79.53) is False


def test_is_at_stop_none_coords():
    """is_at_stop handles None gracefully."""
    assert is_at_stop(None, 79.53, 17.98, 79.53) is False
    assert is_at_stop(17.98, None, 17.98, 79.53) is False
    assert is_at_stop(17.98, 79.53, None, 79.53) is False
    assert is_at_stop(17.98, 79.53, 17.98, None) is False


# =====================================================================
# API INTEGRATION TESTS
# =====================================================================

def test_manual_accept_reserves_budget_no_wait_until(wait_env):
    """
    POST /driver/wait-request/{id}/accept should:
      1. Set status = 'accepted'
      2. Reserve budget (trip.wait_budget_used += minutes)
      3. NOT set wait_until (it stays NULL until bus arrives)
    """
    env = wait_env
    db = SessionLocal()
    try:
        # Create a pending wait request directly in DB
        wr = WaitRequest(
            student_id=env['student'].id,
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            stop_id=env['stop1'].id,
            minutes=3,
            status='pending',
            created_at=datetime.utcnow(),
        )
        db.add(wr)
        db.commit()
        db.refresh(wr)
        wr_id = wr.id

        # Driver accepts
        resp = client.post(
            f'/driver/wait-request/{wr_id}/accept',
            headers=env['headers_driver'],
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data['status'] == 'accepted'
        assert data['minutes'] == 3

        # Verify DB state
        db.expire_all()
        wr_after = db.query(WaitRequest).filter(WaitRequest.id == wr_id).first()
        assert wr_after.status == 'accepted'
        assert wr_after.wait_until is None, 'wait_until must be NULL after accept (bus not at stop yet)'

        trip_after = db.query(Trip).filter(Trip.id == env['trip'].id).first()
        assert trip_after.wait_budget_used == 3, f'Budget should be reserved: expected 3, got {trip_after.wait_budget_used}'
    finally:
        db.query(WaitRequest).filter(WaitRequest.id == wr_id).delete()
        # Reset budget
        t = db.query(Trip).filter(Trip.id == env['trip'].id).first()
        if t:
            t.wait_budget_used = 0
        db.commit()
        db.close()


def test_arrival_transitions_accepted_to_waiting(wait_env):
    """
    maybe_start_wait_countdown should transition accepted -> waiting
    when bus is within 75m of the stop, and set wait_until.
    """
    env = wait_env
    db = SessionLocal()
    try:
        # Create accepted request (simulating post-accept state)
        wr = WaitRequest(
            student_id=env['student'].id,
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            stop_id=env['stop1'].id,
            minutes=2,
            status='accepted',
            wait_until=None,
            created_at=datetime.utcnow(),
        )
        db.add(wr)
        db.commit()
        db.refresh(wr)
        wr_id = wr.id

        trip = db.query(Trip).filter(Trip.id == env['trip'].id).first()

        # Bus at stop1 coordinates (exact match)
        maybe_start_wait_countdown(db, trip, 17.98000, 79.53000)

        db.expire_all()
        wr_after = db.query(WaitRequest).filter(WaitRequest.id == wr_id).first()
        assert wr_after.status == 'waiting', f'Expected waiting, got {wr_after.status}'
        assert wr_after.wait_until is not None, 'wait_until must be set after arrival'
        # wait_until should be ~2 minutes from now
        delta = (wr_after.wait_until - datetime.utcnow()).total_seconds()
        assert 90 < delta < 150, f'wait_until should be ~2min from now, got {delta:.0f}s'
    finally:
        db.query(WaitRequest).filter(WaitRequest.id == wr_id).delete()
        db.commit()
        db.close()


def test_arrival_does_not_transition_far_away(wait_env):
    """
    maybe_start_wait_countdown should NOT transition if bus is far from stop.
    """
    env = wait_env
    db = SessionLocal()
    try:
        wr = WaitRequest(
            student_id=env['student'].id,
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            stop_id=env['stop1'].id,
            minutes=2,
            status='accepted',
            wait_until=None,
            created_at=datetime.utcnow(),
        )
        db.add(wr)
        db.commit()
        db.refresh(wr)
        wr_id = wr.id

        trip = db.query(Trip).filter(Trip.id == env['trip'].id).first()

        # Bus 1km away from stop1
        maybe_start_wait_countdown(db, trip, 17.99000, 79.53000)

        db.expire_all()
        wr_after = db.query(WaitRequest).filter(WaitRequest.id == wr_id).first()
        assert wr_after.status == 'accepted', f'Should stay accepted, got {wr_after.status}'
        assert wr_after.wait_until is None
    finally:
        db.query(WaitRequest).filter(WaitRequest.id == wr_id).delete()
        db.commit()
        db.close()


def test_board_confirmation_completes_wait(wait_env):
    """
    POST /student/wait-request/{id}/board should:
      1. Only work when status == 'waiting'
      2. Set status = 'completed'
      3. Set wait_until = now (clears countdown)
    """
    env = wait_env
    db = SessionLocal()
    try:
        now = datetime.utcnow()
        wr = WaitRequest(
            student_id=env['student'].id,
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            stop_id=env['stop1'].id,
            minutes=3,
            status='waiting',
            wait_until=now + timedelta(minutes=3),
            created_at=now,
        )
        db.add(wr)
        db.commit()
        db.refresh(wr)
        wr_id = wr.id

        # Student confirms boarding
        resp = client.post(
            f'/student/wait-request/{wr_id}/board',
            headers=env['headers_student'],
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()['status'] == 'completed'

        db.expire_all()
        wr_after = db.query(WaitRequest).filter(WaitRequest.id == wr_id).first()
        assert wr_after.status == 'completed'
        # wait_until should be set to approx now (clearing the countdown)
        assert wr_after.wait_until is not None
        delta = abs((wr_after.wait_until - datetime.utcnow()).total_seconds())
        assert delta < 10, f'wait_until should be ~now, got delta {delta:.0f}s'
    finally:
        db.query(WaitRequest).filter(WaitRequest.id == wr_id).delete()
        db.commit()
        db.close()


def test_board_rejects_non_waiting_status(wait_env):
    """POST /student/wait-request/{id}/board rejects if status != waiting."""
    env = wait_env
    db = SessionLocal()
    try:
        wr = WaitRequest(
            student_id=env['student'].id,
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            stop_id=env['stop1'].id,
            minutes=2,
            status='accepted',
            created_at=datetime.utcnow(),
        )
        db.add(wr)
        db.commit()
        db.refresh(wr)
        wr_id = wr.id

        resp = client.post(
            f'/student/wait-request/{wr_id}/board',
            headers=env['headers_student'],
        )
        assert resp.status_code == 400
        assert 'not currently active' in resp.json()['detail'].lower()
    finally:
        db.query(WaitRequest).filter(WaitRequest.id == wr_id).delete()
        db.commit()
        db.close()


def test_cancel_releases_reserved_budget(wait_env):
    """
    POST /student/wait-request/{id}/cancel should:
      1. Accept pending/accepted/waiting statuses
      2. Set status = 'rejected'
      3. Release budget from trip.wait_budget_used
    """
    env = wait_env
    db = SessionLocal()
    try:
        # Pre-reserve budget (as if accept happened)
        trip = db.query(Trip).filter(Trip.id == env['trip'].id).first()
        trip.wait_budget_used = 3
        db.commit()

        wr = WaitRequest(
            student_id=env['student'].id,
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            stop_id=env['stop1'].id,
            minutes=3,
            status='accepted',
            created_at=datetime.utcnow(),
        )
        db.add(wr)
        db.commit()
        db.refresh(wr)
        wr_id = wr.id

        # Cancel
        resp = client.post(
            f'/student/wait-request/{wr_id}/cancel',
            headers=env['headers_student'],
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()['status'] == 'rejected'

        db.expire_all()
        wr_after = db.query(WaitRequest).filter(WaitRequest.id == wr_id).first()
        assert wr_after.status == 'rejected'

        trip_after = db.query(Trip).filter(Trip.id == env['trip'].id).first()
        assert trip_after.wait_budget_used == 0, (
            f'Budget should be released: expected 0, got {trip_after.wait_budget_used}'
        )
    finally:
        db.query(WaitRequest).filter(WaitRequest.id == wr_id).delete()
        t = db.query(Trip).filter(Trip.id == env['trip'].id).first()
        if t:
            t.wait_budget_used = 0
        db.commit()
        db.close()


def test_cancel_rejects_completed_status(wait_env):
    """Cancel is rejected if the request is already completed."""
    env = wait_env
    db = SessionLocal()
    try:
        wr = WaitRequest(
            student_id=env['student'].id,
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            stop_id=env['stop1'].id,
            minutes=2,
            status='completed',
            created_at=datetime.utcnow(),
        )
        db.add(wr)
        db.commit()
        db.refresh(wr)
        wr_id = wr.id

        resp = client.post(
            f'/student/wait-request/{wr_id}/cancel',
            headers=env['headers_student'],
        )
        assert resp.status_code == 400
        assert 'cannot be cancelled' in resp.json()['detail'].lower()
    finally:
        db.query(WaitRequest).filter(WaitRequest.id == wr_id).delete()
        db.commit()
        db.close()


def test_trip_end_force_closes_open_requests(wait_env):
    """
    POST /driver/end-trip should:
      - Set 'waiting' requests -> 'completed'
      - Set 'pending'/'accepted' requests -> 'rejected'
    """
    env = wait_env
    db = SessionLocal()
    try:
        now = datetime.utcnow()
        wr_pending = WaitRequest(
            student_id=env['student'].id,
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            stop_id=env['stop1'].id,
            minutes=2,
            status='pending',
            created_at=now,
        )
        wr_accepted = WaitRequest(
            student_id=env['student'].id,
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            stop_id=env['stop1'].id,
            minutes=3,
            status='accepted',
            created_at=now,
        )
        wr_waiting = WaitRequest(
            student_id=env['student'].id,
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            stop_id=env['stop1'].id,
            minutes=2,
            status='waiting',
            wait_until=now + timedelta(minutes=2),
            created_at=now,
        )
        db.add_all([wr_pending, wr_accepted, wr_waiting])
        db.commit()
        ids = [wr_pending.id, wr_accepted.id, wr_waiting.id]

        # End trip
        resp = client.post('/driver/end-trip', headers=env['headers_driver'])
        assert resp.status_code == 200, resp.text

        db.expire_all()
        for wr_id in ids:
            wr = db.query(WaitRequest).filter(WaitRequest.id == wr_id).first()
            assert wr is not None

        wr_p = db.query(WaitRequest).filter(WaitRequest.id == ids[0]).first()
        assert wr_p.status == 'rejected', f'pending should become rejected, got {wr_p.status}'

        wr_a = db.query(WaitRequest).filter(WaitRequest.id == ids[1]).first()
        assert wr_a.status == 'rejected', f'accepted should become rejected, got {wr_a.status}'

        wr_w = db.query(WaitRequest).filter(WaitRequest.id == ids[2]).first()
        assert wr_w.status == 'completed', f'waiting should become completed, got {wr_w.status}'

        # Verify trip is completed
        trip = db.query(Trip).filter(Trip.id == env['trip'].id).first()
        assert trip.status == 'completed'
    finally:
        for wr_id in ids:
            db.query(WaitRequest).filter(WaitRequest.id == wr_id).delete()
        # Re-activate trip for other tests (though fixture teardown handles it)
        t = db.query(Trip).filter(Trip.id == env['trip'].id).first()
        if t:
            t.status = 'active'
            t.ended_at = None
            t.wait_budget_used = 0
        db.commit()
        db.close()


def test_bus_location_shows_waiting_not_accepted(wait_env):
    """
    GET /buses/{id}/location should show is_waiting=True only when
    status == 'waiting' (not 'accepted').
    """
    env = wait_env
    db = SessionLocal()
    try:
        now = datetime.utcnow()

        # Add a bus location so the endpoint has data to return
        loc = BusLocation(
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            latitude=17.98,
            longitude=79.53,
            speed=0.0,
        )
        db.add(loc)
        db.commit()
        loc_id = loc.id

        # Create an 'accepted' request (no wait_until) -- should NOT show as waiting
        wr = WaitRequest(
            student_id=env['student'].id,
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            stop_id=env['stop1'].id,
            minutes=3,
            status='accepted',
            wait_until=None,
            created_at=now,
        )
        db.add(wr)
        db.commit()
        db.refresh(wr)
        wr_id = wr.id

        resp = client.get(
            f'/buses/{env["bus"].id}/location',
            headers=env['headers_student'],
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data['is_waiting'] is False, 'accepted (not waiting) should show is_waiting=False'

        # Now transition to 'waiting' with a future wait_until
        wr_db = db.query(WaitRequest).filter(WaitRequest.id == wr_id).first()
        wr_db.status = 'waiting'
        wr_db.wait_until = now + timedelta(minutes=3)
        db.commit()

        resp2 = client.get(
            f'/buses/{env["bus"].id}/location',
            headers=env['headers_student'],
        )
        assert resp2.status_code == 200
        data2 = resp2.json()
        assert data2['is_waiting'] is True, 'waiting status should show is_waiting=True'
        assert data2['wait_remaining_seconds'] is not None
        assert data2['wait_remaining_seconds'] > 0
        assert data2['wait_stop_name'] == 'WAIT_S1'
    finally:
        db.query(WaitRequest).filter(WaitRequest.id == wr_id).delete()
        db.query(BusLocation).filter(BusLocation.id == loc_id).delete()
        db.commit()
        db.close()


def test_accept_rejects_insufficient_budget(wait_env):
    """
    Accept should fail with 400 if request.minutes > remaining budget.
    """
    env = wait_env
    db = SessionLocal()
    try:
        # Use up budget
        trip = db.query(Trip).filter(Trip.id == env['trip'].id).first()
        trip.wait_budget_used = 9  # only 1 minute remaining
        db.commit()

        wr = WaitRequest(
            student_id=env['student'].id,
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            stop_id=env['stop1'].id,
            minutes=3,  # needs 3, only 1 available
            status='pending',
            created_at=datetime.utcnow(),
        )
        db.add(wr)
        db.commit()
        db.refresh(wr)
        wr_id = wr.id

        resp = client.post(
            f'/driver/wait-request/{wr_id}/accept',
            headers=env['headers_driver'],
        )
        assert resp.status_code == 400
        assert 'insufficient' in resp.json()['detail'].lower()

        # Verify status unchanged
        db.expire_all()
        wr_after = db.query(WaitRequest).filter(WaitRequest.id == wr_id).first()
        assert wr_after.status == 'pending'
    finally:
        db.query(WaitRequest).filter(WaitRequest.id == wr_id).delete()
        t = db.query(Trip).filter(Trip.id == env['trip'].id).first()
        if t:
            t.wait_budget_used = 0
        db.commit()
        db.close()


def test_accept_rejects_non_pending(wait_env):
    """Accept should reject if request is not pending."""
    env = wait_env
    db = SessionLocal()
    try:
        wr = WaitRequest(
            student_id=env['student'].id,
            bus_id=env['bus'].id,
            trip_id=env['trip'].id,
            stop_id=env['stop1'].id,
            minutes=2,
            status='accepted',  # already accepted
            created_at=datetime.utcnow(),
        )
        db.add(wr)
        db.commit()
        db.refresh(wr)
        wr_id = wr.id

        resp = client.post(
            f'/driver/wait-request/{wr_id}/accept',
            headers=env['headers_driver'],
        )
        assert resp.status_code == 400
        assert 'no longer pending' in resp.json()['detail'].lower()
    finally:
        db.query(WaitRequest).filter(WaitRequest.id == wr_id).delete()
        db.commit()
        db.close()
