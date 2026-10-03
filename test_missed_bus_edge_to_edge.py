"""Missed-bus API edge cases. Run with pytest -v -x test_missed_bus_edge_to_edge.py.

Uses isolated SQLite sessions and the existing 17.98/79.53 bend-back regression
geometry. Only road-provider IO, authentication identity, and push delivery are
stubbed; selection, progress, ETA, persistence and roster code are real.
Authentication/session security and frontend response races are out of scope.
"""
from datetime import datetime, timedelta
from math import ceil
from types import SimpleNamespace

import pytest
from fastapi import Header
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import main
import models
from database import Base
from models import User, Student, Driver, Route, Stop, Bus, Trip, BusLocation, MissedBusAllotment, Notification

PICKUP = (17.98, 79.53)
# Reused from test_replacement_progress_uses_road_position_not_nearest_stop_order.
BEND = [(17.98, 79.529), (17.99, 79.54), PICKUP, (17.98, 79.54)]
ORIGINAL = [(17.98, 79.52), (17.99, 79.525), PICKUP, (17.98, 79.54)]
NO_BUS = 'No bus is currently travelling through this route.'

@pytest.fixture
def fleet(monkeypatch):
    engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()
    # Support the user's uncommitted model scaffolding without depending on it.
    if hasattr(models, 'Campus'):
        db.add(models.Campus(id=1, name='Test', slug='edge-tests'))
        db.flush()
    def user(uid, role, name):
        extra = {'campus_id': 1} if hasattr(User, 'campus_id') else {}
        return User(id=uid, name=name, phone=f'900000{uid:04}', password_hash='unused', role=role, **extra)
    db.add_all([user(1, 'student', 'Missed Student'), user(2, 'student', 'Original Peer'),
                user(3, 'student', 'Replacement Peer')])
    buses, drivers = {}, {}
    for bid in (1, 2, 3, 4):
        db.add(user(10 + bid, 'driver', f'Driver {bid}'))
        drivers[bid] = Driver(id=bid, user_id=10 + bid, driver_code=f'EDGE_D{bid}')
        buses[bid] = Bus(id=bid, bus_number=f'EDGE_{bid}', route_id=bid, driver_id=bid, status='active')
        db.add_all([drivers[bid], Route(id=bid, name=f'Route {bid}'), buses[bid]])
    db.add_all([Stop(id=1, route_id=1, name='Original first', latitude=17.98, longitude=79.52, stop_order=1),
                Stop(id=2, route_id=1, name='Pickup', latitude=PICKUP[0], longitude=PICKUP[1], stop_order=2),
                Stop(id=3, route_id=1, name='Original last', latitude=17.98, longitude=79.54, stop_order=3)])
    for bid in (2, 3, 4):
        for index, point in enumerate((BEND[0], BEND[-1]), 1):
            db.add(Stop(id=bid * 10 + index, route_id=bid, name=f'Route {bid} stop {index}',
                        latitude=point[0], longitude=point[1], stop_order=index))
    student = Student(id=1, user_id=1, roll_number='EDGE1', bus_id=1, stop_id=2)
    db.add_all([student, Student(id=2, user_id=2, roll_number='EDGE2', bus_id=1, stop_id=3),
                Student(id=3, user_id=3, roll_number='EDGE3', bus_id=2, stop_id=22)])
    db.commit()
    paths = {1: ORIGINAL, 2: BEND, 3: BEND, 4: BEND}
    geometry_calls, pushes = [], []
    def geometry(_db, route_id, trip_type=None):
        geometry_calls.append((route_id, trip_type))
        points = paths[route_id]
        return list(reversed(points)) if trip_type == 'evening' else list(points)
    monkeypatch.setattr(main, 'get_route_polyline_points', geometry)
    monkeypatch.setattr(main.notification_manager, 'push_notification_sync', lambda uid, payload: pushes.append((uid, payload)))
    monkeypatch.setattr('notification_service._firebase', lambda: None)
    def identity(x_test_user: int = Header(default=1)):
        return {'user_id': x_test_user, 'role': 'student' if x_test_user < 10 else 'driver'}
    old_overrides = dict(main.app.dependency_overrides)
    main.app.dependency_overrides.update({main.get_db: lambda: db, main.require_student: identity,
                                         main.require_driver: identity, main.get_current_user: identity})
    client = TestClient(main.app)
    def gps(bid, point, speed=20):
        trip = db.query(Trip).filter_by(bus_id=bid, status='active').one()
        db.add(BusLocation(bus_id=bid, trip_id=trip.id, latitude=point[0], longitude=point[1],
                           speed=speed, timestamp=datetime.utcnow()))
        db.commit()
    def start(bid, direction='morning', point=None, speed=20):
        trip = Trip(bus_id=bid, driver_id=bid, route_id=bid, status='active', trip_type=direction,
                    started_at=datetime.utcnow() - timedelta(minutes=15))
        db.add(trip)
        db.commit()
        if point is not None:
            gps(bid, point, speed)
        return trip
    def ready(direction='morning'):
        start(1, direction, ORIGINAL[-1] if direction == 'morning' else ORIGINAL[0], 40)
        start(2, direction, BEND[1] if direction == 'morning' else (17.98, 79.538), 20)
    def get(path, uid=1):
        response = client.get(path, headers={'x-test-user': str(uid)})
        assert response.status_code == 200, response.text
        return response.json()
    env = SimpleNamespace(db=db, client=client, student=student, buses=buses, start=start, gps=gps,
                          ready=ready, get=get, paths=paths, geometry_calls=geometry_calls, pushes=pushes,
                          allot=lambda: client.post('/student/missed-bus/allot', json={}),
                          roster=lambda bid: get('/driver/route-stops', 10 + bid))
    yield env
    client.close()
    main.app.dependency_overrides.clear()
    main.app.dependency_overrides.update(old_overrides)
    db.close()
    engine.dispose()

def rejected(f, message):
    response = f.allot()
    assert response.status_code == 400, response.text
    assert response.json()['detail'] == message
    assert f.db.query(MissedBusAllotment).count() == 0
    assert main.get_current_bus_id(f.student, f.db) == 1

def allotted(f):
    response = f.allot()
    assert response.status_code == 200, response.text
    return response.json()

def test_01_original_not_started(fleet):
    fleet.start(2, point=BEND[1])
    rejected(fleet, "Your bus hasn't started yet.")

@pytest.mark.parametrize('direction', ['morning', 'evening'])
def test_02_original_not_passed(fleet, direction):
    fleet.start(1, direction, ORIGINAL[0] if direction == 'morning' else ORIGINAL[-1])
    fleet.start(2, direction, BEND[1] if direction == 'morning' else BEND[-1])
    rejected(fleet, 'Your bus has not reached your stop yet.')

@pytest.mark.parametrize('direction', ['morning', 'evening'])
def test_03_and_08_valid_replacement_live_details(fleet, direction):
    f = fleet
    f.ready(direction)
    data = allotted(f)
    assert (data['alternative_bus_id'], data['alternative_bus_number'], data['stop_id']) == (2, 'EDGE_2', 2)
    point = BEND[1] if direction == 'morning' else (17.98, 79.538)
    expected_eta = max(1, ceil(main.haversine_km(*point, *PICKUP) / 20 * 60))
    assert data['eta_minutes'] == expected_eta
    bus = f.get('/student/my-bus')
    assert (bus['bus_id'], bus['driver_name'], bus['driver_phone']) == (2, 'Driver 2', '9000000012')
    assert (bus['location']['latitude'], bus['location']['longitude']) == point
    assert f.get('/student/my-stop')['bus_id'] == 2
    assert f.get('/student/my-stop')['stop_id'] == 2
    assert (2, direction) in f.geometry_calls
    # In the morning the bus is nearest the last registered route stop, yet it
    # has not reached this pickup along the bend-back road. No order shortcut.
    if direction == 'morning':
        assert main.has_passed_stop(f.db, f.db.query(Trip).filter_by(bus_id=2).one(), f.db.get(Stop, 2))
        assert not main.replacement_bus_has_passed_stop(f.db, f.db.query(Trip).filter_by(bus_id=2).one(), f.db.get(Stop, 2))

def test_04_no_active_replacement(fleet):
    fleet.start(1, point=ORIGINAL[-1])
    rejected(fleet, NO_BUS)

@pytest.mark.parametrize('direction', ['morning', 'evening'])
def test_05_replacement_already_passed(fleet, direction):
    fleet.ready(direction)
    fleet.gps(2, (17.98, 79.536) if direction == 'morning' else BEND[1])
    rejected(fleet, NO_BUS)

@pytest.mark.parametrize('direction', ['morning', 'evening'])
def test_06_and_08_approaching_route_does_not_cover_pickup(fleet, direction):
    fleet.ready(direction)
    # Translate the entire curved road 330m north; proximity is not coverage.
    fleet.paths[2] = [(lat + .003, lng) for lat, lng in BEND]
    point = BEND[1] if direction == 'morning' else (17.98, 79.538)
    fleet.gps(2, (point[0] + .003, point[1]))
    rejected(fleet, NO_BUS)
    assert (2, direction) in fleet.geometry_calls

@pytest.mark.parametrize('ranking', ['eta', 'distance', 'bus_id'])
def test_07_multiple_candidates_deterministic(fleet, ranking):
    f = fleet
    f.ready()
    if ranking == 'eta':
        # Bus 3 is faster despite being at the same distance.
        f.start(3, point=BEND[1], speed=40)
        winner = 3
    elif ranking == 'distance':
        # Both ETAs ceil to one minute: shorter GPS distance wins.
        f.gps(2, (17.982, 79.532), speed=40)
        f.start(3, point=(17.981, 79.531), speed=40)
        winner = 3
    else:
        f.start(3, point=BEND[1], speed=20)
        winner = 2
    for _ in range(3):
        candidates, _, _ = main.find_candidate_buses_for_location(f.db, stop_id=2, lat=PICKUP[0], lng=PICKUP[1], exclude_bus_id=1, require_active_trip=True)
        assert candidates[0]['bus_id'] == winner
    assert allotted(f)['alternative_bus_id'] == winner

@pytest.mark.parametrize('direction', ['morning', 'evening'])
def test_09_driver_pickup_location_eta_and_refresh_events(fleet, direction):
    f = fleet
    f.ready(direction)
    data = allotted(f)
    roster = f.roster(2)
    pickup = next(s for s in roster['stops'] if s['stop_id'] == 2)
    assert (pickup['latitude'], pickup['longitude'], pickup['student_count']) == (*PICKUP, 1)
    live = f.get('/buses/2/location', 12)
    trip = f.db.query(Trip).filter_by(bus_id=2).one()
    assert live['trip_id'] == trip.id
    assert main.get_eta_minutes_to_stop(f.db, 2, trip.id, f.db.get(Stop, 2)) == data['eta_minutes']
    events = [(uid, p['bus_id']) for uid, p in f.pushes if p['type'] == 'bus_roster_changed']
    assert sorted(events) == [(11, 1), (12, 2)]
    # ETA is calculated by the client from this payload, not a driver API field.
    assert 'eta_minutes' not in pickup

def test_10_original_driver_route_and_peer_preserved(fleet):
    f = fleet
    f.ready()
    before, live_before = f.roster(1), f.get('/buses/1/location', 11)
    allotted(f)
    after = f.roster(1)
    for old, new in zip(before['stops'], after['stops']):
        assert {k: v for k, v in old.items() if k != 'student_count'} == {k: v for k, v in new.items() if k != 'student_count'}
        assert new['student_count'] == old['student_count'] - (old['stop_id'] == 2)
    assert f.get('/buses/1/location', 11) == live_before
    assert main.get_current_bus_id(f.db.get(Student, 2), f.db) == 1

def test_11_counts_transfer_without_double_counting(fleet):
    f = fleet
    f.ready()
    before = {bid: f.roster(bid) for bid in (1, 2)}
    first = allotted(f)
    assert allotted(f)['allotment_id'] == first['allotment_id']
    for bid, delta in ((1, -1), (2, 1)):
        after = f.roster(bid)
        for key in ('total_students_today', 'total_assigned_students'):
            assert after[key] == before[bid][key] + delta
        assert sum(s['student_count'] for s in after['stops']) == after['total_students_today']
    assert f.db.query(MissedBusAllotment).count() == 1

def test_12_delayed_original_gps_cannot_restore_old_assignment(fleet):
    f = fleet
    f.ready()
    first = allotted(f)
    replacement_before = f.get('/buses/2/location')
    # A previously sent packet arrives now, after the allocation transaction.
    response = f.client.post('/buses/1/location', headers={'x-test-user': '11'},
                             json={'latitude': PICKUP[0], 'longitude': PICKUP[1], 'speed': 99})
    assert response.status_code == 200, response.text
    assert f.get('/student/my-bus')['bus_id'] == 2
    assert f.get('/student/my-stop')['bus_id'] == 2
    assert f.get('/student/missed-bus/allotment')['allotment_id'] == first['allotment_id']
    assert f.get('/buses/2/location') == replacement_before
    assert f.client.get('/buses/1/location').status_code == 403
    assert f.db.query(Notification).filter_by(user_id=1, type='bus_approaching', related_bus_id=1).count() == 0

def test_13_second_miss_requires_explicit_recovery_or_rejection(fleet):
    f = fleet
    f.ready()
    first = allotted(f)
    f.start(3, point=BEND[1])  # a genuinely available recovery candidate
    response = f.client.post('/buses/2/location', headers={'x-test-user': '12'},
                             json={'latitude': 17.98, 'longitude': 79.536, 'speed': 20})
    assert response.status_code == 200, response.text
    trip = f.db.query(Trip).filter_by(bus_id=2).one()
    assert main.replacement_bus_has_passed_stop(f.db, trip, f.db.get(Stop, 2))
    status = f.get('/student/missed-bus/allotment')
    retry = f.allot()
    stale_success = retry.status_code == 200 and retry.json().get('allotment_id') == first['allotment_id']
    assert not (status['active'] and stale_success), (
        'PILOT DECISION: replacement passed the pickup, but allotment stays active and retry returns '
        f'the same bus with HTTP {retry.status_code}: {retry.json()}. Bus 3 is still approaching. '
        'No auto-reallocation or explicit rejection; student remains assigned until trip ends or expiry.'
    )
