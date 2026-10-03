"""Focused coverage for the map_stops additions on both student route APIs."""

import os
from datetime import date, timedelta
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "sqlite://")
os.environ.setdefault("JWT_SECRET", "test-only-secret-not-for-production-0123456789")

import pytest
from fastapi import Header
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import main
import models
from database import Base
from models import (
    Bus,
    Driver,
    MissedBusAllotment,
    Route,
    Stop,
    Student,
    TemporaryStopChange,
    TravelStatus,
    Trip,
    User,
)


@pytest.fixture
def fleet():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()

    if hasattr(models, "Campus"):
        db.add(models.Campus(id=1, name="Map Test Campus", slug="map-test"))
        db.flush()

    def user(user_id, role, name):
        return User(
            id=user_id,
            name=name,
            phone=f"900000{user_id:04}",
            password_hash="unused",
            role=role,
        )

    db.add_all([
        user(1, "student", "Missed Student"),
        user(2, "student", "Normal Student"),
        user(3, "student", "Replacement Student"),
        user(11, "driver", "Driver One"),
        user(12, "driver", "Driver Two"),
    ])
    db.add_all([
        Route(id=1, name="Original Route"),
        Route(id=2, name="Replacement Route"),
        Driver(id=1, user_id=11, driver_code="MAP_D1"),
        Driver(id=2, user_id=12, driver_code="MAP_D2"),
        Bus(id=1, bus_number="MAP_1", route_id=1, driver_id=1, status="active"),
        Bus(id=2, bus_number="MAP_2", route_id=2, driver_id=2, status="active"),
    ])
    db.add_all([
        Stop(id=1, route_id=1, name="Original One", latitude=17.98, longitude=79.52, stop_order=1),
        Stop(id=2, route_id=1, name="Original Pickup", latitude=17.98, longitude=79.53, stop_order=2),
        Stop(id=3, route_id=1, name="Original Three", latitude=17.98, longitude=79.54, stop_order=3),
        Stop(id=21, route_id=2, name="Replacement One", latitude=18.00, longitude=79.50, stop_order=1),
        Stop(id=22, route_id=2, name="Replacement Two", latitude=18.01, longitude=79.51, stop_order=2),
    ])
    db.add_all([
        Student(id=1, user_id=1, roll_number="MAP1", bus_id=1, stop_id=2),
        Student(id=2, user_id=2, roll_number="MAP2", bus_id=1, stop_id=3),
        Student(id=3, user_id=3, roll_number="MAP3", bus_id=2, stop_id=21),
    ])
    db.commit()

    def identity(x_test_user: int = Header(default=1)):
        return {"user_id": x_test_user, "role": "student"}

    old_overrides = dict(main.app.dependency_overrides)
    main.app.dependency_overrides.update({
        main.get_db: lambda: db,
        main.require_student: identity,
    })
    client = TestClient(main.app)
    yield SimpleNamespace(db=db, client=client)
    client.close()
    main.app.dependency_overrides.clear()
    main.app.dependency_overrides.update(old_overrides)
    db.close()
    engine.dispose()


def get_json(fleet, path):
    response = fleet.client.get(path, headers={"x-test-user": "1"})
    assert response.status_code == 200, response.text
    return response.json()


def active_trip(fleet, trip_type="morning"):
    trip = Trip(
        bus_id=2,
        driver_id=2,
        route_id=2,
        status="active",
        trip_type=trip_type,
    )
    fleet.db.add(trip)
    fleet.db.commit()
    return trip


def test_missed_bus_pickup_appears_and_is_affected(fleet):
    trip = active_trip(fleet, "morning")
    fleet.db.add(MissedBusAllotment(
        student_id=1,
        alternative_bus_id=2,
        alternative_trip_id=trip.id,
        stop_id=2,
        status="active",
        expires_at=None,
    ))
    fleet.db.commit()

    data = get_json(fleet, "/student/my-route-stops")
    pickup = next(stop for stop in data["map_stops"] if stop["stop_id"] == 2)
    assert pickup["affected_directions"] == ["morning"]
    assert pickup["directions"] == ["morning"]


def test_morning_and_evening_temporary_stops_are_separate(fleet):
    fleet.db.add_all([
        Stop(id=90, route_id=2, name="Morning Temporary", latitude=18.02, longitude=79.52,
             stop_order=3, is_custom=True),
        Stop(id=91, route_id=2, name="Evening Temporary", latitude=18.03, longitude=79.53,
             stop_order=4, is_custom=True),
        TemporaryStopChange(
            student_id=1,
            original_stop_id=2,
            temporary_stop_id=90,
            morning_temporary_stop_id=90,
            evening_temporary_stop_id=91,
            target_bus_id=2,
            start_date=date.today(),
            end_date=date.today(),
            status="active",
        ),
    ])
    fleet.db.commit()

    stops = {stop["stop_id"]: stop for stop in get_json(fleet, "/student/my-route-stops")["map_stops"]}
    assert stops[90]["affected_directions"] == ["morning"]
    assert stops[91]["affected_directions"] == ["evening"]
    assert stops[90]["directions"] == ["morning"]
    assert stops[91]["directions"] == ["evening"]


def test_replacement_stop_is_flagged_once(fleet):
    trip = active_trip(fleet, "morning")
    fleet.db.add(MissedBusAllotment(
        student_id=1,
        alternative_bus_id=2,
        alternative_trip_id=trip.id,
        stop_id=21,
        status="active",
    ))
    fleet.db.commit()

    buses = get_json(fleet, "/student/all-bus-routes")["buses"]
    replacement = next(bus for bus in buses if bus["bus_id"] == 2)["map_stops"]
    matches = [stop for stop in replacement if stop["stop_id"] == 21]
    assert len(matches) == 1
    assert matches[0]["affected_directions"] == ["morning"]


def test_not_travelling_students_are_excluded(fleet):
    fleet.db.add_all([
        Trip(bus_id=2, driver_id=2, route_id=2, status="active", trip_type="morning"),
        MissedBusAllotment(student_id=1, alternative_bus_id=2, alternative_trip_id=1,
                           stop_id=2, status="active"),
        TravelStatus(student_id=1, date=date.today(), status="not_travelling"),
    ])
    fleet.db.commit()
    # Point the allotment at the actual active trip after its insert-generated id exists.
    trip = fleet.db.query(Trip).filter(Trip.bus_id == 2).one()
    fleet.db.query(MissedBusAllotment).update({"alternative_trip_id": trip.id})
    fleet.db.commit()

    map_stops = get_json(fleet, "/student/my-route-stops")["map_stops"]
    assert 2 not in {stop["stop_id"] for stop in map_stops}


def test_existing_stops_fields_are_unchanged(fleet):
    student = get_json(fleet, "/student/my-route-stops")
    assert student["stops"] == [
        {"stop_id": 1, "name": "Original One", "latitude": 17.98, "longitude": 79.52,
         "morning_latitude": 17.98, "morning_longitude": 79.52,
         "evening_latitude": None, "evening_longitude": None, "stop_order": 1},
        {"stop_id": 2, "name": "Original Pickup", "latitude": 17.98, "longitude": 79.53,
         "morning_latitude": 17.98, "morning_longitude": 79.53,
         "evening_latitude": None, "evening_longitude": None, "stop_order": 2},
        {"stop_id": 3, "name": "Original Three", "latitude": 17.98, "longitude": 79.54,
         "morning_latitude": 17.98, "morning_longitude": 79.54,
         "evening_latitude": None, "evening_longitude": None, "stop_order": 3},
    ]

    all_routes = get_json(fleet, "/student/all-bus-routes")
    bus_one = next(bus for bus in all_routes["buses"] if bus["bus_id"] == 1)
    bus_two = next(bus for bus in all_routes["buses"] if bus["bus_id"] == 2)
    assert [stop["stop_id"] for stop in bus_one["stops"]] == [1, 2, 3]
    assert [stop["stop_id"] for stop in bus_two["stops"]] == [21, 22]


def test_all_bus_routes_hides_another_students_custom_pin(fleet):
    fleet.db.add_all([
        Stop(id=90, route_id=1, name="Student A Custom Pin", latitude=18.1, longitude=79.6,
             stop_order=4, is_custom=True),
        TemporaryStopChange(
            student_id=1,
            original_stop_id=2,
            temporary_stop_id=90,
            morning_temporary_stop_id=90,
            target_bus_id=1,
            start_date=date.today(),
            end_date=date.today(),
            status="active",
        ),
    ])
    fleet.db.commit()

    response = fleet.client.get("/student/all-bus-routes", headers={"x-test-user": "2"})
    assert response.status_code == 200, response.text
    bus_one = next(bus for bus in response.json()["buses"] if bus["bus_id"] == 1)
    assert 90 not in {stop["stop_id"] for stop in bus_one["map_stops"]}


def test_my_route_stops_hides_other_passenger_pin_keeps_requesters_pin(fleet):
    fleet.db.add_all([
        Stop(id=91, route_id=1, name="Student A Custom Pin", latitude=18.11, longitude=79.61,
             stop_order=4, is_custom=True),
        Stop(id=92, route_id=1, name="Student B Custom Pin", latitude=18.12, longitude=79.62,
             stop_order=5, is_custom=True),
        TemporaryStopChange(
            student_id=1,
            original_stop_id=2,
            temporary_stop_id=91,
            morning_temporary_stop_id=91,
            target_bus_id=1,
            start_date=date.today(),
            end_date=date.today(),
            status="active",
        ),
        TemporaryStopChange(
            student_id=2,
            original_stop_id=3,
            temporary_stop_id=92,
            morning_temporary_stop_id=92,
            target_bus_id=1,
            start_date=date.today(),
            end_date=date.today(),
            status="active",
        ),
    ])
    fleet.db.commit()

    response = fleet.client.get("/student/my-route-stops", headers={"x-test-user": "2"})
    assert response.status_code == 200, response.text
    map_stop_ids = {stop["stop_id"] for stop in response.json()["map_stops"]}
    assert 91 not in map_stop_ids
    assert 92 in map_stop_ids
