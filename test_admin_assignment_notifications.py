"""
Unit and integration tests for student assignment change notifications.
Covers:
  - PATCH /admin/students/{id}/bus -> "bus_changed" notification
  - PATCH /admin/students/{id} with bus_id -> "bus_changed" notification
  - PATCH /admin/students/{id}/stop -> "stop_changed" notification
  - Editing only name/phone -> NO notification
  - Bus change to different route clears stop and prompts to select stop again
  - Both bus and stop changed in one edit -> only bus notification
"""
import pytest
from database import SessionLocal
from models import User, Student, Route, Bus, Stop, Notification
from auth import hash_password, create_access_token
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


@pytest.fixture
def db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def admin_notif_env(db_session):
    db = db_session

    # Clean up any leftover test records
    db.query(Notification).filter(Notification.user_id.in_(
        db.query(User.id).filter(User.email.like("test_notif_%@kambus.test"))
    )).delete(synchronize_session=False)
    db.query(Student).filter(Student.roll_number.like("NOTIF_STU_%")).delete(synchronize_session=False)
    db.query(Bus).filter(Bus.bus_number.like("NOTIF_BUS_%")).delete(synchronize_session=False)
    db.query(Stop).filter(Stop.name.like("NOTIF_STOP_%")).delete(synchronize_session=False)
    db.query(Route).filter(Route.name.like("NOTIF_ROUTE_%")).delete(synchronize_session=False)
    db.query(User).filter(User.email.like("test_notif_%@kambus.test")).delete(synchronize_session=False)
    db.commit()

    # Create Admin User
    u_admin = User(
        name="Notif Admin",
        email="test_notif_admin@kambus.test",
        phone="9988776601",
        password_hash=hash_password("adminpass"),
        role="admin",
        is_verified=True,
    )
    # Create Student User
    u_student = User(
        name="Notif Student",
        email="test_notif_student@kambus.test",
        phone="9988776602",
        password_hash=hash_password("stupass"),
        role="student",
        is_verified=True,
    )
    db.add_all([u_admin, u_student])
    db.commit()

    # Create 2 Routes
    r1 = Route(name="NOTIF_ROUTE_1", description="Route 1")
    r2 = Route(name="NOTIF_ROUTE_2", description="Route 2")
    db.add_all([r1, r2])
    db.commit()

    # Create Stops
    stop1_a = Stop(route_id=r1.id, name="NOTIF_STOP_1A", latitude=17.980, longitude=79.530, stop_order=1, is_active=True)
    stop1_b = Stop(route_id=r1.id, name="NOTIF_STOP_1B", latitude=17.981, longitude=79.531, stop_order=2, is_active=True)
    stop2_a = Stop(route_id=r2.id, name="NOTIF_STOP_2A", latitude=17.950, longitude=79.500, stop_order=1, is_active=True)
    db.add_all([stop1_a, stop1_b, stop2_a])
    db.commit()

    # Create Buses
    bus1 = Bus(bus_number="NOTIF_BUS_1", route_id=r1.id, status="active")
    bus2 = Bus(bus_number="NOTIF_BUS_2", route_id=r2.id, status="active")
    db.add_all([bus1, bus2])
    db.commit()

    # Create Student assigned to Bus 1, Stop 1A
    student = Student(
        user_id=u_student.id,
        roll_number="NOTIF_STU_1",
        department="CSE",
        bus_id=bus1.id,
        stop_id=stop1_a.id,
    )
    db.add(student)
    db.commit()
    db.refresh(student)

    token_admin = create_access_token(u_admin.id, "admin")
    headers_admin = {"Authorization": f"Bearer {token_admin}"}

    yield {
        "admin_user": u_admin,
        "student_user": u_student,
        "student": student,
        "route1": r1,
        "route2": r2,
        "stop1_a": stop1_a,
        "stop1_b": stop1_b,
        "stop2_a": stop2_a,
        "bus1": bus1,
        "bus2": bus2,
        "headers_admin": headers_admin,
    }

    # Teardown
    db.query(Notification).filter(Notification.user_id.in_([u_admin.id, u_student.id])).delete(synchronize_session=False)
    db.query(Student).filter(Student.id == student.id).delete(synchronize_session=False)
    db.query(Bus).filter(Bus.id.in_([bus1.id, bus2.id])).delete(synchronize_session=False)
    db.query(Stop).filter(Stop.id.in_([stop1_a.id, stop1_b.id, stop2_a.id])).delete(synchronize_session=False)
    db.query(Route).filter(Route.id.in_([r1.id, r2.id])).delete(synchronize_session=False)
    db.query(User).filter(User.id.in_([u_admin.id, u_student.id])).delete(synchronize_session=False)
    db.commit()


def test_admin_change_bus_via_bus_endpoint(admin_notif_env, db_session):
    """
    Admin changes student's bus via PATCH /admin/students/{id}/bus
    -> Notification of type 'bus_changed' created for student.
    """
    env = admin_notif_env
    stu_id = env["student"].id
    new_bus_id = env["bus2"].id

    resp = client.patch(
        f"/admin/students/{stu_id}/bus",
        json={"bus_id": new_bus_id},
        headers=env["headers_admin"],
    )
    assert resp.status_code == 200, resp.text

    notifs = (
        db_session.query(Notification)
        .filter(Notification.user_id == env["student_user"].id)
        .all()
    )
    assert len(notifs) == 1
    n = notifs[0]
    assert n.type == "bus_changed"
    assert n.title == "Bus Changed"
    assert env["bus2"].bus_number in n.message


def test_admin_change_bus_via_update_student_endpoint(admin_notif_env, db_session):
    """
    Admin changes student's bus via PATCH /admin/students/{id}
    -> Notification of type 'bus_changed' created for student.
    """
    env = admin_notif_env
    stu_id = env["student"].id
    new_bus_id = env["bus2"].id

    resp = client.patch(
        f"/admin/students/{stu_id}",
        json={"bus_id": new_bus_id},
        headers=env["headers_admin"],
    )
    assert resp.status_code == 200, resp.text

    notifs = (
        db_session.query(Notification)
        .filter(Notification.user_id == env["student_user"].id)
        .all()
    )
    assert len(notifs) == 1
    n = notifs[0]
    assert n.type == "bus_changed"
    assert n.title == "Bus Changed"
    assert env["bus2"].bus_number in n.message


def test_admin_change_stop_only(admin_notif_env, db_session):
    """
    Admin changes only student's stop via PATCH /admin/students/{id}/stop
    -> Notification of type 'stop_changed' created for student.
    """
    env = admin_notif_env
    stu_id = env["student"].id
    new_stop_id = env["stop1_b"].id

    resp = client.patch(
        f"/admin/students/{stu_id}/stop",
        json={"stop_id": new_stop_id},
        headers=env["headers_admin"],
    )
    assert resp.status_code == 200, resp.text

    notifs = (
        db_session.query(Notification)
        .filter(Notification.user_id == env["student_user"].id)
        .all()
    )
    assert len(notifs) == 1
    n = notifs[0]
    assert n.type == "stop_changed"
    assert n.title == "Stop Changed"
    assert env["stop1_b"].name in n.message


def test_admin_edit_only_student_name_no_notification(admin_notif_env, db_session):
    """
    Admin edits only student's name via PATCH /admin/students/{id}
    -> NO Notification is created.
    """
    env = admin_notif_env
    stu_id = env["student"].id

    resp = client.patch(
        f"/admin/students/{stu_id}",
        json={"name": "Renamed Student"},
        headers=env["headers_admin"],
    )
    assert resp.status_code == 200, resp.text

    notifs = (
        db_session.query(Notification)
        .filter(Notification.user_id == env["student_user"].id)
        .all()
    )
    assert len(notifs) == 0


def test_bus_change_different_route_clears_stop_and_includes_prompt(admin_notif_env, db_session):
    """
    Bus change to a bus on a different route clears student's stop
    and the notification message contains 'select your stop again'.
    """
    env = admin_notif_env
    stu_id = env["student"].id
    new_bus_id = env["bus2"].id  # Route 2, while stop was Route 1

    resp = client.patch(
        f"/admin/students/{stu_id}/bus",
        json={"bus_id": new_bus_id},
        headers=env["headers_admin"],
    )
    assert resp.status_code == 200, resp.text

    db_session.expire_all()
    stu = db_session.query(Student).filter(Student.id == stu_id).first()
    assert stu.stop_id is None, "Stop should have been cleared because route differed"

    notifs = (
        db_session.query(Notification)
        .filter(Notification.user_id == env["student_user"].id)
        .all()
    )
    assert len(notifs) == 1
    n = notifs[0]
    assert n.type == "bus_changed"
    assert "select your stop again" in n.message.lower()


def test_both_bus_and_stop_changed_in_one_edit_only_bus_notification(admin_notif_env, db_session):
    """
    When both bus and stop are changed in one edit via PATCH /admin/students/{id},
    only the 'bus_changed' notification is emitted.
    """
    env = admin_notif_env
    stu_id = env["student"].id
    new_bus_id = env["bus2"].id
    new_stop_id = env["stop2_a"].id

    resp = client.patch(
        f"/admin/students/{stu_id}",
        json={"bus_id": new_bus_id, "stop_id": new_stop_id},
        headers=env["headers_admin"],
    )
    assert resp.status_code == 200, resp.text

    notifs = (
        db_session.query(Notification)
        .filter(Notification.user_id == env["student_user"].id)
        .all()
    )
    assert len(notifs) == 1
    assert notifs[0].type == "bus_changed"
