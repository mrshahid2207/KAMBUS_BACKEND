"""One-off live integration checks for the deployed KAMBUS API.

This script deliberately talks to a real API.  It never hard-codes credentials.
Set KAMBUS_TEST_ALLOW_PRODUCTION_WRITES=yes before it creates any records.

Required environment variables:
  KAMBUS_TEST_ADMIN_IDENTIFIER, KAMBUS_TEST_ADMIN_PASSWORD
  KAMBUS_TEST_SOURCE_STUDENT_IDENTIFIER, KAMBUS_TEST_SOURCE_STUDENT_PASSWORD
  KAMBUS_TEST_TARGET_STUDENT_IDENTIFIER, KAMBUS_TEST_TARGET_STUDENT_PASSWORD

Optional: KAMBUS_TEST_DRIVER_IDENTIFIER / KAMBUS_TEST_DRIVER_PASSWORD for the
best-effort wait-request check.  Test-created records use a per-run nonce.
Some audit/history records (bus changes, complaints, completed trips) are
intentionally retained by the API and cannot be deleted through an endpoint.
"""
from __future__ import annotations

import os
import secrets
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import requests


BASE_URL = os.getenv("KAMBUS_TEST_BASE_URL", "https://kambus-backend.onrender.com").rstrip("/")
TIMEOUT = int(os.getenv("KAMBUS_TEST_TIMEOUT", "30"))
WRITE_GUARD = os.getenv("KAMBUS_TEST_ALLOW_PRODUCTION_WRITES") == "yes"
SOURCE_BUS_NUMBER = os.getenv("KAMBUS_TEST_SOURCE_BUS_NUMBER", "3")
TARGET_BUS_NUMBER = os.getenv("KAMBUS_TEST_TARGET_BUS_NUMBER", "4")
RUN_ID = secrets.token_hex(4)
TEST_GROUPS = {
    value.strip().lower()
    for value in os.getenv("KAMBUS_TEST_GROUPS", "").split(",")
    if value.strip()
}


class SkipTest(RuntimeError):
    """A live precondition was unavailable, not an assertion failure."""


@dataclass
class Outcome:
    name: str
    state: str
    detail: str


class Api:
    def __init__(self) -> None:
        self.session = requests.Session()

    def call(self, method: str, path: str, token: str | None = None, *, expected: int | tuple[int, ...] = 200,
             json: dict[str, Any] | None = None) -> dict[str, Any]:
        # Retrying an uncertain POST could duplicate a production write.  Only
        # retry safe GETs and the credential-only login request on a gateway error.
        retries = 3 if method == "GET" or path == "/auth/login" else 1
        response = None
        for attempt in range(retries):
            response = self.session.request(
                method, f"{BASE_URL}{path}", json=json,
                headers={"Authorization": f"Bearer {token}"} if token else {}, timeout=TIMEOUT,
            )
            if response.status_code not in {502, 503, 504, 520, 521, 522, 523, 524} or attempt == retries - 1:
                break
            time.sleep(attempt + 1)
        assert response is not None
        allowed = (expected,) if isinstance(expected, int) else expected
        if response.status_code not in allowed:
            try:
                body: Any = response.json()
            except ValueError:
                body = response.text[:500]
            raise AssertionError(f"{method} {path}: expected {allowed}, got {response.status_code}: {body}")
        try:
            return response.json()
        except ValueError as exc:
            raise AssertionError(f"{method} {path}: response was not JSON") from exc

    def login(self, role: str, identifier: str, password: str) -> str:
        body = self.call("POST", "/auth/login", expected=200, json={
            "role": role, "identifier": identifier, "password": password,
        })
        token = body.get("access_token")
        assert token, f"{role} login returned no access_token"
        return token


api = Api()
outcomes: list[Outcome] = []
test_cases: list[tuple[str, Any]] = []
created_changes: list[int] = []
created_waits: list[tuple[int, str]] = []
created_students: list[int] = []
created_drivers: list[int] = []
created_buses: list[int] = []


def env(name: str, required: bool = True) -> str | None:
    value = os.getenv(name)
    if required and not value:
        raise SkipTest(f"missing environment variable {name}")
    return value


def test(name: str):
    def decorator(fn):
        test_cases.append((name, fn))
        return fn
    return decorator


def auth_config() -> dict[str, str]:
    return {
        "admin": api.login("admin", env("KAMBUS_TEST_ADMIN_IDENTIFIER"), env("KAMBUS_TEST_ADMIN_PASSWORD")),
        "source": api.login("student", env("KAMBUS_TEST_SOURCE_STUDENT_IDENTIFIER"), env("KAMBUS_TEST_SOURCE_STUDENT_PASSWORD")),
        "target": api.login("student", env("KAMBUS_TEST_TARGET_STUDENT_IDENTIFIER"), env("KAMBUS_TEST_TARGET_STUDENT_PASSWORD")),
    }


tokens: dict[str, str] = {}
bus_ids: dict[str, int] = {}


def live_buses(admin_token: str) -> tuple[dict[str, Any], dict[str, Any]]:
    body = api.call("GET", "/admin/buses", admin_token)
    buses = body.get("buses", [])
    source = next((b for b in buses if str(b.get("bus_number")) == SOURCE_BUS_NUMBER), None)
    target = next((b for b in buses if str(b.get("bus_number")) == TARGET_BUS_NUMBER), None)
    assert source and target, f"could not find configured buses {SOURCE_BUS_NUMBER!r} and {TARGET_BUS_NUMBER!r}"
    return source, target


def create_bus_change(admin: str, start: date, end: date, label: str) -> int:
    body = api.call("POST", "/admin/bus-changes", admin, expected=201, json={
        "source_bus_id": bus_ids["source"], "target_bus_id": bus_ids["target"],
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "title": f"LIVE TEST {RUN_ID} {label}", "message": "Disposable integration-test bus change.",
        "template_type": "TEMPLATE_B",
    })
    change_id = body.get("bus_change_id")
    assert change_id, f"bus change response lacked id: {body}"
    created_changes.append(int(change_id))
    return int(change_id)


def current_bus(token: str) -> int:
    body = api.call("GET", "/student/my-bus", token)
    value = body.get("bus_id")
    assert value is not None, f"my-bus response lacked bus_id: {body}"
    return int(value)


@test("preflight: authenticate and resolve configured buses")
def _preflight() -> None:
    if not WRITE_GUARD:
        raise SkipTest("set KAMBUS_TEST_ALLOW_PRODUCTION_WRITES=yes to enable live writes")
    tokens.update(auth_config())
    source, target = live_buses(tokens["admin"])
    bus_ids.update(source=int(source["bus_id"]), target=int(target["bus_id"]))
    assert current_bus(tokens["source"]) == bus_ids["source"], "source student is not currently on source bus"
    assert current_bus(tokens["target"]) == bus_ids["target"], "target student is not currently on target bus"


@test("group 1: active admin bus change resolves and cancels")
def _bus_change_lifecycle() -> None:
    if not tokens:
        raise SkipTest("preflight did not succeed")
    change_id = create_bus_change(tokens["admin"], date.today(), date.today() + timedelta(days=1), "ACTIVE")
    assert current_bus(tokens["source"]) == bus_ids["target"], "source student did not resolve to target bus"
    listed = api.call("GET", "/admin/bus-changes", tokens["admin"]).get("bus_changes", [])
    active = next((c for c in listed if int(c["id"]) == change_id), None)
    assert active and active["status"] == "active", f"active change missing/wrong state: {active}"
    api.call("POST", f"/admin/bus-changes/{change_id}/cancel", tokens["admin"])
    assert current_bus(tokens["source"]) == bus_ids["source"], "cancelling did not restore source bus"
    created_changes.remove(change_id)


@test("group 1: past-dated bus change expires lazily")
def _past_change() -> None:
    if not tokens:
        raise SkipTest("preflight did not succeed")
    yesterday = date.today() - timedelta(days=1)
    change_id = create_bus_change(tokens["admin"], yesterday, yesterday, "PAST")
    listed = api.call("GET", "/admin/bus-changes", tokens["admin"]).get("bus_changes", [])
    item = next((c for c in listed if int(c["id"]) == change_id), None)
    assert item and item["status"] == "expired", f"past change was not expired: {item}"
    created_changes.remove(change_id)  # terminal audit row intentionally remains


@test("group 3: complaint uses effective bus and rejects unrelated student")
def _complaint_routing() -> None:
    if not tokens:
        raise SkipTest("preflight did not succeed")
    change_id = create_bus_change(tokens["admin"], date.today(), date.today() + timedelta(days=1), "COMPLAINT")
    assert current_bus(tokens["source"]) == bus_ids["target"]
    complaint = api.call("POST", "/student/driver-complaint", tokens["source"], expected=200, json={
        "reason": "driver_not_on_time", "description": f"Disposable integration-test complaint {RUN_ID}",
    })
    complaint_id = int(complaint["complaint_id"])
    assert int(complaint["bus_id"]) == bus_ids["target"], "complaint was attached to permanent rather than effective bus"
    verified = api.call("POST", "/student/driver-complaint/verify", tokens["target"], expected=200,
                        json={"complaint_id": complaint_id, "response": "no"})
    assert int(verified["complaint_id"]) == complaint_id

    # A short-lived unassigned student proves the 403 path without using another real account.
    outsider = create_disposable_student(tokens["admin"], bus_id=None, stop_id=None)
    outsider_token = api.login("student", outsider["roll_number"], outsider["_password"])
    response = api.session.post(f"{BASE_URL}/student/driver-complaint/verify", headers={"Authorization": f"Bearer {outsider_token}"},
                                json={"complaint_id": complaint_id, "response": "no"}, timeout=TIMEOUT)
    assert response.status_code == 403, f"unrelated student expected 403, got {response.status_code}: {response.text[:300]}"
    api.call("DELETE", f"/admin/students/{outsider['student_id']}", tokens["admin"])
    created_students.remove(int(outsider["student_id"]))
    api.call("POST", f"/admin/bus-changes/{change_id}/cancel", tokens["admin"])
    created_changes.remove(change_id)


@test("group 4: wait request lifecycle (best effort)")
def _wait_request() -> None:
    if not tokens:
        raise SkipTest("preflight did not succeed")
    driver_id, driver_password = env("KAMBUS_TEST_DRIVER_IDENTIFIER", False), env("KAMBUS_TEST_DRIVER_PASSWORD", False)
    if not driver_id or not driver_password:
        raise SkipTest("driver credentials not configured")
    driver_token = api.login("driver", driver_id, driver_password)
    response = api.session.post(f"{BASE_URL}/student/wait-request", headers={"Authorization": f"Bearer {tokens['source']}"},
                                json={"minutes": 1}, timeout=TIMEOUT)
    if response.status_code == 400:
        raise SkipTest(f"no live wait eligibility: {response.json().get('detail')}")
    assert response.status_code == 200, f"wait request failed: {response.status_code} {response.text[:500]}"
    body = response.json()
    request_id = int(body["request_id"])
    created_waits.append((request_id, tokens["source"]))
    groups = api.call("GET", "/driver/wait-requests", driver_token).get("requests", [])
    assert any(request_id in g.get("request_ids", []) or g.get("request_id") == request_id for g in groups), "driver did not receive wait request"
    api.call("POST", f"/student/wait-request/{request_id}/cancel", tokens["source"])
    created_waits.remove((request_id, tokens["source"]))


def create_disposable_student(admin: str, bus_id: int | None, stop_id: int | None) -> dict[str, Any]:
    suffix = secrets.token_hex(5).upper()
    password = "KambusTest!9"
    body = api.call("POST", "/admin/students", admin, expected=201, json={
        "name": f"Integration Test {RUN_ID}", "phone": f"9{secrets.randbelow(10**9):09d}",
        "password": password, "roll_number": f"TEST{suffix}", "department": "TEST",
        "bus_id": bus_id, "stop_id": stop_id,
    })
    body["_password"] = password
    created_students.append(int(body["student_id"]))
    return body


@test("group 5: disposable hard deletes and deactivation safeguards")
def _delete_safeguards() -> None:
    if not tokens:
        raise SkipTest("preflight did not succeed")
    admin = tokens["admin"]
    # Unreferenced student must hard-delete cleanly.
    simple_student = create_disposable_student(admin, None, None)
    deleted = api.call("DELETE", f"/admin/students/{simple_student['student_id']}", admin)
    assert deleted.get("action") == "deleted", f"expected hard delete: {deleted}"
    created_students.remove(int(simple_student["student_id"]))

    suffix = secrets.token_hex(5).upper()
    password = "KambusTest!9"
    driver = api.call("POST", "/admin/drivers", admin, expected=201, json={
        "name": f"Integration Driver {RUN_ID}", "phone": f"8{secrets.randbelow(10**9):09d}",
        "password": password, "driver_code": f"T{suffix}", "license_number": "TEST-LICENSE",
    })
    driver["_password"] = password
    created_drivers.append(int(driver["driver_id"]))
    bus = api.call("POST", "/admin/buses", admin, expected=201, json={
        "bus_number": f"TEST-{RUN_ID}-{suffix}", "registration_number": f"TEST-{suffix}",
        "route_id": None, "driver_id": int(driver["driver_id"]), "status": "active",
    })
    created_buses.append(int(bus["bus_id"]))
    driver_token = api.login("driver", driver["driver_code"], password)
    trip_started = False
    trip_ended = False
    try:
        api.call("POST", "/driver/start-trip", driver_token, expected=200, json={"trip_type": "morning"})
        trip_started = True
        # Create this rider *after* trip-start notifications, so its user has
        # no notification FK and the complaint is the sole deactivation link.
        linked_student = create_disposable_student(admin, int(bus["bus_id"]), None)
        student_token = api.login("student", linked_student["roll_number"], linked_student["_password"])
        api.call("POST", "/student/driver-complaint", student_token, expected=200, json={
            "reason": "driver_not_on_time", "description": f"Disposable deactivation reference {RUN_ID}",
        })
        api.call("POST", "/driver/end-trip", driver_token, expected=200)
        trip_ended = True
    finally:
        # This runs before any driver deactivation, keeping a failed test from
        # orphaning an active trip in production.
        if trip_started and not trip_ended:
            try:
                api.call("POST", "/driver/end-trip", driver_token, expected=200)
            except Exception as exc:
                raise AssertionError(f"could not end disposable trip during cleanup: {exc}") from exc
    student_deactivated = api.call("DELETE", f"/admin/students/{linked_student['student_id']}", admin)
    assert student_deactivated.get("action") == "deactivated", f"student should deactivate: {student_deactivated}"
    created_students.remove(int(linked_student["student_id"]))
    driver_deactivated = api.call("DELETE", f"/admin/drivers/{driver['driver_id']}", admin)
    assert driver_deactivated.get("action") == "deactivated", f"driver should deactivate: {driver_deactivated}"
    created_drivers.remove(int(driver["driver_id"]))
    blocked_login = api.session.post(f"{BASE_URL}/auth/login", json={"role": "driver", "identifier": driver["driver_code"], "password": password}, timeout=TIMEOUT)
    assert blocked_login.status_code == 403, f"deactivated driver login expected 403, got {blocked_login.status_code}"
    blocked_assign = api.session.post(f"{BASE_URL}/admin/buses/{bus['bus_id']}/assign-driver", headers={"Authorization": f"Bearer {admin}"},
                                      json={"driver_id": driver["driver_id"]}, timeout=TIMEOUT)
    assert blocked_assign.status_code == 400, f"inactive driver assignment expected 400, got {blocked_assign.status_code}"
    bus_deactivated = api.call("DELETE", f"/admin/buses/{bus['bus_id']}", admin)
    assert bus_deactivated.get("action") == "deactivated", f"referenced bus should deactivate: {bus_deactivated}"
    created_buses.remove(int(bus["bus_id"]))


def cleanup() -> None:
    admin = tokens.get("admin")
    if not admin:
        return
    for request_id, token in list(created_waits):
        try:
            api.call("POST", f"/student/wait-request/{request_id}/cancel", token)
        except Exception as exc:
            print(f"CLEANUP WARNING wait request {request_id}: {exc}")
    for change_id in list(created_changes):
        try:
            api.call("POST", f"/admin/bus-changes/{change_id}/cancel", admin)
        except Exception as exc:
            print(f"CLEANUP WARNING bus change {change_id}: {exc}")
    for student_id in list(created_students):
        try:
            api.call("DELETE", f"/admin/students/{student_id}", admin)
        except Exception as exc:
            print(f"CLEANUP WARNING student {student_id}: {exc}")
    for driver_id in list(created_drivers):
        try:
            api.call("DELETE", f"/admin/drivers/{driver_id}", admin)
        except Exception as exc:
            print(f"CLEANUP WARNING driver {driver_id}: {exc}")
    for bus_id in list(created_buses):
        try:
            api.call("DELETE", f"/admin/buses/{bus_id}", admin)
        except Exception as exc:
            print(f"CLEANUP WARNING bus {bus_id}: {exc}")


def main() -> int:
    try:
        for name, fn in test_cases:
            group = name.split(":", 1)[0].lower()
            if TEST_GROUPS and group not in TEST_GROUPS:
                outcomes.append(Outcome(name, "SKIPPED", f"not selected (KAMBUS_TEST_GROUPS={','.join(sorted(TEST_GROUPS))})"))
                continue
            try:
                fn()
            except SkipTest as exc:
                outcomes.append(Outcome(name, "SKIPPED", str(exc)))
            except Exception as exc:  # keep later tests running
                outcomes.append(Outcome(name, "FAIL", str(exc)))
            else:
                outcomes.append(Outcome(name, "PASS", ""))
    finally:
        cleanup()
        print("\nLIVE INTEGRATION RESULTS")
        for item in outcomes:
            print(f"{item.state:7} {item.name}" + (f" — {item.detail}" if item.detail else ""))
        counts = {state: sum(item.state == state for item in outcomes) for state in ("PASS", "FAIL", "SKIPPED")}
        print(f"\nPASS={counts['PASS']}  FAIL={counts['FAIL']}  SKIPPED={counts['SKIPPED']}")
    return 1 if any(item.state == "FAIL" for item in outcomes) else 0


if __name__ == "__main__":
    sys.exit(main())
