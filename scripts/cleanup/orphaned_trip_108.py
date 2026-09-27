"""Complete the known disposable orphaned trip without emitting notifications.

This is intentionally hard-coded to the reported trip/driver IDs and refuses
to modify anything if those identifiers no longer describe the expected state.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from database import SessionLocal  # noqa: E402
from models import Trip, WaitRequest  # noqa: E402


TRIP_ID = 108
DRIVER_ID = 5
OPEN_WAIT_STATUSES = ("pending", "accepted", "waiting")


def main() -> None:
    db = SessionLocal()
    try:
        trip = db.query(Trip).filter(Trip.id == TRIP_ID).first()
        if trip is None:
            raise RuntimeError(f"Abort: trip {TRIP_ID} no longer exists.")
        if trip.status != "active":
            raise RuntimeError(
                f"Abort: trip {TRIP_ID} has status {trip.status!r}; expected 'active'."
            )
        if trip.driver_id != DRIVER_ID:
            raise RuntimeError(
                f"Abort: trip {TRIP_ID} belongs to driver {trip.driver_id!r}; "
                f"expected driver {DRIVER_ID}."
            )

        before_status = trip.status
        now = datetime.utcnow()
        open_waits = (
            db.query(WaitRequest)
            .filter(
                WaitRequest.trip_id == trip.id,
                WaitRequest.status.in_(OPEN_WAIT_STATUSES),
            )
            .all()
        )
        waiting_completed = 0
        other_rejected = 0
        for request in open_waits:
            if request.status == "waiting":
                request.status = "completed"
                request.wait_until = min(request.wait_until, now) if request.wait_until else now
                waiting_completed += 1
            else:
                request.status = "rejected"
                other_rejected += 1

        trip.ended_at = now
        trip.status = "completed"
        db.commit()
        db.refresh(trip)
        print(
            f"Trip {trip.id}: {before_status!r} -> {trip.status!r}; "
            f"ended_at={trip.ended_at.isoformat()}"
        )
        print(
            f"Wait requests resolved: {waiting_completed} waiting -> completed, "
            f"{other_rejected} pending/accepted -> rejected."
        )
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
