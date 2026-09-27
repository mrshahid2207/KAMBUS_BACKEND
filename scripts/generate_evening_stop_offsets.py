"""Generate a read-only CSV preview of raw evening-side stop offsets.

This script deliberately never assigns ``Stop.evening_latitude`` or
``Stop.evening_longitude`` and never commits a database transaction. Review
the generated CSV and HTML visualization before introducing a separate,
explicit write step.
"""

from __future__ import annotations

import csv
import logging
import math
import sys
from pathlib import Path
from typing import Iterable


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from database import SessionLocal  # noqa: E402
from models import Route, Stop  # noqa: E402


LOGGER = logging.getLogger("evening_stop_offsets")
EARTH_RADIUS_METERS = 6_371_000.0
OFFSET_METERS = 8.0
OUTPUT_CSV = BACKEND_ROOT / "scripts" / "evening_stop_offsets_preview.csv"


def haversine_distance_meters(
    latitude_a: float,
    longitude_a: float,
    latitude_b: float,
    longitude_b: float,
) -> float:
    """Return the great-circle distance between two latitude/longitude points."""
    latitude_delta = math.radians(latitude_b - latitude_a)
    longitude_delta = math.radians(longitude_b - longitude_a)
    latitude_a_radians = math.radians(latitude_a)
    latitude_b_radians = math.radians(latitude_b)

    value = (
        math.sin(latitude_delta / 2) ** 2
        + math.cos(latitude_a_radians)
        * math.cos(latitude_b_radians)
        * math.sin(longitude_delta / 2) ** 2
    )
    return EARTH_RADIUS_METERS * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))


def initial_bearing_degrees(
    origin_latitude: float,
    origin_longitude: float,
    destination_latitude: float,
    destination_longitude: float,
) -> float:
    """Return the initial great-circle bearing in compass degrees (0-360)."""
    origin_latitude_radians = math.radians(origin_latitude)
    destination_latitude_radians = math.radians(destination_latitude)
    longitude_delta_radians = math.radians(destination_longitude - origin_longitude)

    x = math.sin(longitude_delta_radians) * math.cos(destination_latitude_radians)
    y = (
        math.cos(origin_latitude_radians) * math.sin(destination_latitude_radians)
        - math.sin(origin_latitude_radians)
        * math.cos(destination_latitude_radians)
        * math.cos(longitude_delta_radians)
    )
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def destination_point(
    latitude: float,
    longitude: float,
    bearing_degrees: float,
    distance_meters: float,
) -> tuple[float, float]:
    """Return the point reached by travelling a bearing/distance on a sphere."""
    latitude_radians = math.radians(latitude)
    longitude_radians = math.radians(longitude)
    bearing_radians = math.radians(bearing_degrees)
    angular_distance = distance_meters / EARTH_RADIUS_METERS

    destination_latitude = math.asin(
        math.sin(latitude_radians) * math.cos(angular_distance)
        + math.cos(latitude_radians)
        * math.sin(angular_distance)
        * math.cos(bearing_radians)
    )
    destination_longitude = longitude_radians + math.atan2(
        math.sin(bearing_radians)
        * math.sin(angular_distance)
        * math.cos(latitude_radians),
        math.cos(angular_distance)
        - math.sin(latitude_radians) * math.sin(destination_latitude),
    )

    normalized_longitude = (math.degrees(destination_longitude) + 540.0) % 360.0 - 180.0
    return math.degrees(destination_latitude), normalized_longitude


def route_stops(route_id: int, db) -> list[Stop]:
    return (
        db.query(Stop)
        .filter(Stop.route_id == route_id, Stop.is_active.is_(True))
        .order_by(Stop.stop_order.asc(), Stop.id.asc())
        .all()
    )


def preview_row(stop: Stop, bearing_degrees: float | None) -> dict[str, object]:
    row: dict[str, object] = {
        "stop_id": stop.id,
        "stop_name": stop.name,
        "route_id": stop.route_id,
        "original_lat": stop.latitude,
        "original_lng": stop.longitude,
        "computed_evening_lat": "",
        "computed_evening_lng": "",
        "offset_distance_meters_actual": "",
    }

    if bearing_degrees is None:
        LOGGER.warning("Skipping stop id=%s name=%r: route has no usable bearing", stop.id, stop.name)
        return row

    offset_bearing = (bearing_degrees - 90.0) % 360.0
    offset_latitude, offset_longitude = destination_point(
        float(stop.latitude),
        float(stop.longitude),
        offset_bearing,
        OFFSET_METERS,
    )
    row["computed_evening_lat"] = offset_latitude
    row["computed_evening_lng"] = offset_longitude
    row["offset_distance_meters_actual"] = haversine_distance_meters(
        float(stop.latitude),
        float(stop.longitude),
        offset_latitude,
        offset_longitude,
    )
    return row


def route_preview_rows(stops: Iterable[Stop]) -> list[dict[str, object]]:
    ordered_stops = list(stops)
    rows: list[dict[str, object]] = []

    if len(ordered_stops) < 2:
        return [preview_row(stop, None) for stop in ordered_stops]

    for index, stop in enumerate(ordered_stops):
        bearing_origin = ordered_stops[index - 1] if index > 0 else stop
        bearing_destination = ordered_stops[index + 1] if index < len(ordered_stops) - 1 else stop

        bearing = initial_bearing_degrees(
            float(bearing_origin.latitude),
            float(bearing_origin.longitude),
            float(bearing_destination.latitude),
            float(bearing_destination.longitude),
        )
        rows.append(preview_row(stop, bearing))

    return rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    db = SessionLocal()
    try:
        rows: list[dict[str, object]] = []
        routes = db.query(Route).order_by(Route.id.asc()).all()
        for route in routes:
            stops = route_stops(route.id, db)
            LOGGER.info("Previewing route id=%s with %s active stops", route.id, len(stops))
            rows.extend(route_preview_rows(stops))

        fieldnames = [
            "stop_id",
            "stop_name",
            "route_id",
            "original_lat",
            "original_lng",
            "computed_evening_lat",
            "computed_evening_lng",
            "offset_distance_meters_actual",
        ]
        with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        LOGGER.info("Wrote %s preview rows to %s", len(rows), OUTPUT_CSV)
        LOGGER.info("No Stop rows were modified and no database transaction was committed.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
