"""Print Google Maps links for generated evening stop-offset preview rows."""

from __future__ import annotations

import csv
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = BACKEND_ROOT / "scripts" / "evening_stop_offsets_preview.csv"


def main() -> None:
    skipped: list[tuple[str, str]] = []

    with INPUT_CSV.open("r", newline="", encoding="utf-8") as csv_file:
        for row in csv.DictReader(csv_file):
            stop_id = row["stop_id"]
            stop_name = row["stop_name"]
            evening_latitude = row.get("computed_evening_lat", "").strip()

            if not evening_latitude:
                skipped.append((stop_name, stop_id))
                continue

            original_latitude = row["original_lat"]
            original_longitude = row["original_lng"]
            evening_longitude = row["computed_evening_lng"]

            print(f"Stop {stop_name} (id={stop_id}):")
            print(f"Original: https://www.google.com/maps?q={original_latitude},{original_longitude}")
            print(f"Evening:  https://www.google.com/maps?q={evening_latitude},{evening_longitude}")

    print("\nSKIPPED (no OSRM snap)")
    for stop_name, stop_id in skipped:
        print(f"Stop {stop_name} (id={stop_id})")


if __name__ == "__main__":
    main()
