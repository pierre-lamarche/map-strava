"""
List all available fields in FIT files from a Strava export zip.

Usage:
    python list_fit_fields.py [zip_path] [--json] [--max-records N]

Examples:
    python list_fit_fields.py
    python list_fit_fields.py /path/to/export.zip
    python list_fit_fields.py --json > fields.json
"""

import argparse
import gzip
import json
import re
import sys
import zipfile
from collections import defaultdict

import fitdecode
from fitdecode.records import FitDataMessage


def parse_args():
    parser = argparse.ArgumentParser(
        description="Enumerate all fields present in FIT files from a Strava export zip."
    )
    parser.add_argument(
        "zip_path",
        nargs="?",
        default="/home/pierre/Documents/strava/data/export_121802745.zip",
        help="Path to the Strava export zip file.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output the full inventory as JSON.",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=2000,
        help="Max record frames to scan per file (default: 2000). "
        "Field presence rarely changes mid-file.",
    )
    return parser.parse_args()


def extract_fields_from_fit(data: bytes, max_records: int) -> dict[str, set[str]]:
    """Parse a single FIT binary stream and return {message_type: set(field_names)}."""
    from io import BytesIO

    fields_by_type: dict[str, set[str]] = defaultdict(set)
    record_count = 0
    with fitdecode.FitReader(BytesIO(data)) as fit_file:
        for frame in fit_file:
            if not isinstance(frame, FitDataMessage):
                continue
            if frame.name == "record":
                record_count += 1
                for fd in frame.fields:
                    fields_by_type["record"].add(fd.name)
                if record_count >= max_records:
                    # Stop collecting record fields; still finish reading
                    # the rest of the file for non-record messages.
                    continue
            else:
                for fd in frame.fields:
                    fields_by_type[frame.name].add(fd.name)
    return dict(fields_by_type)


def main():
    args = parse_args()

    inventory: dict[str, dict[str, set[str]]] = {}

    with zipfile.ZipFile(args.zip_path, "r") as zf:
        fit_files = [
            name
            for name in zf.namelist()
            if name.startswith("activities/") and name.endswith(".fit.gz")
        ]

        for fit_name in fit_files:
            file_id = re.match(r".*?/(.*?)\.fit\.gz", fit_name).group(1)
            with zf.open(fit_name, "r") as f:
                raw = gzip.decompress(f.read())
            fields = extract_fields_from_fit(raw, args.max_records)
            inventory[file_id] = fields

    # ---- JSON output ----
    if args.json:
        # Convert sets to sorted lists for JSON serializability
        json_inventory = {
            fid: {mt: sorted(fields) for mt, fields in mts.items()}
            for fid, mts in inventory.items()
        }

        # Summary: for each (message_type, field), count how many files have it
        field_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for mts in inventory.values():
            for mt, fields in mts.items():
                for field in fields:
                    field_counts[mt][field] += 1

        n_files = len(inventory)
        summary = {
            mt: {
                field: {"files": count, "pct": round(100 * count / n_files, 1)}
                for field, count in sorted(fields.items())
            }
            for mt, fields in sorted(field_counts.items())
        }

        output = {"files": json_inventory, "summary": summary}
        json.dump(output, sys.stdout, indent=2)
        print()
        return

    # ---- Text output ----
    print(f"Scanned {len(inventory)} FIT files from {args.zip_path}\n")

    for file_id, mts in sorted(inventory.items()):
        record_count = ""
        print(f"{file_id}")
        for mt in sorted(mts.keys()):
            fields = sorted(mts[mt])
            print(f"  {mt} ({len(fields)}): {', '.join(fields)}")
        print()

    # Summary table
    print("=" * 70)
    print(f"SUMMARY — field presence across {len(inventory)} files")
    print("=" * 70)

    field_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for mts in inventory.values():
        for mt, fields in mts.items():
            for field in fields:
                field_counts[mt][field] += 1

    n_files = len(inventory)
    for mt in sorted(field_counts.keys()):
        print(f"\n  [{mt}]")
        # Sort by frequency descending
        sorted_fields = sorted(
            field_counts[mt].items(), key=lambda x: -x[1]
        )
        for field, count in sorted_fields:
            pct = 100 * count / n_files
            print(f"    {field:<40s} {count:4d} / {n_files}  ({pct:.0f}%)")


if __name__ == "__main__":
    main()
