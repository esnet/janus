#!/usr/bin/env python3
"""
TinyDB migration script for the Janus Controller database.

Applies schema changes introduced in commit d22ede4
("updated the model for ports in ContainerProfileSettings and related functions")
and subsequent refactoring commits.

Changes applied
---------------
profiles table:
  settings.ctrl_port_range  -> settings.ctrl_ports
  settings.data_port_range  -> settings.data_ports
  settings.serv_port_range  -> settings.serv_ports
  settings.mem              -> settings.memory
  settings.internal_port    -> settings.exposed_ports
  settings.entrypoint       added (null) if missing
  settings.dns              added (null) if missing

host table:
  settings.mem (boolean)    removed when settings.memory (int) already present
  settings.dns              added (null) if missing

Usage
-----
  python scripts/migrate_db.py [--db PATH] [--dry-run] [--verbose]

  --db PATH     Path to db.json  (default: db.json next to this script's repo root)
  --dry-run     Report what would change without writing anything
  --verbose     Print every individual field change
"""

import argparse
import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Field rename map for ContainerProfileSettings (profiles + host tables)
# ---------------------------------------------------------------------------
SETTINGS_RENAMES = {
    "ctrl_port_range": "ctrl_ports",
    "data_port_range": "data_ports",
    "serv_port_range": "serv_ports",
    "mem": "memory",
    "internal_port": "exposed_ports",
}

# New fields added to ContainerProfileSettings with None defaults
SETTINGS_NEW_FIELDS = {
    "entrypoint": None,
    "dns": None,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _record_name(record: dict, table: str) -> str:
    """Return a human-readable identifier for a record."""
    return record.get("name") or record.get("uuid") or f"<{table} record>"


def migrate_container_profile_settings(
    settings: dict,
    record_name: str,
    table: str,
    verbose: bool,
) -> tuple[dict, list[str]]:
    """
    Apply field renames and additions to a ContainerProfileSettings dict.

    Returns (updated_settings, list_of_change_descriptions).
    """
    changes: list[str] = []
    s = dict(settings)  # work on a copy

    # 1. Rename old fields
    for old_key, new_key in SETTINGS_RENAMES.items():
        if old_key in s:
            if new_key in s:
                # Both old and new exist — keep new, drop old
                msg = (
                    f"[{table}] {record_name}: "
                    f"removed stale '{old_key}' ('{new_key}' already present)"
                )
                del s[old_key]
            else:
                msg = (
                    f"[{table}] {record_name}: "
                    f"renamed '{old_key}' -> '{new_key}' (value={s[old_key]!r})"
                )
                s[new_key] = s.pop(old_key)
            changes.append(msg)
            if verbose:
                print(msg)

    # 2. Add missing new fields
    for field, default in SETTINGS_NEW_FIELDS.items():
        if field not in s:
            msg = (
                f"[{table}] {record_name}: "
                f"added missing field '{field}' = {default!r}"
            )
            s[field] = default
            changes.append(msg)
            if verbose:
                print(msg)

    return s, changes


# ---------------------------------------------------------------------------
# Per-table migration functions
# ---------------------------------------------------------------------------

def migrate_profiles(
    table_data: dict, verbose: bool
) -> tuple[dict, list[str]]:
    """Migrate the 'profiles' TinyDB table."""
    all_changes: list[str] = []
    updated: dict = {}

    for doc_id, record in table_data.items():
        if not isinstance(record, dict):
            updated[doc_id] = record
            continue

        record = dict(record)
        name = _record_name(record, "profiles")
        settings = record.get("settings")

        if isinstance(settings, dict):
            new_settings, changes = migrate_container_profile_settings(
                settings, name, "profiles", verbose
            )
            record["settings"] = new_settings
            all_changes.extend(changes)

        updated[doc_id] = record

    return updated, all_changes


def migrate_host(
    table_data: dict, verbose: bool
) -> tuple[dict, list[str]]:
    """
    Migrate the 'host' TinyDB table.

    The host table was partially migrated already.  Five records have both
    a stale boolean ``mem`` field and the correct integer ``memory`` field.
    We remove the stale one and add any still-missing new fields.
    """
    all_changes: list[str] = []
    updated: dict = {}

    for doc_id, record in table_data.items():
        if not isinstance(record, dict):
            updated[doc_id] = record
            continue

        record = dict(record)
        name = _record_name(record, "host")
        settings = record.get("settings")

        if isinstance(settings, dict):
            new_settings, changes = migrate_container_profile_settings(
                settings, name, "host", verbose
            )
            record["settings"] = new_settings
            all_changes.extend(changes)

        updated[doc_id] = record

    return updated, all_changes


# ---------------------------------------------------------------------------
# Main migration driver
# ---------------------------------------------------------------------------

TABLE_MIGRATIONS = {
    "profiles": migrate_profiles,
    "host": migrate_host,
}


def run_migration(db_path: Path, dry_run: bool, verbose: bool) -> int:
    """
    Load db.json, apply all migrations, optionally write back.

    Returns the total number of field-level changes made.
    """
    print(f"Loading database from: {db_path}")
    with db_path.open("r", encoding="utf-8") as fh:
        db = json.load(fh)

    total_changes: list[str] = []

    for table_name, migrate_fn in TABLE_MIGRATIONS.items():
        table_data = db.get(table_name)
        if table_data is None:
            print(f"  [SKIP] Table '{table_name}' not found in database.")
            continue

        if not isinstance(table_data, dict):
            print(
                f"  [SKIP] Table '{table_name}' has unexpected type "
                f"{type(table_data).__name__} (expected dict)."
            )
            continue

        print(f"\nMigrating table '{table_name}' ({len(table_data)} records) ...")
        updated_table, changes = migrate_fn(table_data, verbose)
        db[table_name] = updated_table
        total_changes.extend(changes)

        if changes:
            print(f"  -> {len(changes)} field change(s) in '{table_name}'.")
        else:
            print(f"  -> No changes needed in '{table_name}'.")

    print(f"\nTotal field changes: {len(total_changes)}")

    if dry_run:
        print("\n[DRY RUN] No files were written.")
        return len(total_changes)

    # --- Backup ---
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_suffix(f".json.bak.{timestamp}")
    shutil.copy2(db_path, backup_path)
    print(f"\nBackup written to: {backup_path}")

    # --- Write updated database ---
    with db_path.open("w", encoding="utf-8") as fh:
        json.dump(db, fh, indent=2, ensure_ascii=False)
        fh.write("\n")

    print(f"Updated database written to: {db_path}")
    return len(total_changes)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _default_db_path() -> Path:
    """Resolve the default db.json path relative to this script."""
    # scripts/migrate_db.py -> repo root -> db.json
    return Path(__file__).resolve().parent.parent / "db.json"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate Janus TinyDB (db.json) to the current schema.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        type=Path,
        default=_default_db_path(),
        help="Path to db.json (default: %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report changes without writing anything",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print every individual field change",
    )
    args = parser.parse_args()

    db_path: Path = args.db.resolve()

    if not db_path.exists():
        print(f"ERROR: Database file not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    n = run_migration(db_path, dry_run=args.dry_run, verbose=args.verbose)

    if args.dry_run and n > 0:
        print(
            "\nRe-run without --dry-run to apply these changes.",
            file=sys.stderr,
        )
        sys.exit(0)


if __name__ == "__main__":
    main()