#!/usr/bin/env python3
"""
Add a new target to the RTS2 database.

CLI usage:
    addtarget --name "GK Per" --radec "3:31:12 +43:54:16" --duration 3600
    addtarget --name "NGC 1234" --ra 50.123 --dec 12.456 --telescope sbt,d50 --enable
    addtarget --name "V* BL Lac" --ra 330.68 --dec 42.28 \
              --script "C0:E 60:C1:'' exe /etc/rts2/filtros.py 300 ''"

Library usage:
    from rtspy.cli.addtarget import add_target
    tar_id = add_target(name="NGC 1234", ra=50.123, dec=12.456,
                        telescopes=['sbt'], duration=3600,
                        scripts={'C0': 'E 60', 'C1': "'' exe /etc/rts2/filtros.py 300 ''"})
"""

import argparse
import logging
import os
import re
import sys
from typing import Dict, List, Optional

import psycopg2
import yaml

logger = logging.getLogger(__name__)

# ── default telescope configs (fallback if no scheduler config found) ─────────

_BUILTIN_DB = {
    'sbt': dict(host='lascaux.asu.cas.cz', dbname='stars', user='mates', password='pasewcic25'),
    'd50': dict(host='d50.asu.cas.cz',     dbname='stars', user='mates', password='pasewcic25'),
}

_CONFIG_SEARCH = [
    os.environ.get('RTS2_SCHEDULER_CFG', ''),
    os.path.expanduser('~/.config/rts2/scheduler.cfg'),
    '/home/mates/sch/rts2-scheduler.cfg',
]

_ID_AUTO_BASE = 8000     # first candidate for auto-assigned IDs
_ID_AUTO_MAX  = 49999    # upper bound for regular targets


# ── config loading ────────────────────────────────────────────────────────────

def _load_telescope_db_configs(config_file: Optional[str] = None) -> Dict[str, dict]:
    """
    Return {telescope_name: psycopg2_kwargs} from the scheduler YAML config.
    Falls back to built-in hardcoded values if the file is not found.
    """
    paths = ([config_file] if config_file else []) + _CONFIG_SEARCH
    for path in paths:
        if not path or not os.path.exists(path):
            continue
        try:
            with open(path) as f:
                cfg = yaml.safe_load(f)
            result = {}
            for name, res in cfg.get('resources', {}).items():
                db = res.get('database', {})
                if db:
                    result[name] = dict(db)   # has dbname/user/password/host
            if result:
                logger.debug("Loaded telescope configs from %s: %s", path, list(result))
                return result
        except Exception as e:
            logger.warning("Could not load %s: %s", path, e)

    logger.debug("Using built-in telescope DB config")
    return dict(_BUILTIN_DB)


# ── coordinate helpers ────────────────────────────────────────────────────────

def _parse_angle(s: str) -> float:
    sign = -1 if s.strip().startswith('-') else 1
    s = s.strip().lstrip('+-')
    parts = re.split(r'[:hmd\'"]\s*', s)
    parts = [p for p in parts if p]
    value = float(parts[0])
    if len(parts) > 1:
        value += float(parts[1]) / 60.0
    if len(parts) > 2:
        value += float(parts[2]) / 3600.0
    return sign * value


def parse_ra(s: str) -> float:
    """Parse RA string to decimal degrees.  Sexagesimal is treated as hours."""
    if re.search(r'[: hH]', s.strip()):
        return _parse_angle(s) * 15.0
    return float(s)


def parse_dec(s: str) -> float:
    if re.search(r'[: dD\'"]', s.strip()):
        return _parse_angle(s)
    return float(s)


def parse_radec(s: str):
    """Parse a combined 'RA Dec' string → (ra_deg, dec_deg)."""
    parts = s.strip().split()
    if len(parts) == 2:
        return parse_ra(parts[0]), parse_dec(parts[1])
    if len(parts) == 6:
        return parse_ra(':'.join(parts[:3])), parse_dec(':'.join(parts[3:]))
    raise ValueError(f"Cannot parse RA Dec: {s!r}")


# ── database helpers ──────────────────────────────────────────────────────────

def _connect(db_cfg: dict):
    return psycopg2.connect(**db_cfg)


def _used_ids_above(db_cfg: dict, base: int) -> set:
    """Return the set of tar_ids >= base present in this database."""
    try:
        conn = _connect(db_cfg)
        try:
            cur = conn.cursor()
            cur.execute("SELECT tar_id FROM targets WHERE tar_id >= %s AND tar_id <= %s",
                        (base, _ID_AUTO_MAX))
            return {row[0] for row in cur.fetchall()}
        finally:
            conn.close()
    except psycopg2.Error as e:
        logger.warning("Could not query IDs from %s: %s", db_cfg.get('host'), e)
        return set()


def _find_free_id(all_db_configs: Dict[str, dict]) -> int:
    """
    Find the first tar_id >= _ID_AUTO_BASE that is unused in ALL known databases.

    We always check both telescopes regardless of where we will insert, to
    preserve the cross-telescope uniqueness convention.
    """
    used: set = set()
    for name, cfg in all_db_configs.items():
        ids = _used_ids_above(cfg, _ID_AUTO_BASE)
        logger.debug("Telescope %s has %d IDs in range [%d, %d]",
                     name, len(ids), _ID_AUTO_BASE, _ID_AUTO_MAX)
        used |= ids

    candidate = _ID_AUTO_BASE
    while candidate <= _ID_AUTO_MAX:
        if candidate not in used:
            return candidate
        candidate += 1
    raise RuntimeError(f"No free tar_id found in range [{_ID_AUTO_BASE}, {_ID_AUTO_MAX}]")


def _insert_one(db_cfg: dict, tar_id: int, name: str, ra: float, dec: float,
                enabled: bool, priority: int, comment: Optional[str],
                duration: Optional[int], scripts: Optional[Dict[str, str]]):
    """Insert target (and optional scheduling/script rows) into one database."""
    conn = _connect(db_cfg)
    try:
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO targets
                (tar_id, type_id, tar_name, tar_ra, tar_dec,
                 tar_enabled, tar_priority, tar_comment)
            VALUES (%s, 'O', %s, %s, %s, %s, %s, %s)
            """,
            (tar_id, name, ra, dec, enabled, priority, comment or None),
        )

        if duration is not None:
            cur.execute(
                "INSERT INTO scheduling (tar_id, sinfo) VALUES (%s, %s)",
                (tar_id, f"duration={duration}"),
            )

        for camera, script in (scripts or {}).items():
            cur.execute(
                "INSERT INTO scripts (tar_id, camera_name, script) VALUES (%s, %s, %s)",
                (tar_id, camera, script),
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── main API ──────────────────────────────────────────────────────────────────

def add_target(
    name: str,
    ra: float,
    dec: float,
    *,
    telescopes: Optional[List[str]] = None,
    tar_id: Optional[int] = None,
    duration: Optional[int] = None,
    scripts: Optional[Dict[str, str]] = None,
    priority: int = 0,
    comment: str = '',
    enabled: bool = False,
    config_file: Optional[str] = None,
) -> int:
    """
    Insert an 'O'-type target into one or both RTS2 databases.

    Parameters
    ----------
    name       : target name
    ra, dec    : J2000 coordinates in decimal degrees
    telescopes : list of telescope names, e.g. ['sbt'] or ['sbt','d50'].
                 Defaults to ['sbt'].
    tar_id     : explicit ID; if omitted, the first free ID >= 8000 across BOTH
                 telescope databases is used (preserving cross-telescope uniqueness).
    duration   : reservation length in seconds (written to scheduling.sinfo)
    scripts    : dict mapping camera name → script string, e.g.
                 {'C0': 'E 60', 'C1': "'' exe /etc/rts2/filtros.py 300 ''"}
    priority   : tar_priority (default 0)
    comment    : tar_comment text
    enabled    : set tar_enabled = True on creation (default False)
    config_file: path to rts2-scheduler.cfg (auto-searched if omitted)

    Returns the tar_id of the newly created target.
    """
    if not (0 <= ra <= 360):
        raise ValueError(f"RA {ra} out of range [0, 360]")
    if not (-90 <= dec <= 90):
        raise ValueError(f"Dec {dec} out of range [-90, 90]")

    if telescopes is None:
        telescopes = ['sbt']

    all_db = _load_telescope_db_configs(config_file)

    # Validate telescope names
    unknown = set(telescopes) - set(all_db)
    if unknown:
        raise ValueError(f"Unknown telescope(s): {', '.join(sorted(unknown))}. "
                         f"Available: {', '.join(sorted(all_db))}")

    # Auto-assign ID: always check both telescopes for uniqueness
    if tar_id is None:
        tar_id = _find_free_id(all_db)
        logger.info("Auto-assigned tar_id=%d (first free >= %d across all telescopes)",
                    tar_id, _ID_AUTO_BASE)

    # Insert into each requested telescope
    inserted = []
    for scope in telescopes:
        db_cfg = all_db[scope]
        try:
            _insert_one(db_cfg, tar_id, name, ra, dec, enabled, priority, comment,
                        duration, scripts)
            logger.info("Inserted target #%d '%s' into %s (%s)",
                        tar_id, name, scope, db_cfg.get('host'))
            inserted.append(scope)
        except psycopg2.Error as e:
            # Roll back what we can and re-raise with context
            raise psycopg2.Error(
                f"Failed to insert into {scope} ({db_cfg.get('host')}): {e}"
            ) from e

    return tar_id


def parse_scripts_arg(s: str) -> Dict[str, str]:
    """
    Parse CLI --script value into {camera: script} dict.

    Format: "CAM:script text:CAM:script text:..."
    Colons alternate between camera name (no spaces, ≤8 chars) and script body.
    Example: "C0:E 60:C1:'' exe /etc/rts2/filtros.py 300 ''"
    """
    parts = s.split(':')
    if len(parts) < 2 or len(parts) % 2 != 0:
        raise ValueError(
            f"--script must be an even number of colon-separated fields "
            f"(CAM:script[:CAM:script...]): {s!r}"
        )
    result = {}
    for i in range(0, len(parts), 2):
        cam = parts[i].strip()
        if not cam:
            raise ValueError(f"Empty camera name in --script at position {i}: {s!r}")
        result[cam] = parts[i + 1]
    return result


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Insert an 'O'-type target into the RTS2 database(s).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Telescope IDs: sbt (default, lascaux.asu.cas.cz), d50 (d50.asu.cas.cz)

IDs are auto-assigned from 8000 upward, checking BOTH telescope databases for
uniqueness even when inserting into only one, to preserve the cross-telescope
ID convention.  Use --id to override.

Examples:
  addtarget --name "GK Per" --radec "52.8000 43.9045" --duration 3600
  addtarget --name "V* XY UMa" --ra "9:10:23" --dec "+54:30:11" \\
            --duration 7200 --script "C0:E 120:C1:E 60" --priority 100 --enable
  addtarget --name "V* BL Lac" --ra 330.68 --dec 42.28 \\
            --script "C0:E 60:C1:'' exe /etc/rts2/filtros.py 300 ''"
  addtarget --name "NGC 1234" --ra 50.12 --dec 12.45 --telescope sbt,d50
""",
    )

    # identity
    p.add_argument('--name', required=True, help='Target name')
    p.add_argument('--id', type=int, dest='tar_id', metavar='ID',
                   help='Explicit target ID (default: auto from 8000, unique across both DBs)')

    # coordinates
    coord = p.add_mutually_exclusive_group(required=True)
    coord.add_argument('--radec', metavar='"RA Dec"',
                       help='RA and Dec as a single string (decimal or sexagesimal)')
    coord.add_argument('--ra', metavar='DEG',
                       help='RA in decimal degrees or HH:MM:SS (requires --dec)')
    p.add_argument('--dec', metavar='DEG',
                   help='Dec in decimal degrees or ±DD:MM:SS (required with --ra)')

    # scheduling
    p.add_argument('--duration', type=int, metavar='SEC',
                   help='Reservation length in seconds')

    # scripts (multi-camera)
    p.add_argument('--script', metavar='CAM:SCRIPT[:CAM:SCRIPT...]',
                   help='Per-camera scripts as alternating colon-separated pairs, '
                        'e.g. "C0:E 60:C1:E 120"')

    # target properties
    p.add_argument('--priority', type=int, default=0,
                   help='Target priority (default: 0)')
    p.add_argument('--comment', default='', help='Target comment')
    p.add_argument('--enable', action='store_true', dest='enabled',
                   help='Enable the target on creation (default: disabled)')

    # telescope selection
    p.add_argument('--telescope', default='sbt', metavar='SCOPE[,SCOPE]',
                   help='Comma-separated telescope(s): sbt, d50, or sbt,d50 (default: sbt)')

    # config
    p.add_argument('--config', metavar='FILE',
                   help='Path to rts2-scheduler.cfg (auto-searched if omitted)')

    p.add_argument('--verbose', '-v', action='store_true')

    return p


def main(argv=None) -> int:
    p = _build_parser()
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(levelname)s: %(message)s',
    )

    # resolve coordinates
    try:
        if args.radec:
            ra, dec = parse_radec(args.radec)
        else:
            if args.dec is None:
                p.error("--dec is required when --ra is used")
            ra  = parse_ra(args.ra)
            dec = parse_dec(args.dec)
    except ValueError as e:
        p.error(str(e))

    # parse scripts
    scripts = None
    if args.script:
        try:
            scripts = parse_scripts_arg(args.script)
        except ValueError as e:
            p.error(str(e))

    telescopes = [t.strip() for t in args.telescope.split(',') if t.strip()]

    try:
        tar_id = add_target(
            name=args.name,
            ra=ra,
            dec=dec,
            telescopes=telescopes,
            tar_id=args.tar_id,
            duration=args.duration,
            scripts=scripts,
            priority=args.priority,
            comment=args.comment,
            enabled=args.enabled,
            config_file=args.config,
        )
    except ValueError as e:
        p.error(str(e))
    except psycopg2.Error as e:
        logger.error("Database error: %s", e)
        return 1

    state = "enabled" if args.enabled else "disabled"
    scope_str = ', '.join(telescopes)
    print(f"Created target #{tar_id}  '{args.name}'  "
          f"RA={ra:.6f}  Dec={dec:.6f}  [{state}]  → {scope_str}")
    if args.duration:
        print(f"  scheduling: duration={args.duration}s")
    if scripts:
        for cam, scr in scripts.items():
            print(f"  script [{cam}]: {scr}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
