#!/usr/bin/env python3

"""
Queue Manual - Simple CLI for adding targets to the manual queue

Usage:
  queue-manual [-d DURATION] TARGET_ID TIME
  
Options:
  -d DURATION    Duration in human-readable format (e.g., 30s, 5m, 2h, 1h30m)
                 If not specified, uses duration from queuing table in database
  
Examples:
  queue-manual 123 18:30              # Today at 18:30 UT (duration from database)
  queue-manual -d 2h 123 18:30        # Today at 18:30 UT for 2 hours
  queue-manual -d 45m 456 2025-02-15U21:30  # February 15, 2025 at 21:30 UT for 45 minutes
  queue-manual -d 1h30m 789 2025-03-10T14:45  # March 10, 2025 at 14:45 UT for 1.5 hours
"""

import sys
import re
import psycopg2
import configparser
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path


def parse_rts2_config():
    """Parse RTS2 configuration to get database settings."""
    rts2_ini_paths = [
        '/etc/rts2/rts2.ini',
        '/usr/local/etc/rts2/rts2.ini',
        '~/.rts2/rts2.ini'
    ]
    
    config = configparser.ConfigParser()
    
    for path_str in rts2_ini_paths:
        path = Path(path_str).expanduser()
        if path.exists():
            config.read(path)
            break
    
    # Default values matching queue_selector.py
    db_config = {
        'host': 'localhost',
        'database': 'stars',
        'user': 'mates',
        'password': 'pasewcic25'
    }
    
    # Override with values from rts2.ini if available
    if config.has_section('database'):
        db_section = config['database']
        if 'host' in db_section:
            db_config['host'] = db_section['host']
        if 'db' in db_section:
            db_config['database'] = db_section['db']
        if 'user' in db_section:
            db_config['user'] = db_section['user']
        if 'password' in db_section:
            db_config['password'] = db_section['password']
    
    return db_config


def parse_time(time_str):
    """
    Parse time string into datetime object.
    
    Formats supported:
    - HH:MM (today at that time UT)
    - YYYY-MM-DDUHH:MM or YYYY-MM-DDTHH:MM (specific date and time)
    - YYYY-MM-DD HH:MM (specific date and time with space)
    
    Both U and T separators are treated as UT time.
    """
    now = datetime.now(timezone.utc)
    
    # Pattern for time only (HH:MM)
    time_only_pattern = r'^(\d{1,2}):(\d{2})$'
    match = re.match(time_only_pattern, time_str)
    if match:
        hour, minute = int(match.group(1)), int(match.group(2))
        target_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        # If the time has already passed today, schedule for tomorrow
        if target_time <= now:
            target_time += timedelta(days=1)
        
        return target_time
    
    # Pattern for full date and time (YYYY-MM-DD[UT]HH:MM)
    full_pattern = r'^(\d{4})-(\d{1,2})-(\d{1,2})[UT\s](\d{1,2}):(\d{2})$'
    match = re.match(full_pattern, time_str)
    if match:
        year, month, day, hour, minute = map(int, match.groups())
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    
    # Pattern for ISO-like format with space
    iso_pattern = r'^(\d{4})-(\d{1,2})-(\d{1,2}) (\d{1,2}):(\d{2})$'
    match = re.match(iso_pattern, time_str)
    if match:
        year, month, day, hour, minute = map(int, match.groups())
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    
    raise ValueError(f"Invalid time format: {time_str}")


def parse_duration(duration_str):
    """
    Parse human-readable duration string into timedelta object.
    
    Formats supported:
    - 30s, 45s (seconds)
    - 5m, 15m (minutes)
    - 2h, 3h (hours)
    - 1h30m, 2h45m (combined hours and minutes)
    - 90 (bare number treated as minutes for backward compatibility)
    """
    if not duration_str:
        raise ValueError("Duration cannot be empty")
    
    duration_str = duration_str.strip().lower()
    
    # Handle bare numbers (backward compatibility - treat as minutes)
    if duration_str.isdigit():
        return timedelta(minutes=int(duration_str))
    
    total_seconds = 0
    
    # Pattern for hours and minutes combined (e.g., "1h30m", "2h45m")
    combined_pattern = r'^(\d+)h(\d+)m$'
    match = re.match(combined_pattern, duration_str)
    if match:
        hours, minutes = int(match.group(1)), int(match.group(2))
        return timedelta(hours=hours, minutes=minutes)
    
    # Pattern for individual units
    unit_pattern = r'^(\d+)([smh])$'
    match = re.match(unit_pattern, duration_str)
    if match:
        value, unit = int(match.group(1)), match.group(2)
        if unit == 's':
            return timedelta(seconds=value)
        elif unit == 'm':
            return timedelta(minutes=value)
        elif unit == 'h':
            return timedelta(hours=value)
    
    raise ValueError(f"Invalid duration format: {duration_str}. Use formats like: 30s, 5m, 2h, 1h30m")


def get_target_duration_from_db(db_config, target_id):
    """
    Get default duration for target from queuing table in database.
    
    Returns timedelta object or None if not found.
    """
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Look for recent queuing entries for this target to get typical duration
        cursor.execute("""
            SELECT time_start, time_end 
            FROM queues_targets 
            WHERE tar_id = %s 
              AND time_start IS NOT NULL 
              AND time_end IS NOT NULL
            ORDER BY time_start DESC 
            LIMIT 1
        """, (target_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            start_time, end_time = result
            # Ensure timezone awareness
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)
            
            duration = end_time - start_time
            return duration
        
        return None
        
    except Exception as e:
        print(f"Warning: Could not get duration from database: {e}")
        return None


def sync_qid_sequence(cursor):
    """Ensure qid sequence is in sync with the actual data."""
    try:
        cursor.execute("SELECT setval('qid', (SELECT COALESCE(MAX(qid), 0) FROM queues_targets));")
    except Exception as e:
        print(f"Warning: Could not sync qid sequence: {e}")


def add_to_manual_queue(db_config, target_id, start_time, duration=None):
    """Add target to manual queue in database."""
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Ensure sequence is in sync before inserting
        sync_qid_sequence(cursor)
        
        # Manual queue is queue_id = 1 (from queue_selector.py mapping)
        manual_queue_id = 1
        
        # Check if target exists
        cursor.execute("SELECT tar_name FROM targets WHERE tar_id = %s", (target_id,))
        result = cursor.fetchone()
        if not result:
            print(f"Error: Target {target_id} does not exist in targets table")
            return False
        
        target_name = result[0]
        
        # Determine duration
        if duration is None:
            # Try to get duration from database
            duration = get_target_duration_from_db(db_config, target_id)
            if duration is None:
                # Fall back to 1 hour default
                duration = timedelta(hours=1)
                print(f"No previous duration found for target {target_id}, using 1 hour default")
            else:
                print(f"Using duration from database: {duration}")
        else:
            print(f"Using specified duration: {duration}")
        
        # Calculate end time
        end_time = start_time + duration
        
        cursor.execute("""
            INSERT INTO queues_targets (qid, queue_id, tar_id, time_start, time_end, queue_order)
            VALUES (nextval('qid'), %s, %s, %s, %s, 0)
        """, (manual_queue_id, target_id, start_time, end_time))
        
        conn.commit()
        conn.close()
        
        print(f"Successfully added target {target_id} ({target_name}) to manual queue")
        print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')} UT")
        print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')} UT")
        print(f"Duration: {duration}")
        return True
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False


def clear_manual_queue(db_config):
    """Clear all entries from the manual queue."""
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Manual queue is queue_id = 1
        manual_queue_id = 1
        
        # Count existing entries first
        cursor.execute("SELECT COUNT(*) FROM queues_targets WHERE queue_id = %s", (manual_queue_id,))
        count = cursor.fetchone()[0]
        
        if count == 0:
            print("Manual queue is already empty")
            conn.close()
            return True
        
        # Delete all entries from manual queue
        cursor.execute("DELETE FROM queues_targets WHERE queue_id = %s", (manual_queue_id,))
        
        conn.commit()
        conn.close()
        
        print(f"Successfully cleared {count} entries from manual queue")
        return True
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Add targets to the manual queue or clear the queue',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  queue-manual 123 18:30              # Today at 18:30 UT (duration from database)
  queue-manual -d 2h 123 18:30        # Today at 18:30 UT for 2 hours
  queue-manual -d 45m 456 2025-02-15U21:30  # February 15, 2025 at 21:30 UT for 45 minutes
  queue-manual -d 1h30m 789 2025-03-10T14:45  # March 10, 2025 at 14:45 UT for 1.5 hours
  queue-manual -c                     # Clear all entries from manual queue

Duration formats:
  30s, 45s          # seconds
  5m, 15m           # minutes  
  2h, 3h            # hours
  1h30m, 2h45m      # hours and minutes combined
  90                # bare number (minutes for compatibility)
"""
    )
    
    parser.add_argument('-d', '--duration', type=str, help='Duration in human-readable format (e.g., 30s, 5m, 2h, 1h30m)')
    parser.add_argument('-c', '--clear', action='store_true', help='Clear all entries from manual queue')
    parser.add_argument('target_id', type=int, nargs='?', help='Target ID from targets table')
    parser.add_argument('time', type=str, nargs='?', help='Start time (HH:MM or YYYY-MM-DD[UT]HH:MM)')
    
    try:
        args = parser.parse_args()
        
        # Get database configuration
        db_config = parse_rts2_config()
        
        # Handle clear option
        if args.clear:
            success = clear_manual_queue(db_config)
            return 0 if success else 1
        
        # Validate required arguments for adding targets
        if not args.target_id or not args.time:
            print("Error: target_id and time are required when not using --clear option")
            parser.print_help()
            return 1
        
        # Parse time
        start_time = parse_time(args.time)
        
        # Parse duration if provided
        duration = None
        if args.duration:
            duration = parse_duration(args.duration)
        
        # Add to manual queue
        success = add_to_manual_queue(db_config, args.target_id, start_time, duration)
        
        return 0 if success else 1
        
    except ValueError as e:
        print(f"Error: {e}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())