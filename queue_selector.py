#!/usr/bin/env python3

"""
RTS2 Queue Selector Daemon

A simplified queue-based target selector that:
1. Reads scheduled targets from the RTS2 PostgreSQL database
2. Executes targets from queues in proper time order
3. Handles calibrations during DUSK/DAWN system states
4. Respects GRB grace periods to avoid interrupting alert observations

This replaces the complex C++ selector with a simple queue executor.
"""

import time
import logging
import threading
import psycopg2
import configparser
import os
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple

from device import Device
from config import DeviceConfig
from app import App
from value import ValueDouble, ValueInteger, ValueBool, ValueTime, ValueString
from constants import DeviceType, CentralState, ConnectionState


@dataclass
class ScheduledTarget:
    """Represents a scheduled target from the database."""
    qid: int
    tar_id: int
    queue_start: datetime
    queue_end: Optional[datetime]
    tar_name: str
    tar_ra: float
    tar_dec: float
    queue_order: int = 0


class QueueSelector(Device, DeviceConfig):
    """
    Queue-based target selector for RTS2.

    Reads scheduled targets from database and executes them at proper times.
    Handles calibrations and respects GRB grace periods.
    """

    # Target type constants (from C++ RTS2)
    TARGET_FLAT = 2  # Calibration script (handles both flats and darks internally)

    # Default configuration
    DEFAULT_CONFIG = {
        'time_slice': 300,           # Seconds before target start to issue 'next'
        'grb_grace_period': 1200,    # Grace period duration (seconds)
        'update_interval': 30,       # Selector loop interval (seconds)
        'executor': 'EXEC',          # Executor device name
    }


    def setup_config(self, config):
        """Register selector-specific configuration."""

        # Database configuration (can override rts2.ini)
        config.add_argument('--db-host', default=None,
                          help='Database host (overrides rts2.ini)', section='database')
        config.add_argument('--db-name', default=None,
                          help='Database name (overrides rts2.ini)', section='database')
        config.add_argument('--db-user', default=None,
                          help='Database user (overrides rts2.ini)', section='database')
        config.add_argument('--db-password', default=None,
                          help='Database password (overrides rts2.ini)', section='database')

        # Queue names (mapped to queue IDs in database)
        config.add_argument('--add-queue', action='append', dest='queue_names',
                          help='Add queue name (can be used multiple times)')

        # Timing control
        config.add_argument('--time-slice', type=float, default=300.0,
                          help='Time slice before target start to issue next command (seconds)')
        config.add_argument('--grb-grace-period', type=float, default=1200.0,
                          help='GRB grace period to avoid interruptions (seconds)')
        config.add_argument('--update-interval', type=float, default=30.0,
                          help='Selector main loop interval (seconds)')

        # Executor device
        config.add_argument('--executor', default='EXEC',
                          help='Executor device name')

    def apply_config(self, config):
        """Apply configuration and initialize selector."""
        super().apply_config(config)

        # Database configuration - command line overrides rts2.ini
        self.db_config = {
            'host': config.get('db_host', 'localhost'),
            'database': config.get('db_name', 'stars'),
            'user': config.get('db_user', 'mates'),
            'password': config.get('db_password', 'pasewcic25')
        }

        # Queue name to ID mapping (default RTS2 queues)
        default_queues = ['grb', 'manual', 'scheduler', 'integral_targets',
                         'regular_targets', 'longscript', 'longscript2', 'plan']
        queue_names = config.get('queue_names') or default_queues

        # Map queue names to IDs (0-based indexing as per RTS2 convention)
        self.queue_name_to_id = {name: idx for idx, name in enumerate(queue_names)}
        logging.info(f"Queue mapping: {self.queue_name_to_id}")

        # Timing settings
        self.time_slice = config.get('time_slice')
        self.grb_grace_period = config.get('grb_grace_period')
        self.update_interval = config.get('update_interval')
        self.executor_name = config.get('executor')

        # Initialize networked time_slice value with command-line config
        self.time_slice_value.value = self.time_slice

        # Runtime state
        self.db_conn = None
        self.selector_thread = None
        self.running = False

        # System state tracking
        self.system_state = 0
        self.system_ready = False

        # Command timing
        self.last_command_time = 0.0

        # Startup handling
        self.startup_time = None
        self.startup_completed = False  # Track if initial evaluation is done

    def __init__(self, device_name="SEL", device_type=DeviceType.SELECTOR, port=0):
        """Initialize the queue selector."""
        super().__init__(device_name, device_type, port)

        # RTS2 values for monitoring and control
        self.enabled = ValueBool("enabled", "Enable/disable selector daemon", writable=True, initial=True)
        self.time_slice_value = ValueDouble("time_slice", "Time slice before target start to issue next command (seconds)", writable=True, initial=300.0)
        self.queue_size = ValueInteger("queue_size", "Number of targets in scheduler queue", initial=0)
        self.current_queue = ValueString("current_queue", "Currently active queue name", initial="")
        self.next_target = ValueInteger("next_target", "Next target to observe", initial=-1)
        self.system_state_desc = ValueString("system_state", "Current system state description", initial="unknown")
        self.grb_grace_active = ValueBool("grb_grace_active", "GRB grace period active", writable=True, initial=False)
        self.grb_grace_until = ValueTime("grb_grace_until", "GRB grace period end time", initial=0.0)

        # Grace period control callback is handled automatically via on_grb_grace_active_changed method

        # Executor state monitoring
        self.executor_current_target = ValueInteger("executor_current_target", "Current target running on executor", initial=-1)
        self.last_target_id = ValueInteger("last_target_id", "Last target we sent to executor", initial=-1)
        self.expected_executor_target = ValueInteger("expected_executor_target", "Target we expect executor to run", initial=-1)

    def start(self):
        """Start the selector daemon."""
        super().start()

        # Record startup time for initial evaluation
        self.startup_time = time.time()
        logging.info("Selector starting - will evaluate current situation on first loop")

        # Test database connection
        if not self._test_database():
            raise RuntimeError("Cannot connect to RTS2 database")

        # Monitor centrald for system state
        self.network.register_state_interest(
            device_name="centrald",
            state_callback=self._on_system_state_changed
        )

        # Monitor executor current target for external activity detection
        self.network.register_interest_in_value(
            device_name=self.executor_name,
            value_name="current_sel",
            callback=self._on_executor_current_changed
        )

        # Start selector thread
        self.running = True
        self.selector_thread = threading.Thread(target=self._selector_loop, daemon=True)
        self.selector_thread.start()

        logging.info("Queue selector started")

    def stop(self):
        """Stop the selector daemon."""
        self.running = False
        if self.selector_thread and self.selector_thread.is_alive():
            self.selector_thread.join(timeout=5.0)

        if self.db_conn:
            self.db_conn.close()

        super().stop()

    def info(self):
        """Update device information."""
        super().info()

        # Ensure GRB grace status is consistent (safety check)
        current_time = time.time()
        expected_active = current_time < self.grb_grace_until.value
        if self.grb_grace_active.value != expected_active:
            self.grb_grace_active.value = expected_active

    def _test_database(self) -> bool:
        """Test database connection."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()
            conn.close()
            logging.info(f"Database connection OK: {version[0]}")
            return True
        except Exception as e:
            logging.error(f"Database connection failed: {e}")
            return False

    def _on_system_state_changed(self, device_name, state, bop_state, message):
        """Handle system state changes from centrald."""
        old_state = self.system_state
        self.system_state = state

        # Extract state components using proper RTS2 masks
        period = state & CentralState.PERIOD_MASK
        onoff = state & CentralState.ONOFF_MASK

        # System ready for science observations: ON + NIGHT
        # (centrald automatically changes ON→STANDBY when weather is bad)
        self.system_ready = (onoff == CentralState.ON and period == CentralState.NIGHT)

        if self.system_ready and old_state != self.system_state:
            self.last_target_id.value = -1  # Clear to force re-evaluation

        # Issue calibration on any state change TO calibration time
        if old_state != state and (onoff == CentralState.ON and
                                  (period == CentralState.DUSK or period == CentralState.DAWN)):
            period_name = "dusk" if period == CentralState.DUSK else "dawn"
            logging.info(f"State changed to {period_name} - issuing calibration")
            self._send_executor_command("now 2")

        # Update system state description immediately
        state_desc = self._get_system_state_description()
        self.system_state_desc.value = state_desc

        if old_state != state:
            logging.info(f"System state: 0x{state:08x} ({state_desc}) "
                        f"{'ready for science' if self.system_ready else 'calibrations/standby'}")

    def _is_calibration_time(self) -> bool:
        """Check if current system state calls for calibrations."""
        period = self.system_state & CentralState.PERIOD_MASK
        onoff = self.system_state & CentralState.ONOFF_MASK

        # Calibrations during ON + (DUSK or DAWN)
        # (centrald handles weather by changing ON→STANDBY automatically)
        return (onoff == CentralState.ON and
                (period == CentralState.DUSK or period == CentralState.DAWN))

    def _get_system_state_description(self) -> str:
        """Get human-readable system state description."""
        period = self.system_state & CentralState.PERIOD_MASK
        onoff = self.system_state & CentralState.ONOFF_MASK
        weather = self.system_state & 0x80000000  # Weather bit

        # Map period states
        period_names = {
            CentralState.DAY: "DAY",
            CentralState.EVENING: "EVENING",
            CentralState.DUSK: "DUSK",
            CentralState.NIGHT: "NIGHT",
            CentralState.DAWN: "DAWN",
            CentralState.MORNING: "MORNING",
            CentralState.UNKNOWN: "UNKNOWN"
        }

        # Map on/off states
        onoff_names = {
            CentralState.ON: "ON",
            CentralState.STANDBY: "STANDBY",
            CentralState.SOFT_OFF: "SOFT_OFF",
            CentralState.HARD_OFF: "HARD_OFF"
        }

        period_name = period_names.get(period, f"UNKNOWN_PERIOD({period:x})")
        onoff_name = onoff_names.get(onoff, f"UNKNOWN_ONOFF({onoff:x})")

        description = f"{onoff_name}_{period_name}"
        if weather:
            description += "_BAD_WEATHER"

        return description

    def _selector_loop(self):
        """Main selector loop."""
        logging.info("Selector loop started")

        while self.running:
            try:

                logging.debug(f"Selector loop iteration - enabled: {self.enabled.value}, "
                         f"system_ready: {self.system_ready}, "
                         f"system_state: 0x{self.system_state:08x}, "
                         f"calibration_time: {self._is_calibration_time()}")

                # Check if daemon is enabled
                if not self.enabled.value:
                    logging.debug("Selector daemon disabled - waiting")
                    # Clear target info when disabled
                    if self.next_target.value != -1:
                        self.next_target.value = -1
                        self.current_queue.value = ""
                    time.sleep(self.update_interval)
                    continue

                # Clean up expired targets periodically
                self._cleanup_expired_targets()

                # Only run when system is ready
                if not self.system_ready and not self._is_calibration_time():

                    logging.debug(f"System not ready for observations, "
                             f"waiting for ON_NIGHT or ON_DUSK/ON_DAWN")

                    # Clear target info when system not active
                    if self.next_target.value != -1:
                        self.next_target.value = -1
                        self.current_queue.value = ""
                    time.sleep(self.update_interval)
                    continue

                logging.debug("System ready for target selection")

                # Handle startup evaluation first
                if not self.startup_completed:
                    self._perform_startup_evaluation()
                    time.sleep(self.update_interval)
                    continue

                # Check for GRB grace period
                if self._detect_external_activity():
                    logging.debug("In GRB grace period, skipping target selection")
                    time.sleep(self.update_interval)
                    continue
                else:
                    # Grace period expired, update status immediately
                    if self.grb_grace_active.value:
                        self.grb_grace_active.value = False

                logging.debug("Selecting next target...")

                # Get next target to execute with timing information
                target, time_until_action = self._select_next_target()

                if target:
                    logging.info(f"Selected target: {target}")
                    self._execute_target(target)

                    # Calculate sleep time - use time_until_action if available and reasonable
                    if time_until_action is not None and 0 < time_until_action <= 3600:  # Max 1 hour
                        sleep_time = min(self.update_interval, time_until_action)
                        logging.debug(f"Next action needed in {time_until_action:.1f}s, sleeping for {sleep_time:.1f}s")
                    else:
                        sleep_time = self.update_interval
                        logging.debug(f"Using standard sleep interval: {sleep_time:.1f}s")
                else:
                    # No target selected - update values immediately
                    if self.next_target.value != -1:
                        self.next_target.value = -1
                        self.current_queue.value = ""
                    logging.debug("No target selected")
                    sleep_time = self.update_interval

                time.sleep(sleep_time)

            except Exception as e:
                logging.error(f"Error in selector loop: {e}")
                time.sleep(self.update_interval)

        logging.info("Selector loop stopped")

    def _select_next_target(self) -> Tuple[Optional[ScheduledTarget], Optional[float]]:
        """Select next target based on queue priority and timing (for NEXT command).

        Returns:
            Tuple of (target, time_until_action) where time_until_action is seconds
            until we should take action on this target, or None if no target found.
        """

        # 1. Check for calibrations first (during DUSK/DAWN)
        if self._is_calibration_time():
            # Return calibration target with immediate action
            calibration_target = ScheduledTarget(
                qid=-1, tar_id=self.TARGET_FLAT,
                queue_start=datetime.now(timezone.utc),
                queue_end=None,
                tar_name="calibration",
                tar_ra=0.0, tar_dec=0.0
            )
            return calibration_target, 0  # Action needed immediately

        # 2. Check scheduler queue for time-scheduled targets (NEXT mode)
        scheduler_target, time_until_action = self._get_scheduler_target(mode="next")
        if scheduler_target:
            return scheduler_target, time_until_action

        # 3. Check manual queue only (queue_id=1)
        # Note: Other queues (grb, integral_targets, regular_targets, etc.)
        # are legacy and should not be processed by this selector
        if 'manual' in self.queue_name_to_id:
            targets = self._get_queue_targets('manual', limit=1)
            if targets:
                qid, tar_id, time_start, time_end, tar_name, tar_ra, tar_dec, queue_order = targets[0]
                target = ScheduledTarget(
                    qid=qid, tar_id=tar_id,
                    queue_start=time_start or datetime.now(timezone.utc),
                    queue_end=time_end,
                    tar_name=tar_name or f"target_{tar_id}",
                    tar_ra=tar_ra or 0.0, tar_dec=tar_dec or 0.0,
                    queue_order=queue_order or 0
                )
                # Manual queue targets should start immediately
                return target, 0

        return None, None

    def _select_current_target(self) -> Optional[ScheduledTarget]:
        """Select target that should be running RIGHT NOW (for NOW command)."""

        # 1. Check for calibrations first (during DUSK/DAWN)
        if self._is_calibration_time():
            # Return calibration target
            return ScheduledTarget(
                qid=-1, tar_id=self.TARGET_FLAT,
                queue_start=datetime.now(timezone.utc),
                queue_end=None,
                tar_name="calibration",
                tar_ra=0.0, tar_dec=0.0
            )

        # 2. Check scheduler queue for currently active targets (NOW mode)
        scheduler_target, _ = self._get_scheduler_target(mode="now")
        if scheduler_target:
            return scheduler_target

        # 3. Check manual queue for currently active targets
        # For manual queue, we don't have time constraints - just return first target
        if 'manual' in self.queue_name_to_id:
            targets = self._get_queue_targets('manual', limit=1)
            if targets:
                qid, tar_id, time_start, time_end, tar_name, tar_ra, tar_dec, queue_order = targets[0]
                return ScheduledTarget(
                    qid=qid, tar_id=tar_id,
                    queue_start=time_start or datetime.now(timezone.utc),
                    queue_end=time_end,
                    tar_name=tar_name or f"target_{tar_id}",
                    tar_ra=tar_ra or 0.0, tar_dec=tar_dec or 0.0,
                    queue_order=queue_order or 0
                )

        return None

    def _cleanup_expired_targets(self):
        """Remove targets where both start and end times are in the past."""
        try:
            if not self.db_conn or self.db_conn.closed:
                self.db_conn = psycopg2.connect(**self.db_config)

            cursor = self.db_conn.cursor()
            current_time = datetime.now(timezone.utc)

            # Remove targets where BOTH start and end times are in the past
            cursor.execute("""
                DELETE FROM queues_targets
                WHERE time_start IS NOT NULL
                  AND time_end IS NOT NULL
                  AND time_start < %s
                  AND time_end < %s
            """, (current_time, current_time))

            deleted_count = cursor.rowcount
            self.db_conn.commit()

            if deleted_count > 0:
                logging.info(f"Cleaned up {deleted_count} expired targets")

        except Exception as e:
            logging.error(f"Error cleaning expired targets: {e}")
            if self.db_conn:
                self.db_conn.rollback()

    def _get_scheduler_target(self, mode="next") -> Tuple[Optional[ScheduledTarget], Optional[float]]:
        """Get target from scheduler queue.

        Args:
            mode: "next" for upcoming targets (within time_slice window)
                  "now" for currently active targets (start <= now <= end)

        Returns:
            Tuple of (target, time_until_action) where time_until_action is seconds
            until we should take action on this target, or None if no target found.
        """
        try:
            if not self.db_conn or self.db_conn.closed:
                self.db_conn = psycopg2.connect(**self.db_config)

            cursor = self.db_conn.cursor()
            scheduler_queue_id = self.queue_name_to_id.get('scheduler')
            if scheduler_queue_id is None:
                return None

            logging.debug(f"Scheduler queue lookup ({mode}): scheduler_queue_id = {scheduler_queue_id}")

            current_time = datetime.now(timezone.utc)

            if mode == "next":
                # Get next target that's either future or slightly overdue (within time_slice)
                time_slice_interval = f"{self.time_slice_value.value} seconds"
                logging.debug(f"Looking for NEXT targets >= {current_time - timedelta(seconds=self.time_slice_value.value)}")

                query = f"""
                    SELECT qt.qid, qt.tar_id, qt.time_start, qt.time_end,
                           t.tar_name, t.tar_ra, t.tar_dec, qt.queue_order
                    FROM queues_targets qt
                    JOIN targets t ON qt.tar_id = t.tar_id
                    WHERE qt.queue_id = %s
                      AND (qt.time_start IS NULL OR qt.time_start >= %s - interval '{time_slice_interval}')
                    ORDER BY COALESCE(qt.time_start, %s), qt.queue_order
                    LIMIT 1
                """
                query_params = (scheduler_queue_id, current_time, current_time)

            elif mode == "now":
                # Get target that should be running RIGHT NOW
                logging.debug(f"Looking for NOW targets: start <= {current_time} <= end")

                query = """
                    SELECT qt.qid, qt.tar_id, qt.time_start, qt.time_end,
                           t.tar_name, t.tar_ra, t.tar_dec, qt.queue_order
                    FROM queues_targets qt
                    JOIN targets t ON qt.tar_id = t.tar_id
                    WHERE qt.queue_id = %s
                      AND (qt.time_start IS NULL OR qt.time_start <= %s)
                      AND (qt.time_end IS NULL OR qt.time_end >= %s)
                    ORDER BY qt.queue_order, qt.time_start
                    LIMIT 1
                """
                query_params = (scheduler_queue_id, current_time, current_time)
            else:
                logging.error(f"Unknown mode: {mode}")
                return None

            cursor.execute(query, query_params)
            row = cursor.fetchone()

            if row:
                qid, tar_id, time_start, time_end, tar_name, tar_ra, tar_dec, queue_order = row

                # Ensure timezone awareness
                if time_start and time_start.tzinfo is None:
                    time_start = time_start.replace(tzinfo=timezone.utc)
                if time_end and time_end.tzinfo is None:
                    time_end = time_end.replace(tzinfo=timezone.utc)

                # Update queue size for monitoring (only for "next" mode to avoid spam)
                if mode == "next":
                    cursor.execute("""
                        SELECT COUNT(*) FROM queues_targets
                        WHERE queue_id = %s
                    """, (scheduler_queue_id,))
                    count = cursor.fetchone()[0]
                    self.queue_size.value = count

                target = ScheduledTarget(
                    qid=qid, tar_id=tar_id,
                    queue_start=time_start or current_time,
                    queue_end=time_end,
                    tar_name=tar_name or f"target_{tar_id}",
                    tar_ra=tar_ra or 0.0, tar_dec=tar_dec or 0.0,
                    queue_order=queue_order or 0
                )
                logging.debug(f"Found {mode} target: {target.tar_id} ({target.tar_name})")

                # Calculate time until action needed
                time_until_action = None
                if mode == "next":
                    # For next mode, we need to issue command when current target is within time_slice of ending
                    # But we don't know current target end time here, so return None for now
                    # The main logic will handle this in _execute_target
                    time_until_action = None
                elif mode == "now":
                    # For now mode, action needed when target should start
                    if target.queue_start and target.queue_start > current_time:
                        time_until_action = (target.queue_start - current_time).total_seconds()
                    else:
                        time_until_action = 0  # Should start now or is overdue

                return target, time_until_action

            else:
                if mode == "next":
                    # Only show detailed debug info for "next" mode
                    cursor.execute("""
                        SELECT COUNT(*), MIN(qt.time_start), MAX(qt.time_start)
                        FROM queues_targets qt
                        WHERE qt.queue_id = %s
                    """, (scheduler_queue_id,))
                    count, min_start, max_start = cursor.fetchone()

                    if count > 0:
                        logging.debug(f"No {mode} target found. Total in scheduler queue: {count} (range: {min_start} to {max_start})")

                    self.queue_size.value = 0

                return None, None

        except Exception as e:
            logging.error(f"Error querying scheduler queue ({mode}): {e}")
            return None, None

    def _get_queue_targets(self, queue_name: str, limit: int = 10) -> List[Tuple]:
        """Get targets from specified queue."""
        try:
            if not self.db_conn or self.db_conn.closed:
                self.db_conn = psycopg2.connect(**self.db_config)

            cursor = self.db_conn.cursor()
            queue_id = self.queue_name_to_id.get(queue_name)

            if queue_id is None:
                logging.warning(f"Unknown queue name: {queue_name}")
                return []

            query = """
                SELECT qt.qid, qt.tar_id, qt.time_start, qt.time_end,
                       t.tar_name, t.tar_ra, t.tar_dec, qt.queue_order
                FROM queues_targets qt
                JOIN targets t ON qt.tar_id = t.tar_id
                WHERE qt.queue_id = %s
                ORDER BY COALESCE(qt.time_start, '2000-01-01'::timestamp), qt.queue_order
                LIMIT %s
            """

            cursor.execute(query, (queue_id, limit))
            rows = cursor.fetchall()

            # Convert timezone-naive datetimes to UTC timezone-aware
            result = []
            for row in rows:
                qid, tar_id, time_start, time_end, tar_name, tar_ra, tar_dec, queue_order = row
                if time_start and time_start.tzinfo is None:
                    time_start = time_start.replace(tzinfo=timezone.utc)
                if time_end and time_end.tzinfo is None:
                    time_end = time_end.replace(tzinfo=timezone.utc)
                result.append((qid, tar_id, time_start, time_end, tar_name, tar_ra, tar_dec, queue_order))

            return result

        except Exception as e:
            logging.error(f"Error querying queue {queue_name}: {e}")
            return []

    def _execute_target(self, target: ScheduledTarget):
        current_time = datetime.now(timezone.utc)

        # Skip targets without proper timing
        if not target.queue_start or not target.queue_end:
            logging.debug(f"Skipping target {target.tar_id} - missing start/end times")
            return

        # Check if desired target is already running - no action needed
        if self.executor_current_target.value == target.tar_id:
            logging.debug(f"Target {target.tar_id} already running - no action needed")
            self._update_target_status(target)  # Update status for monitoring
            return

        # Find current target's end time if one is running
        current_target_end = None
        if self.executor_current_target.value != -1:
            current_target_end = self._get_target_end_time(self.executor_current_target.value)

        logging.debug(f"Execute target logic: current={self.executor_current_target.value}, "
                     f"desired={target.tar_id}, current_end={current_target_end}, "
                     f"target_start={target.queue_start}")

        # Determine command timing
        if current_target_end and current_target_end > current_time:
            # Current target still running - check if we're in last timeslice
            time_until_current_ends = (current_target_end - current_time).total_seconds()

            if time_until_current_ends <= self.time_slice_value.value:
                # In last timeslice of current target - prepare next
                command = f"next {target.tar_id}"
                logging.info(f"Current target {self.executor_current_target.value} ending in {time_until_current_ends:.0f}s - "
                            f"issuing 'next' for {target.tar_id}")
                self._send_executor_command(command)
                self._update_target_status(target)
            else:
                # Current target still has time - don't change next yet
                logging.debug(f"Current target {self.executor_current_target.value} has {time_until_current_ends:.0f}s left - waiting")
                return

        elif target.queue_start <= current_time:
            # New target should start now - force transition
            command = f"now {target.tar_id}"
            if target.queue_start < current_time:
                delay = (current_time - target.queue_start).total_seconds()
                logging.info(f"Issuing 'now' for {target.tar_id} (delayed by {delay:.0f}s)")
            else:
                logging.info(f"Issuing 'now' for {target.tar_id}")
            self._send_executor_command(command)
            self._update_target_status(target)
        else:
            # Too early for next target
            return

    def _get_target_end_time(self, target_id: int) -> Optional[datetime]:
        """Get end time for currently executing target."""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT time_end FROM queues_targets
                WHERE tar_id = %s AND time_start IS NOT NULL AND time_end IS NOT NULL
                ORDER BY time_start DESC LIMIT 1
            """, (target_id,))

            result = cursor.fetchone()
            if result and result[0]:
                end_time = result[0]
                if end_time.tzinfo is None:
                    end_time = end_time.replace(tzinfo=timezone.utc)
                return end_time
        except Exception as e:
            logging.error(f"Error getting end time for target {target_id}: {e}")

        return None

    def _update_target_status(self, target: ScheduledTarget):
        """Update status values for monitoring."""
        if target.tar_id == self.TARGET_FLAT:
            self.current_queue.value = "calibration"
        elif target.qid == 0:
            self.current_queue.value = "manual"
        else:
            # Determine queue from target source
            for queue_name, queue_id in self.queue_name_to_id.items():
                if queue_id == 1:
                    self.current_queue.value = "manual"
                    break
                elif queue_id == 2:
                    self.current_queue.value = "scheduler"
                    break
            else:
                self.current_queue.value = "unknown"

        self.next_target.value = target.tar_id

    def _ex_send_executor_command(self, command: str) -> bool:
        """Send command to executor device."""
        try:
            # Extract target ID from command for tracking
            parts = command.split()
            if len(parts) >= 2 and parts[0] in ['next', 'now']:
                target_id = int(parts[1])
                self.expected_executor_target.value = target_id
                logging.debug(f"Setting expected executor target to {target_id}")

            # Find executor connection (same pattern as grbd.py)
            executor_conn = None
            for conn in self.network.connection_manager.connections.values():
                if (hasattr(conn, 'remote_device_name') and conn.remote_device_name == self.executor_name): # and conn.state == ConnectionState.AUTH_OK):
        #            logging.info(f"{conn}")
                    executor_conn = conn
                    break

            if executor_conn:
                success = executor_conn.send_command(command, self._on_executor_command_result)
                logging.debug(f"Successfully sent command '{command}' to executor")
                return success
            else:
                logging.warning(f"No authenticated executor connection found")
                return False

        except Exception as e:
            logging.error(f"Error sending command '{command}' to executor: {e}")
            return False

    def _send_executor_command(self, command: str) -> bool:
        """Send command to executor device."""
        try:
            # Extract target ID from command for tracking
            parts = command.split()
            if len(parts) >= 2 and parts[0] in ['next', 'now']:
                target_id = int(parts[1])
                self.expected_executor_target.value = target_id
                logging.debug(f"Setting expected executor target to {target_id}")

            # Find executor connection using device type (more reliable than name)
            from constants import DeviceType
            from netman import ConnectionState

            executor_conn = None
            for conn in self.network.connection_manager.connections.values():
                if (hasattr(conn, 'remote_device_type') and
                    conn.remote_device_type == DeviceType.EXECUTOR):
                    # Note: Removed AUTH_OK requirement - value subscriptions work but
                    # command connections may not complete auth properly
                    executor_conn = conn
                    logging.debug(f"Found executor connection by type: {conn.state}")
                    break

            # Fallback: try finding by name if type search fails
            if not executor_conn:
                for conn in self.network.connection_manager.connections.values():
                    if (hasattr(conn, 'remote_device_name') and
                        conn.remote_device_name == self.executor_name):
                        # Note: Removed AUTH_OK requirement - device-to-device connections
                        # may have authentication issues but still allow command sending
                        executor_conn = conn
                        logging.debug(f"Found executor connection by name: {conn.state}")
                        break

            if executor_conn:
                success = executor_conn.send_command(command, self._on_executor_command_result)
                logging.debug(f"Successfully sent command '{command}' to executor")
                return success
            else:
                logging.warning(f"No authenticated executor connection found")
                return False

        except Exception as e:
            logging.error(f"Error sending command '{command}' to executor: {e}")
            return False

    def _remove_executed_target(self, target: ScheduledTarget):
        """Remove executed targets to prevent re-execution."""
        try:
            if not self.db_conn or self.db_conn.closed:
                self.db_conn = psycopg2.connect(**self.db_config)

            cursor = self.db_conn.cursor()

            # Remove the specific target from queue
            cursor.execute("""
                DELETE FROM queues_targets
                WHERE qid = %s
            """, (target.qid,))

            rows_deleted = cursor.rowcount
            self.db_conn.commit()

            if rows_deleted > 0:
                logging.info(f"Removed executed target {target.tar_id} from queue")

        except Exception as e:
            logging.error(f"Error removing target {target.tar_id} from queue: {e}")
            if self.db_conn:
                self.db_conn.rollback()

    def _on_executor_current_changed(self, value_data):
        """Handle executor current target changes to detect external activity."""
        try:
            # Always update executor current target (no startup blocking)
            old_target = self.executor_current_target.value
            self.executor_current_target.value = int(value_data) if value_data and value_data != "-1" else -1

            logging.debug(f"Executor target changed: {old_target} -> {self.executor_current_target.value}")

            # Skip external activity detection during startup
            if not self.startup_completed:
                return

            # Check if this is external activity (only after startup)
            # We consider it "our" activity if current target is either:
            # 1. The target we just sent as "next" (expected_executor_target)
            # 2. The target we sent previously (last_target_id) - might still be running
            our_targets = {self.expected_executor_target.value, self.last_target_id.value}
            our_targets.discard(-1)  # Remove -1 values

            if self.executor_current_target.value != -1 and self.executor_current_target.value not in our_targets:
                # Executor is running a target we didn't tell it to run
                logging.info(f"External activity detected: executor running target {self.executor_current_target.value}, "
                            f"our targets: {our_targets}")
                self._set_grb_grace_period()
            else:
                logging.debug(f"Executor current target: {self.executor_current_target.value} (expected: {our_targets})")

        except (ValueError, TypeError) as e:
            logging.debug(f"Could not parse executor current target '{value_data}': {e}")

    def _detect_external_activity(self) -> bool:
        """Check if we're in a grace period due to external activity."""
        current_time = time.time()
        return current_time < self.grb_grace_until.value

    def _set_grb_grace_period(self):
        """Set GRB grace period when external activity detected."""
        grace_end = time.time() + self.grb_grace_period
        self.grb_grace_until.value = grace_end
        self.grb_grace_active.value = True  # Update immediately
        logging.info(f"External activity detected - setting grace period until {grace_end}")
        logging.info(f"Grace period: {self.grb_grace_period}s")

    def _perform_startup_evaluation(self):
        """Perform initial evaluation at startup - only handle NOW commands."""
        logging.info("Performing startup evaluation...")

        if self.executor_current_target.value != -1:
            logging.info(f"Executor currently running target {self.executor_current_target.value}")
        else:
            logging.info("Executor not currently running any target")

        # Get what should be running RIGHT NOW
        target_to_run = self._select_current_target()

        if not target_to_run:
            logging.info("No target should be running now - startup complete")
            self.startup_completed = True
            return

        # Check if correct target is already running
        if self.executor_current_target.value == target_to_run.tar_id:
            logging.info(f"Correct target {self.executor_current_target.value} already running - startup complete")
            self.last_target_id.value = self.executor_current_target.value
            self.expected_executor_target.value = self.executor_current_target.value
            self._update_target_status(target_to_run)
            self.startup_completed = True
            return

        # Wrong target running or nothing running - take immediate action
        if self.executor_current_target.value != -1:
            logging.info(f"Wrong target {self.executor_current_target.value} running, should be {target_to_run.tar_id} - issuing 'now'")
        else:
            logging.info(f"Nothing running, should be {target_to_run.tar_id} - issuing 'now'")

        # Issue NOW command only (never NEXT during startup)
        command = f"now {target_to_run.tar_id}"
        logging.info(f"Startup action: {command}")
        self._send_executor_command(command)
        self._update_target_status(target_to_run)

        self.startup_completed = True

    def _on_executor_command_result(self, conn, success, code, message):
        """Handle result from executor command."""
        if success:
            logging.info(f"Executor command successful: {message}")
        else:
            logging.warning(f"Executor command failed: {message}")

    def on_grb_grace_active_changed(self, old_value, new_value):
        """Handle external changes to grb_grace_active value."""
        current_time = time.time()

        if new_value and not old_value:
            # false->true: Initiate grace period with current target
            self.grb_grace_until.value = current_time + self.grb_grace_period
            current_target = self.executor_current_target.value
            if current_target != -1:
                logging.info(f"Grace period manually initiated for current target {current_target} "
                           f"(until {self.grb_grace_until.value}, {self.grb_grace_period}s duration)")
            else:
                logging.info(f"Grace period manually initiated (no current target, "
                           f"until {self.grb_grace_until.value}, {self.grb_grace_period}s duration)")

        elif not new_value and old_value:
            # true->false: Remove grace period immediately
            self.grb_grace_until.value = 0.0
            logging.info("Grace period manually removed - selector will resume normal operation")


def main():
    """Main entry point."""

    # Create application
    app = App(description='RTS2 Queue Selector Daemon')

    # Register device options
    app.register_device_options(QueueSelector)

    # Parse arguments
    args = app.parse_args()

    # Create device
    device = app.create_device(QueueSelector)

    logging.info("Starting RTS2 Queue Selector")
    logging.info(f"Database: {device.db_config['database']} on {device.db_config['host']}")
    logging.info(f"Queues: {list(device.queue_name_to_id.keys())}")

    # Run application
    try:
        app.run()
        return 0
    except KeyboardInterrupt:
        logging.info("Shutting down queue selector")
        return 0
    except Exception as e:
        logging.error(f"Fatal error in queue selector: {e}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
