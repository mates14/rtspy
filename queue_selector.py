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
from datetime import datetime, timezone
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

        # Runtime state
        self.db_conn = None
        self.selector_thread = None
        self.running = False

        # System state tracking
        self.system_state = 0
        self.system_ready = False

        # Last selected target
        self.last_target_id = None
        self.last_command_time = 0.0
        self.expected_executor_target = None  # Target we told executor to run

    def __init__(self, device_name="SEL", device_type=DeviceType.SELECTOR, port=0):
        """Initialize the queue selector."""
        super().__init__(device_name, device_type, port)

        # RTS2 values for monitoring
        self.queue_size = ValueInteger("queue_size", "Number of targets in scheduler queue", initial=0)
        self.current_queue = ValueString("current_queue", "Currently active queue name", initial="none")
        self.next_target = ValueString("next_target", "Next target to observe", initial="none")
        self.system_state_desc = ValueString("system_state", "Current system state description", initial="unknown")
        self.grb_grace_active = ValueBool("grb_grace_active", "GRB grace period active", initial=False)
        self.grb_grace_until = ValueTime("grb_grace_until", "GRB grace period end time", initial=0.0)
        self.last_update = ValueTime("last_update", "Last selector update time", initial=time.time())

    def start(self):
        """Start the selector daemon."""
        super().start()

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
            value_name="current",
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

        # Update last update time
        self.last_update.value = time.time()

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
                # Only run when system is ready
                if not self.system_ready and not self._is_calibration_time():
                    # Clear target info when system not active
                    if self.next_target.value != "none":
                        self.next_target.value = "none"
                        self.current_queue.value = "none"
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

                # Get next target to execute
                target = self._select_next_target()

                if target:
                    self._execute_target(target)
                else:
                    # No target selected - update values immediately
                    if self.next_target.value != "none":
                        self.next_target.value = "none"
                        self.current_queue.value = "none"
                    logging.debug("No target selected")

                time.sleep(self.update_interval)

            except Exception as e:
                logging.error(f"Error in selector loop: {e}")
                time.sleep(self.update_interval)

        logging.info("Selector loop stopped")

    def _select_next_target(self) -> Optional[ScheduledTarget]:
        """Select next target based on queue priority and timing."""

        # 1. Check for calibrations first (during DUSK/DAWN)
        calib_target = self._select_calibration()
        if calib_target:
            return calib_target

        # 2. Check scheduler queue for time-scheduled targets
        scheduler_target = self._get_next_scheduler_target()
        if scheduler_target:
            return scheduler_target

        # 3. Check other queues in priority order
        queue_priority = ['grb', 'manual', 'integral_targets', 'regular_targets',
                         'longscript', 'longscript2', 'plan']

        for queue_name in queue_priority:
            if queue_name in self.queue_name_to_id:
                targets = self._get_queue_targets(queue_name, limit=1)
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

    def _select_calibration(self) -> Optional[ScheduledTarget]:
        """Select calibration target during DUSK/DAWN system states."""
        if not self._is_calibration_time():
            return None

        # Determine period for logging
        period = self.system_state & CentralState.PERIOD_MASK
        period_name = "dusk" if period == CentralState.DUSK else "dawn"

        logging.debug(f"System state indicates {period_name} - selecting calibrations")

        return ScheduledTarget(
            qid=0, tar_id=self.TARGET_FLAT,
            queue_start=datetime.now(timezone.utc), queue_end=None,
            tar_name="calibration", tar_ra=0.0, tar_dec=0.0
        )

    def _get_next_scheduler_target(self) -> Optional[ScheduledTarget]:
        """Get next target from scheduler queue."""
        try:
            if not self.db_conn or self.db_conn.closed:
                self.db_conn = psycopg2.connect(**self.db_config)
            cursor = self.db_conn.cursor()

            # Get scheduler queue ID
            scheduler_queue_id = self.queue_name_to_id.get('scheduler')
            if scheduler_queue_id is None:
                return None

            # Query queues_targets table - get next target scheduled for now or past
            current_time = datetime.now(timezone.utc)

            query = """
                SELECT qt.qid, qt.tar_id, qt.time_start, qt.time_end,
                       t.tar_name, t.tar_ra, t.tar_dec, qt.queue_order
                FROM queues_targets qt
                JOIN targets t ON qt.tar_id = t.tar_id
                WHERE qt.queue_id = %s
                  AND (qt.time_start IS NULL OR qt.time_start <= %s)
                  AND qt.time_start > %s - interval '6 hours'
                ORDER BY COALESCE(qt.time_start, %s), qt.queue_order
                LIMIT 1
            """

            cursor.execute(query, (scheduler_queue_id, current_time, current_time, current_time))
            row = cursor.fetchone()

            if row:
                qid, tar_id, time_start, time_end, tar_name, tar_ra, tar_dec, queue_order = row

                # Ensure database datetimes are timezone-aware (assume UTC)
                if time_start and time_start.tzinfo is None:
                    time_start = time_start.replace(tzinfo=timezone.utc)
                if time_end and time_end.tzinfo is None:
                    time_end = time_end.replace(tzinfo=timezone.utc)

                # Update queue size for monitoring
                cursor.execute("""
                    SELECT COUNT(*) FROM queues_targets
                    WHERE queue_id = %s AND (time_start IS NULL OR time_start > %s)
                """, (scheduler_queue_id, current_time))
                count = cursor.fetchone()[0]
                self.queue_size.value = count

                return ScheduledTarget(
                    qid=qid, tar_id=tar_id,
                    queue_start=time_start or current_time,
                    queue_end=time_end,
                    tar_name=tar_name or f"target_{tar_id}",
                    tar_ra=tar_ra or 0.0, tar_dec=tar_dec or 0.0,
                    queue_order=queue_order or 0
                )
            else:
                self.queue_size.value = 0
                return None

        except Exception as e:
            logging.error(f"Error querying scheduler queue: {e}")
            return None

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
        """Execute target with appropriate timing."""
        current_time = datetime.now(timezone.utc)

        # Skip if we just executed this target
        if (target.tar_id == self.last_target_id and
            current_time.timestamp() - self.last_command_time < 60.0):
            return

        # Determine which queue provided this target and update immediately
        if target.tar_id == self.TARGET_FLAT:
            self.current_queue.value = "calibration"
        elif target.qid == 0:
            self.current_queue.value = "manual"
        else:
            # Determine queue from target source
            for queue_name, queue_id in self.queue_name_to_id.items():
                if queue_id == 1:  # Manual queue check could be more sophisticated
                    self.current_queue.value = "manual"
                    break
                elif queue_id == 2:  # Scheduler queue
                    self.current_queue.value = "scheduler"
                    break
            else:
                self.current_queue.value = "unknown"

        # Update next target immediately
        self.next_target.value = target.tar_name

        # Determine command timing
        if target.queue_start and target.queue_start > current_time:
            # Target is scheduled for future - check if we should issue 'next'
            time_until_start = (target.queue_start - current_time).total_seconds()

            if time_until_start <= self.time_slice:
                # Issue 'next' command to prepare executor
                command = f"next {target.tar_id}"
                logging.info(f"Issuing 'next' for target {target.tar_id} ({target.tar_name}) "
                           f"starting in {time_until_start:.0f}s")
            else:
                # Too early - don't issue command yet
                return
        else:
            # Target should start now - issue 'now' command
            command = f"now {target.tar_id}"
            logging.info(f"Issuing 'now' for target {target.tar_id} ({target.tar_name})")

        # Send command to executor
        try:
            if self._send_executor_command(command):
                self.last_target_id = target.tar_id
                self.last_command_time = current_time.timestamp()

                # Remove executed target from queue (if needed)
                self._remove_executed_target(target)

        except Exception as e:
            logging.error(f"Error executing target {target.tar_id}: {e}")

    def _send_executor_command(self, command: str) -> bool:
        """Send command to executor device."""
        try:
            # Extract target ID from command for tracking
            parts = command.split()
            if len(parts) >= 2 and parts[0] in ['next', 'now']:
                target_id = int(parts[1])
                self.expected_executor_target = target_id
                logging.debug(f"Setting expected executor target to {target_id}")

            # Find executor connection (same pattern as grbd.py)
            executor_conn = None
            for conn in self.network.connection_manager.connections.values():
                if (hasattr(conn, 'remote_device_name') and conn.remote_device_name == self.executor_name): # and conn.state == ConnectionState.AUTH_OK):
                    logging.info(f"{conn}")
                    executor_conn = conn
                    break

            if executor_conn:
                success = executor_conn.send_command(command)
                logging.debug(f"Successfully sent command '{command}' to executor")
                return success
            else:
                logging.warning(f"No authenticated executor connection found")
                return False

        except Exception as e:
            logging.error(f"Error sending command '{command}' to executor: {e}")
            return False

    def _remove_executed_target(self, target: ScheduledTarget):
        """Remove executed target from database queue."""
        try:
            if not self.db_conn or self.db_conn.closed:
                self.db_conn = psycopg2.connect(**self.db_config)
                self.db_conn.set_session(timezone='UTC')

            cursor = self.db_conn.cursor()

            # Check if queue has remove_after_execution flag
            queue_id = self.queue_name_to_id.get('scheduler', 2)
            cursor.execute("""
                SELECT remove_after_execution FROM queues WHERE queue_id = %s
            """, (queue_id,))

            row = cursor.fetchone()
            if row and row[0]:  # remove_after_execution is True
                cursor.execute("DELETE FROM queues_targets WHERE qid = %s", (target.qid,))
                self.db_conn.commit()
                logging.debug(f"Removed executed target qid={target.qid} from queue")

        except Exception as e:
            logging.error(f"Error removing executed target: {e}")

    def _on_executor_current_changed(self, value_data):
        """Handle executor current target changes to detect external activity."""
        try:
            # Parse current target ID from executor
            current_target = int(value_data) if value_data and value_data != "-1" else None

            # Check if this is external activity
            # We consider it "our" activity if current target is either:
            # 1. The target we just sent as "next" (expected_executor_target)
            # 2. The target we sent previously (last_target_id) - might still be running
            our_targets = {self.expected_executor_target, self.last_target_id}
            our_targets.discard(None)  # Remove None values

            if current_target is not None and current_target not in our_targets:
                # Executor is running a target we didn't tell it to run
                logging.info(f"External activity detected: executor running target {current_target}, "
                            f"our targets: {our_targets}")
                self._set_grb_grace_period()
            else:
                logging.debug(f"Executor current target: {current_target} (expected: {our_targets})")

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
