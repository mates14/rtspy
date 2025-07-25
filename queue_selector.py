#!/usr/bin/env python3

"""
RTS2 Queue Selector Daemon

A simplified queue-based target selector that:
1. Reads scheduled targets from the RTS2 PostgreSQL database
2. Executes targets from queues in proper time order
3. Handles calibrations (flats/darks) at appropriate times
4. Respects GRB grace periods to avoid interrupting alert observations

This replaces the complex C++ selector with a simple queue executor.
"""

import time
import math
import logging
import threading
import psycopg2
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple

from device import Device
from config import DeviceConfig
from app import App
from value import ValueDouble, ValueInteger, ValueBool, ValueTime, ValueString
from constants import DeviceType


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
    TARGET_FLAT = 2

    def setup_config(self, config):
        """Register selector-specific configuration."""

        # Database configuration (can override rts2.ini)
        config.add_argument('--db-host', default=None,
                          help='Database host', section='database')
        config.add_argument('--db-name', default=None,
                          help='Database name', section='database')
        config.add_argument('--db-user', default=None,
                          help='Database user', section='database')
        config.add_argument('--db-password', default=None,
                          help='Database password', section='database')

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
        self.grb_grace_end = 0.0

        # System state tracking
        self.system_state = 0
        self.system_ready = False

        # Last selected target
        self.last_target_id = None
        self.last_command_time = 0.0

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

        # Update system state description
        state_desc = self._get_system_state_description()
        self.system_state_desc.value = state_desc

        # Update last update time
        self.last_update.value = time.time()

        # Update GRB grace status
        current_time = time.time()
        if current_time < self.grb_grace_end:
            self.grb_grace_active.value = True
            self.grb_grace_until.value = self.grb_grace_end
        else:
            self.grb_grace_active.value = False

    def _get_system_state_description(self) -> str:
        """Get human-readable system state description."""
        state = self.system_state & 0xFF  # Base state
        weather = self.system_state & 0x80000000  # Weather bit

        # RTS2 system states
        states = {
            0x01: "OFF", 0x02: "STANDBY", 0x03: "ON",
            0x10: "DUSK", 0x20: "NIGHT", 0x40: "DAWN", 0x80: "DAY"
        }

        base_state = states.get(state & 0x0F, f"UNKNOWN({state & 0x0F:x})")
        period_state = states.get(state & 0xF0, "")

        description = base_state
        if period_state:
            description += f"_{period_state}"
        if weather:
            description += "_BAD_WEATHER"

        return description

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

        # Check if system is ready for observations
        base_state = state & 0x0F
        period_state = state & 0xF0
        weather_ok = not (state & 0x80000000)

        # System ready for science observations: ON + NIGHT + good weather
        self.system_ready = (base_state == 0x03 and period_state == 0x20 and weather_ok)

        if old_state != state:
            state_desc = self._get_system_state_description()
            logging.info(f"System state: 0x{state:08x} ({state_desc}) "
                        f"{'ready for science' if self.system_ready else 'calibrations/standby'}")

    def _is_calibration_time(self) -> bool:
        """Check if current system state calls for calibrations."""
        base_state = self.system_state & 0x0F
        period_state = self.system_state & 0xF0
        weather_ok = not (self.system_state & 0x80000000)

        # Calibrations during ON + DUSK or ON + DAWN with good weather
        return (base_state == 0x03 and
                (period_state == 0x10 or period_state == 0x40) and
                weather_ok)

    def _selector_loop(self):
        """Main selector loop."""
        logging.info("Selector loop started")

        while self.running:
            try:
                # Only run when system is ready
                if not self.system_ready:
                    time.sleep(self.update_interval)
                    continue

                # Check for GRB grace period
                current_time = time.time()
                if current_time < self.grb_grace_end:
                    logging.debug("In GRB grace period, skipping target selection")
                    time.sleep(self.update_interval)
                    continue

                # Get next target to execute
                target = self._select_next_target()

                if target:
                    self._execute_target(target)
                else:
                    logging.debug("No target selected")

                time.sleep(self.update_interval)

            except Exception as e:
                logging.error(f"Error in selector loop: {e}")
                time.sleep(self.update_interval)

        logging.info("Selector loop stopped")

    def _select_next_target(self) -> Optional[ScheduledTarget]:
        """Select next target based on queue priority and timing."""

        # 1. Check for calibrations first (flats during twilight, darks during day)
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
        period_state = self.system_state & 0xF0
        period = "dusk" if period_state == 0x10 else "dawn"

        logging.debug(f"System state indicates {period} - selecting calibrations")

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
            return cursor.fetchall()

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
                self.next_target.value = target.tar_name
                self.current_queue.value = "scheduler"  # or determine from queue_id

                # Remove executed target from queue (if needed)
                self._remove_executed_target(target)

        except Exception as e:
            logging.error(f"Error executing target {target.tar_id}: {e}")

    def _send_executor_command(self, command: str) -> bool:
        """Send command to executor device."""
        try:
            # Use RTS2 network to send command
            response = self.network.send_command(self.executor_name, command)
            logging.debug(f"Executor response: {response}")
            return True
        except Exception as e:
            logging.error(f"Error sending command '{command}' to executor: {e}")
            return False

    def _remove_executed_target(self, target: ScheduledTarget):
        """Remove executed target from database queue."""
        try:
            if not self.db_conn or self.db_conn.closed:
                self.db_conn = psycopg2.connect(**self.db_config)

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
