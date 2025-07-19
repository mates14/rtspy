#!/usr/bin/env python3

"""
RTS2 Queue Selector - Simplified Implementation

This selector reads from the "scheduler" queue in the database and executes
targets at their scheduled times. It handles calibrations and respects
GRB grace periods.
"""

import time
import logging
import threading
import math
import psycopg2
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

from device import Device
from config import DeviceConfig
from constants import DeviceType, ConnectionState
from value import (ValueBool, ValueString, ValueInteger, ValueTime, ValueDouble,
                  ValueSelection)
from app import App


@dataclass
class ScheduledTarget:
    """Represents a target from the scheduler queue."""
    qid: int
    tar_id: int
    queue_start: datetime
    queue_end: datetime
    tar_name: str
    tar_ra: float
    tar_dec: float


class SunCalculator:
    """Simple sun position calculator for calibration timing."""

    def __init__(self, latitude: float, longitude: float, altitude: float = 0):
        self.latitude = math.radians(latitude)
        self.longitude = math.radians(longitude)
        self.altitude = altitude

    def get_sun_elevation(self, when: Optional[datetime] = None) -> float:
        """Get sun elevation in degrees."""
        if when is None:
            when = datetime.utcnow()

        # Simplified sun position calculation
        # For production use, consider using astropy or pyephem

        # Days since J2000.0
        jd = self._julian_day(when)
        n = jd - 2451545.0

        # Mean longitude
        L = math.radians(280.460 + 0.9856474 * n)

        # Mean anomaly
        g = math.radians(357.528 + 0.9856003 * n)

        # Ecliptic longitude
        lambda_sun = L + math.radians(1.915 * math.sin(g) + 0.020 * math.sin(2 * g))

        # Declination
        declination = math.asin(math.sin(math.radians(23.439)) * math.sin(lambda_sun))

        # Hour angle
        gmst = math.radians(280.46061837 + 360.98564736629 * (jd - 2451545.0))
        lmst = gmst + self.longitude
        ha = lmst - lambda_sun

        # Elevation
        elevation = math.asin(
            math.sin(self.latitude) * math.sin(declination) +
            math.cos(self.latitude) * math.cos(declination) * math.cos(ha)
        )

        return math.degrees(elevation)

    def _julian_day(self, dt: datetime) -> float:
        """Convert datetime to Julian day."""
        a = (14 - dt.month) // 12
        y = dt.year + 4800 - a
        m = dt.month + 12 * a - 3

        jdn = dt.day + (153 * m + 2) // 5 + 365 * y + y // 4 - y // 100 + y // 400 - 32045
        jd = jdn + (dt.hour - 12) / 24.0 + dt.minute / 1440.0 + dt.second / 86400.0

        return jd


class QueueSelector(Device, DeviceConfig):
    """
    Queue-based target selector for RTS2.

    Reads scheduled targets from database and executes them at proper times.
    Handles calibrations and respects GRB grace periods.
    """

    # Target constants
    TARGET_FLAT = 2
    TARGET_DARK = 3

    def setup_config(self, config):
        """Register selector-specific configuration."""

        # Database configuration
        config.add_argument('--db-host', default='localhost',
                          help='Database host', section='database')
        config.add_argument('--db-name', default='stars',
                          help='Database name', section='database')
        config.add_argument('--db-user', default='mates',
                          help='Database user', section='database')
        config.add_argument('--db-password', default='pasewcic25',
                          help='Database password', section='database')

        # Observatory location
        config.add_argument('--latitude', type=float, default=50.0,
                          help='Observatory latitude (degrees)', section='observatory')
        config.add_argument('--longitude', type=float, default=14.0,
                          help='Observatory longitude (degrees)', section='observatory')
        config.add_argument('--altitude', type=float, default=500.0,
                          help='Observatory altitude (meters)', section='observatory')

        # Calibration timing
        config.add_argument('--flat-sun-min', type=float, default=-15.0,
                          help='Minimum sun elevation for flats (degrees)', section='calibration')
        config.add_argument('--flat-sun-max', type=float, default=-8.0,
                          help='Maximum sun elevation for flats (degrees)', section='calibration')

        # Timing parameters
        config.add_argument('--time-slice', type=int, default=300,
                          help='Time slice for scheduling (seconds)', section='timing')
        config.add_argument('--grb-grace-period', type=int, default=1200,
                          help='GRB grace period (seconds)', section='timing')
        config.add_argument('--update-interval', type=float, default=30.0,
                          help='Queue check interval (seconds)', section='timing')

    def __init__(self, device_name="SEL", port=0):
        """Initialize the queue selector."""
        super().__init__(device_name, DeviceType.SELECTOR, port)

        # Configuration values (set by apply_config)
        self.db_config = {}
        self.observatory_config = {}
        self.time_slice = 300
        self.grb_grace_period = 1200
        self.update_interval = 30.0

        # Create RTS2 values for monitoring
        self._create_values()

        # State tracking
        self.last_issued_target = None
        self.grb_grace_end = 0
        self.system_state = 0
        self.system_ready = False

        # Threading
        self.running = False
        self.selector_thread = None

        # Sun calculator (initialized in apply_config)
        self.sun_calc = None

        # Database connection
        self.db_conn = None

    def apply_config(self, config: Dict[str, Any]):
        """Apply selector configuration."""
        super().apply_config(config)

        # Database configuration
        self.db_config = {
            'host': config.get('db_host', 'localhost'),
            'dbname': config.get('db_name', 'stars'),
            'user': config.get('db_user', 'mates'),
            'password': config.get('db_password', 'pasewcic25')
        }

        # Observatory configuration
        self.observatory_config = {
            'latitude': config.get('latitude', 50.0),
            'longitude': config.get('longitude', 14.0),
            'altitude': config.get('altitude', 500.0)
        }

        # Timing configuration
        self.time_slice = config.get('time_slice', 300)
        self.grb_grace_period = config.get('grb_grace_period', 1200)
        self.update_interval = config.get('update_interval', 30.0)

        # Calibration configuration
        self.flat_sun_min.value = config.get('flat_sun_min', -15.0)
        self.flat_sun_max.value = config.get('flat_sun_max', -8.0)

        # Initialize sun calculator
        self.sun_calc = SunCalculator(
            self.observatory_config['latitude'],
            self.observatory_config['longitude'],
            self.observatory_config['altitude']
        )

        logging.info(f"Queue selector configured:")
        logging.info(f"  Database: {self.db_config['dbname']}@{self.db_config['host']}")
        logging.info(f"  Observatory: {self.observatory_config['latitude']:.2f}°, {self.observatory_config['longitude']:.2f}°")
        logging.info(f"  Time slice: {self.time_slice}s")
        logging.info(f"  Flats: {self.flat_sun_min.value}° to {self.flat_sun_max.value}°")

    def _create_values(self):
        """Create RTS2 values for monitoring."""

        # Status values
        self.enabled = ValueBool("enabled", "Selector enabled", writable=True, initial=True)
        self.queue_only = ValueBool("queue_only", "Only select from queue", writable=True, initial=True)

        # Current target information
        self.next_id = ValueInteger("next_id", "Next target ID")
        self.next_time = ValueTime("next_time", "Next target time")
        self.current_target = ValueInteger("current_target", "Current target ID")
        self.current_start = ValueTime("current_start", "Current target start time")
        self.current_end = ValueTime("current_end", "Current target end time")

        # Calibration settings
        self.flat_sun_min = ValueDouble("flat_sun_min", "Minimum sun elevation for flats [deg]",
                                       writable=True, initial=-15.0)
        self.flat_sun_max = ValueDouble("flat_sun_max", "Maximum sun elevation for flats [deg]",
                                       writable=True, initial=-8.0)

        # Statistics
        self.targets_executed = ValueInteger("targets_executed", "Targets executed today", initial=0)
        self.flats_executed = ValueInteger("flats_executed", "Flats executed today", initial=0)
        self.darks_executed = ValueInteger("darks_executed", "Darks executed today", initial=0)

        # System status
        self.sun_elevation = ValueDouble("sun_elevation", "Current sun elevation [deg]")
        self.queue_size = ValueInteger("queue_size", "Scheduler queue size")
        self.last_update = ValueTime("last_update", "Last queue update")

        # GRB grace tracking
        self.grb_grace_active = ValueBool("grb_grace_active", "GRB grace period active")
        self.grb_grace_until = ValueTime("grb_grace_until", "GRB grace period end")

    def start(self):
        """Start the queue selector."""
        super().start()

        # Test database connection
        if not self._test_database():
            self.set_state(self.STATE_IDLE | self.ERROR_HW, "Database connection failed")
            return

        # Register interest in system state from centrald
        self.network.register_state_interest("centrald", self._on_system_state_changed)

        # Start selector thread
        self.running = True
        self.selector_thread = threading.Thread(
            target=self._selector_loop,
            name="QueueSelector",
            daemon=True
        )
        self.selector_thread.start()

        self.set_ready("Queue selector ready")
        logging.info("Queue selector started")

    def stop(self):
        """Stop the queue selector."""
        self.running = False
        if self.selector_thread and self.selector_thread.is_alive():
            self.selector_thread.join(timeout=5.0)

        if self.db_conn:
            self.db_conn.close()

        super().stop()

    def info(self):
        """Update device information."""
        super().info()

        # Update sun elevation
        if self.sun_calc:
            self.sun_elevation.value = self.sun_calc.get_sun_elevation()

        # Update last update time
        self.last_update.value = time.time()

        # Update GRB grace status
        current_time = time.time()
        if current_time < self.grb_grace_end:
            self.grb_grace_active.value = True
            self.grb_grace_until.value = self.grb_grace_end
        else:
            self.grb_grace_active.value = False

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

        # Check if system is ready (ON + NIGHT = 0x03)
        self.system_ready = (state & 0xFF) == 0x03

        if old_state != state:
            logging.info(f"System state: 0x{state:02x} ({'ready' if self.system_ready else 'not ready'})")

    def _selector_loop(self):
        """Main selector loop."""
        logging.info("Selector loop started")

        while self.running:
            try:
                current_time = time.time()

                # Check for GRB grace period
                if current_time < self.grb_grace_end:
                    logging.debug(f"GRB grace period active for {self.grb_grace_end - current_time:.0f}s")
                    time.sleep(min(10.0, self.grb_grace_end - current_time))
                    continue

                # Get current executor target
                executor_target = self._get_executor_target()

                # Check for external intervention (GRB)
                if executor_target and executor_target != self.last_issued_target:
                    logging.info(f"External target detected: {executor_target}, setting grace period")
                    self.grb_grace_end = current_time + self.grb_grace_period
                    continue

                # Determine what to execute
                target_id, target_type = self._select_next_target()

                if target_id:
                    # Execute target with proper timing
                    if self._execute_target(target_id, target_type):
                        self.last_issued_target = target_id

                        # Update statistics
                        if target_type == "flat":
                            self.flats_executed.value += 1
                        elif target_type == "dark":
                            self.darks_executed.value += 1
                        else:
                            self.targets_executed.value += 1

                # Sleep until next check
                time.sleep(min(self.update_interval, 60.0))

            except Exception as e:
                logging.error(f"Error in selector loop: {e}")
                time.sleep(10.0)  # Back off on error

    def _get_executor_target(self) -> Optional[int]:
        """Get current target from executor."""
        try:
            # Find executor connection
            for conn in self.network.connection_manager.connections.values():
                if (hasattr(conn, 'remote_device_type') and
                    conn.remote_device_type == DeviceType.EXECUTOR and
                    conn.state == ConnectionState.AUTH_OK):

                    # Request current target (would need executor value monitoring)
                    # For now, return None - this would need executor integration
                    return None
            return None
        except Exception as e:
            logging.warning(f"Error getting executor target: {e}")
            return None

    def _select_next_target(self) -> Tuple[Optional[int], str]:
        """Select next target to execute."""
        current_time = time.time()

        # Check for calibrations first
        if self._needs_calibrations():
            cal_target = self._select_calibration()
            if cal_target:
                return cal_target, "calibration"

        # If not enabled or queue only mode, only do calibrations
        if not self.enabled.value:
            return None, "disabled"

        # Get next target from scheduler queue
        scheduled_target = self._get_next_scheduled_target()
        if scheduled_target:
            target_time = scheduled_target.queue_start.timestamp()

            # Check if it's time to execute
            if current_time >= target_time:
                return scheduled_target.tar_id, "scheduled"
            elif current_time >= target_time - self.time_slice:
                # Pre-schedule with 'next' command
                return scheduled_target.tar_id, "pre_scheduled"

        return None, "no_target"

    def _needs_calibrations(self) -> bool:
        """Check if calibrations are needed based on sun elevation."""
        if not self.sun_calc:
            return False

        sun_alt = self.sun_calc.get_sun_elevation()

        # Flats during twilight
        if self.flat_sun_min.value <= sun_alt <= self.flat_sun_max.value:
            return True

        # Darks when sun is well above horizon
        if sun_alt > self.flat_sun_max.value:
            return True

        return False

    def _select_calibration(self) -> Optional[int]:
        """Select appropriate calibration target."""
        if not self.sun_calc:
            return None

        sun_alt = self.sun_calc.get_sun_elevation()

        if self.flat_sun_min.value <= sun_alt <= self.flat_sun_max.value:
            logging.info(f"Selecting flats (sun elevation: {sun_alt:.1f}°)")
            return self.TARGET_FLAT
        elif sun_alt > self.flat_sun_max.value:
            logging.info(f"Selecting darks (sun elevation: {sun_alt:.1f}°)")
            return self.TARGET_DARK

        return None

    def _get_next_scheduled_target(self) -> Optional[ScheduledTarget]:
        """Get next target from scheduler queue."""
        try:
            if not self.db_conn or self.db_conn.closed:
                self.db_conn = psycopg2.connect(**self.db_config)

            cursor = self.db_conn.cursor()

            # Query scheduler queue
            query = """
                SELECT q.qid, q.tar_id, q.queue_start, q.queue_end,
                       t.tar_name, t.tar_ra, t.tar_dec
                FROM queue q
                JOIN targets t ON q.tar_id = t.tar_id
                WHERE q.queue_name = 'scheduler'
                  AND q.queue_start > now() - interval '1 hour'
                  AND q.queue_start < now() + interval '24 hours'
                ORDER BY q.queue_start
                LIMIT 1
            """

            cursor.execute(query)
            row = cursor.fetchone()

            if row:
                qid, tar_id, queue_start, queue_end, tar_name, tar_ra, tar_dec = row

                # Update queue size
                cursor.execute("""
                    SELECT COUNT(*) FROM queue
                    WHERE queue_name = 'scheduler'
                      AND queue_start > now()
                """)
                count = cursor.fetchone()[0]
                self.queue_size.value = count

                return ScheduledTarget(
                    qid=qid,
                    tar_id=tar_id,
                    queue_start=queue_start,
                    queue_end=queue_end,
                    tar_name=tar_name,
                    tar_ra=tar_ra or 0.0,
                    tar_dec=tar_dec or 0.0
                )
            else:
                self.queue_size.value = 0
                return None

        except Exception as e:
            logging.error(f"Error querying scheduler queue: {e}")
            return None

    def _execute_target(self, target_id: int, target_type: str) -> bool:
        """Execute target with appropriate timing."""
        try:
            # Find executor connection
            executor_conn = None
            for conn in self.network.connection_manager.connections.values():
                if (hasattr(conn, 'remote_device_type') and
                    conn.remote_device_type == DeviceType.EXECUTOR and
                    conn.state == ConnectionState.AUTH_OK):
                    executor_conn = conn
                    break

            if not executor_conn:
                logging.warning("No executor connection available")
                return False

            # Check if executor is observing
            is_observing = self._executor_is_observing(executor_conn)

            # Determine command based on state and timing
            if target_type in ["flat", "dark"]:
                # Calibrations execute immediately
                cmd = f"now {target_id}"
                logging.info(f"Executing {target_type}: {cmd}")
            elif target_type == "pre_scheduled" and is_observing:
                # Pre-schedule with 'next' command
                cmd = f"next {target_id}"
                logging.info(f"Pre-scheduling target: {cmd}")
            elif target_type == "scheduled":
                if is_observing:
                    cmd = f"next {target_id}"
                    logging.info(f"Scheduling target: {cmd}")
                else:
                    cmd = f"now {target_id}"
                    logging.info(f"Executing target immediately: {cmd}")
            else:
                return False

            # Send command to executor
            success = executor_conn.send_command(cmd, self._on_execute_result)

            if success:
                # Update tracking values
                self.next_id.value = target_id
                self.next_time.value = time.time()

                if target_type not in ["pre_scheduled"]:
                    self.current_target.value = target_id
                    self.current_start.value = time.time()

            return success

        except Exception as e:
            logging.error(f"Error executing target {target_id}: {e}")
            return False

    def _executor_is_observing(self, executor_conn) -> bool:
        """Check if executor is currently observing."""
        # This is simplified - in real implementation you'd check executor state
        # For now, assume not observing
        return False

    def _on_execute_result(self, conn, success, code, message):
        """Handle result from executor command."""
        if success:
            logging.info(f"Executor command successful: {message}")
        else:
            logging.warning(f"Executor command failed: {message}")

    # Value change handlers
    def on_enabled_changed(self, old_value, new_value):
        """Handle enabled state change."""
        logging.info(f"Selector {'enabled' if new_value else 'disabled'}")

    def on_flat_sun_min_changed(self, old_value, new_value):
        """Handle flat sun minimum change."""
        logging.info(f"Flat sun minimum changed to {new_value}°")

    def on_flat_sun_max_changed(self, old_value, new_value):
        """Handle flat sun maximum change."""
        logging.info(f"Flat sun maximum changed to {new_value}°")


def main():
    """Main entry point for queue selector."""
    # Create application
    app = App(description='RTS2 Queue Selector')

    # Register device options
    app.register_device_options(QueueSelector)

    # Parse arguments
    args = app.parse_args()

    # Create device
    device = app.create_device(QueueSelector)

    # Show config if debug enabled
    if getattr(args, 'debug', False):
        print("\nQueue Selector Configuration:")
        print("=" * 50)
        print(device.get_config_summary())
        print("=" * 50)

    logging.info("Starting RTS2 Queue Selector")

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
