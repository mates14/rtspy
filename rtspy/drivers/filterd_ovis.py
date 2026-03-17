#!/usr/bin/env python3
"""
Complete OVIS Multi-function Device

Driver for the OVIS (Otevřená Věda Imaging Spectrograph) low budget
spectrograph Copyright (C) 2023-2025 Martin Jelínek

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program.  If not, see <https://www.gnu.org/licenses/>.

Project Background:
------------------

"Otevřená Věda" (literally "Open Science") is a program of the Academy of
Sciences of the Czech Republic that offers high-school students opportunities
to participate in research at the institutes of the Academy. The program runs
for one year, with up to three students collaborating on a single project.

This spectrograph was developed across two consecutive years of the Otevřená
Věda program:

1. First year (2023): Students Adam Denko, Jan Sova, and Veronika Modrá
    designed and built the imaging spectrograph, winning second place in the
    national conference with their project.

2. Second year (2024): Students Adam Denko, Filip Bobal, and Barbora Nohová
    continued with the development in a follow-up project "Observing with an
    imaging spectrograph".

The spectrograph initially used an Arduino with a simple bridge control. In
2024, Patrik Novák from FEL-CTU (Czech Technical University) designed and built
the Raspberry Pi Pico-based driver board used in the current implementation.

Martin Jelínek from the Astronomical Institute of the Academy of Sciences,
along with Jan Strobl, mentored these projects and implemented this Python RTS2
driver to enable the spectrograph's integration with the RTS2 observatory
control system.

OVIS Driver Implementation:
---------------------------

This driver controls the OVIS (Otevřená Věda Imaging Spectrograph) hardware
through a Raspberry Pi Pico-based controller board. The controller provides
stepper motor drivers (type commonly used for 3D printers) for positioning the
filter wheel and other moving components.

Hardware capabilities:

- 3 motor control channels (though only 2 are populated with TMC2209 stepper
  motor drivers)
- TMC2209 drivers commonly used in 3D printers, featuring StallGuard capability
- Neon calibration lamp control (on/off)
- Relay control for introducing the neon lamp into the optical path

The driver communicates with the controller via serial port, implementing the
protocol defined at: https://github.com/Pato-99/spectral_firmware_rp

Current implementation:

- Motor 1: Primary filter wheel control with position mapping
- Motor 0: Focusing movement (currently disabled, planned for implementation
  with a future Focusd template class)
- Neon lamp and optical path relay controls fully implemented

The primary functionality is to manage the filter wheel positions, with the
ability to move to absolute positions, home the motors, and track current
position. It also exposes controls for the calibration lamp.

This device combines both filter wheel and focuser functionality using the
OVIS hardware with two stepper motors:
- Motor 0: Focuser
- Motor 1: Filter wheel

"""

import time
import logging
import threading
import serial
import math
from typing import Dict, Any

from rtspy.core.device import Device
from rtspy.core.constants import DeviceType
from rtspy.core.value import (
    ValueSelection, ValueInteger, ValueBool, ValueString, ValueTime, ValueDouble
)
from rtspy.core.focusd import FocuserMixin
from rtspy.core.filterd import FilterMixin
from rtspy.core.app import App


class SerialCommunicator:
    """Serial device communicator for OVIS hardware."""

    def __init__(self, device_file: str, baudrate: int = 9600):
        self.device_file = device_file
        self.baudrate = baudrate
        self.serial_conn = None
        self.connected = False
        self.status_callback = None

        # Threading
        self.running = False
        self.thread = None
        self.lock = threading.RLock()

    def start(self):
        """Start the communicator thread."""
        if self.thread and self.thread.is_alive():
            return True

        self.running = True
        self.thread = threading.Thread(
            target=self._status_loop,
            name="SerialStatusLoop",
            daemon=True
        )
        self.thread.start()
        return True

    def stop(self):
        """Stop the communicator."""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2.0)
        self._close()

    def _connect(self):
        """Connect to the serial device."""
        try:
            logging.info(f"Opening serial connection to {self.device_file}")
            self.serial_conn = serial.Serial(
                port=self.device_file,
                baudrate=self.baudrate,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=5.0
            )

            # Clear buffers
            self.serial_conn.reset_input_buffer()
            self.serial_conn.reset_output_buffer()

            time.sleep(0.5)  # Wait for device to initialize

            # Check device ID
            response = self.send_command("ID", True)
            if response != "RPI_PICO_SPECTRAL_AVCR":
                logging.warning(f"Unknown device ID: {response}")

            self.connected = True
            return True

        except Exception as e:
            logging.error(f"Error connecting to serial device: {e}")
            self._close()
            return False

    def _close(self):
        """Close the serial connection."""
        if self.serial_conn:
            try:
                self.serial_conn.close()
            except:
                pass
            self.serial_conn = None
        self.connected = False

    def send_command(self, cmd: str, expect_response: bool = False, timeout: float = 5.0):
        """Send a command and optionally wait for response."""
        if not self.serial_conn:
            self._connect()

        if not self.connected:
            return None

        # Add newline if needed
        if not cmd.endswith('\n'):
            cmd += '\n'

        try:
            with self.lock:
                self.serial_conn.write(cmd.encode())
                self.serial_conn.flush()

                if expect_response:
                    orig_timeout = self.serial_conn.timeout
                    self.serial_conn.timeout = timeout
                    try:
                        response = self.serial_conn.readline().decode().strip()
                        return response
                    finally:
                        self.serial_conn.timeout = orig_timeout
                return "OK"
        except Exception as e:
            logging.error(f"Error sending command: {e}")
            self.connected = False
            return None

    def _status_loop(self):
        """Background thread loop to poll device status."""
        logging.info("Status monitoring thread started")
        connect_retry_time = 0

        while self.running:
            try:
                # Check if we need to connect
                if not self.connected:
                    current_time = time.time()
                    if current_time - connect_retry_time >= 5.0:
                        self._connect()
                        connect_retry_time = current_time
                    time.sleep(0.1)
                    continue

                # Get status update
                self._update_status()

                # Wait before next status check
                time.sleep(0.25)  # 4Hz status updates

            except Exception as e:
                logging.error(f"Error in status loop: {e}")
                time.sleep(0.5)

    def _update_status(self):
        """Poll device status and pass to callback."""
        if not self.connected or not self.status_callback:
            return

        try:
            response = self.send_command("STATUS", True)
            if not response:
                return

            # Parse motor statuses
            motor_statuses = []

            # Parse motor 0 status (focuser - first line)
            motor_info = response.split()
            if len(motor_info) >= 5 and motor_info[0] == "M" and motor_info[1] == "0":
                motor_statuses.append({
                    'motor': 0,
                    'position': int(motor_info[2]),
                    'is_moving': int(motor_info[3]),
                    'speed': int(float(motor_info[4])),
                    'acceleration': int(float(motor_info[5])) if len(motor_info) > 5 else 0
                })

            # Read motor 1 status (filter wheel - second line)
            response = self.serial_conn.readline().decode().strip()
            motor_info = response.split()
            if len(motor_info) >= 5 and motor_info[0] == "M" and motor_info[1] == "1":
                motor_statuses.append({
                    'motor': 1,
                    'position': int(motor_info[2]),
                    'is_moving': int(motor_info[3]),
                    'speed': int(float(motor_info[4])),
                    'acceleration': int(float(motor_info[5])) if len(motor_info) > 5 else 0
                })

            # Skip motor 2, shutter status
            self.serial_conn.readline()  # motor 2
            self.serial_conn.readline()  # shutter

            # Read neon status
            response = self.serial_conn.readline().decode().strip()
            neon_info = response.split()
            neon_status = None
            if len(neon_info) >= 2 and neon_info[0] == "R":
                neon_status = int(neon_info[1])

            # Call status callback
            self.status_callback(motor_statuses, neon_status)

        except Exception as e:
            logging.error(f"Error updating status: {e}")
            self.connected = False

    def set_status_callback(self, callback):
        """Set callback for status updates."""
        self.status_callback = callback

    def is_connected(self):
        """Check if connected to device."""
        return self.connected


class OvisMultiFunction(Device, FilterMixin, FocuserMixin):
    """
    OVIS Multi-function Device - combines filter wheel and focuser.

    This device uses the proven filter wheel implementation from filterd_ovis.py
    and adds focuser functionality using FocuserMixin.

    Hardware:
    - Motor 0: Focuser
    - Motor 1: Filter wheel
    - Neon calibration lamp with relay control
    """

    def setup_config(self, config):
        """Set up configuration for both filter and focuser."""
        #super().setup_config(config)
        self.setup_filter_config(config)
        self.setup_focuser_config(config)

        # OVIS-specific hardware options
        config.add_argument('-f', '--device-file',
                          help='Serial port device file (e.g., /dev/ttyUSB0)', section='hardware')
        config.add_argument('--baudrate', type=int, default=9600,
                          help='Serial port baud rate', section='hardware')

        # Filter positions (hardware supports positions 0-5)
        config.add_argument('--f0-pos', type=int, default=2000,
                          help='Filter 0 motor position', section='filters')
        config.add_argument('--f1-pos', type=int, default=54500,
                          help='Filter 1 motor position', section='filters')
        config.add_argument('--f2-pos', type=int, default=107000,
                          help='Filter 2 motor position', section='filters')
        config.add_argument('--f3-pos', type=int, default=159500,
                          help='Filter 3 motor position', section='filters')
        config.add_argument('--f4-pos', type=int, default=212000,
                          help='Filter 4 motor position', section='filters')
        config.add_argument('--f5-pos', type=int, default=292000,
                          help='Filter 5 motor position (grism)', section='filters')

        # Motor control options
        config.add_argument('--motor-speed', type=int, default=100000,
                          help='Motor speed setting', section='hardware')
        config.add_argument('--motor-acceleration', type=int, default=100000,
                          help='Motor acceleration setting', section='hardware')
        config.add_argument('--home-timeout', type=float, default=30.0,
                          help='Homing operation timeout in seconds', section='hardware')

    def __init__(self, device_name="OVIS", port=0):
        """Initialize OVIS multi-function device."""
        # Initialize as a filter wheel device type (we could also use FOCUS or custom)
        super().__init__(device_name, DeviceType.FW, port)

        # Hardware configuration (will be set by apply_config)
        self.device_file = None
        self.baudrate = 9600
        self.motor_speed = 100000
        self.motor_acceleration = 100000
        self.home_timeout = 30.0

        # Serial connection
        self.serial_comm = None

        # Motor positions and state
        self.motor_status_lock = threading.RLock()

        # Filter wheel state (from filterd_ovis.py)
        self.filter_num = 0
        self.filter_moving = False
        self.motor_initialized = False

        # OVIS-specific values (from filterd_ovis.py)
        self.m0pos = ValueInteger("M0POS", "[int] focuser motor position", write_to_fits=True)
        self.m1pos = ValueInteger("M1POS", "[int] filter motor position", write_to_fits=True)

        self.neon = ValueSelection("NEON", "[on/off] neon lamp status",
                                 write_to_fits=True, writable=True)
        self.neon.add_sel_val("off")
        self.neon.add_sel_val("on")

        # Filter position values (hardware supports positions 0-5)
        self.f0pos = ValueInteger("F0POS", "[int] filter 0 position", write_to_fits=False, writable=True, initial=2000)
        self.f1pos = ValueInteger("F1POS", "[int] filter 1 position", write_to_fits=False, writable=True, initial=54500)
        self.f2pos = ValueInteger("F2POS", "[int] filter 2 position", write_to_fits=False, writable=True, initial=107000)
        self.f3pos = ValueInteger("F3POS", "[int] filter 3 position", write_to_fits=False, writable=True, initial=159500)
        self.f4pos = ValueInteger("F4POS", "[int] filter 4 position", write_to_fits=False, writable=True, initial=212000)
        self.f5pos = ValueInteger("F5POS", "[int] filter 5 position (grism)", write_to_fits=False, writable=True, initial=292000)

        # Set device types for mixins
        self.focuser_type = "OVIS_FOCUSER"

        # Initialize filter names (matching filterd_ovis.py)
        filter_names = "J:H:K:R:G:B"  # Default filter names, will be overridden by config

        # Initialize not ready until motors are initialized
        self.set_state(self.STATE_IDLE | self.NOT_READY, "Initializing hardware")

    def apply_config(self, config: Dict[str, Any]):
        """Apply configuration to both mixins and OVIS-specific settings."""
        super().apply_config(config)
        self.apply_filter_config(config)
        self.apply_focuser_config(config)

        # Apply OVIS-specific config
        self.device_file = config.get('device_file')
        self.baudrate = config.get('baudrate', 9600)
        self.motor_speed = config.get('motor_speed', 100000)
        self.motor_acceleration = config.get('motor_acceleration', 100000)
        self.home_timeout = config.get('home_timeout', 30.0)

        # Apply filter positions (hardware supports positions 0-5)
        self.f0pos.value = config.get('f0_pos', 2000)
        self.f1pos.value = config.get('f1_pos', 54500)
        self.f2pos.value = config.get('f2_pos', 107000)
        self.f3pos.value = config.get('f3_pos', 159500)
        self.f4pos.value = config.get('f4_pos', 212000)
        self.f5pos.value = config.get('f5_pos', 292000)

        # Set focuser type value
        if hasattr(self, 'foc_type'):
            self.foc_type.value = self.focuser_type

        logging.info(f"OVIS Multi-function configuration applied:")
        logging.info(f"  Device file: {self.device_file}")
        logging.info(f"  Baud rate: {self.baudrate}")
        logging.info(f"  Motor speed: {self.motor_speed}")
        logging.info(f"  Motor acceleration: {self.motor_acceleration}")
        logging.info(f"  Home timeout: {self.home_timeout}s")

    def _register_device_commands(self):
        """Register command handlers for both filter and focuser."""
        # Register base device commands
        super()._register_device_commands()

        # Register filter commands
        self._register_filter_commands()

        # Register focuser commands
        self._register_focuser_commands()

    def start(self):
        """Start the OVIS device."""
        super().start()

        if not self.device_file:
            logging.error("No device file specified. Use --device-file option.")
            self.set_state(self.STATE_IDLE | self.ERROR_HW, "No device file specified")
            return

        try:
            # Create serial communicator
            self.serial_comm = SerialCommunicator(self.device_file, self.baudrate)
            self.serial_comm.set_status_callback(self._handle_status_update)
            self.serial_comm.start()

            # Initialize the hardware (from filterd_ovis.py)
            logging.info("Initializing OVIS multi-function device")

            # Power on both motors
            if not self.serial_comm.send_command("M 0 ON"):  # Focuser
                logging.error("Failed to power on focuser motor")
                self.set_state(self.STATE_IDLE | self.ERROR_HW, "Failed to power on focuser motor")
                return

            if not self.serial_comm.send_command("M 1 ON"):  # Filter wheel
                logging.error("Failed to power on filter wheel motor")
                self.set_state(self.STATE_IDLE | self.ERROR_HW, "Failed to power on filter wheel motor")
                return

            # Set configured speed/acceleration for both motors
            self.serial_comm.send_command(f"M 0 SPD {self.motor_speed}")
            self.serial_comm.send_command(f"M 0 ACC {self.motor_acceleration}")
            self.serial_comm.send_command(f"M 1 SPD {self.motor_speed}")
            self.serial_comm.send_command(f"M 1 ACC {self.motor_acceleration}")

            # Home the filter wheel (from filterd_ovis.py)
            logging.info("Homing filter wheel")
            self.set_state(self._state | self.FILTERD_MOVE, "Homing filter wheel", self.BOP_EXPOSURE)

            response = self.serial_comm.send_command("M 1 HOM", True, self.home_timeout)
            if not response or "OK" not in response:
                logging.error("Failed to home filter wheel")
                self.set_state(self.STATE_IDLE | self.ERROR_HW, "Failed to home filter wheel")
                return

            # Homing successful
            logging.info("Filter wheel homed successfully")
            self.filter_num = 0
            self.motor_initialized = True

            # Move filter to position 0
            self.set_filter_num(0)

            # Initialize focuser position
            self.foc_pos.value = 0.0
            self.foc_tar.value = 0.0
            self.foc_def.value = 0.0

            # Mark device as ready
            self.set_state(self.STATE_IDLE, "OVIS multi-function device ready", 0)
            self.set_ready("Multi-function device initialized and ready")

        except Exception as e:
            logging.error(f"Error initializing OVIS device: {e}")
            self.set_state(self.STATE_IDLE | self.ERROR_HW, f"Initialization error: {e}")

    def stop(self):
        """Stop the OVIS device."""
        if self.serial_comm:
            try:
                # Turn off both motors
                self.serial_comm.send_command("M 0 OFF")
                self.serial_comm.send_command("M 1 OFF")
            except:
                pass
            self.serial_comm.stop()
            self.serial_comm = None
        super().stop()

    def _handle_status_update(self, motor_statuses, neon_status):
        """Handle status updates from hardware."""
        with self.motor_status_lock:
            # Process each motor status
            for motor in motor_statuses:
                if motor['motor'] == 0:  # Focuser
                    if not self.m0pos.value == motor['position']:
                        self.m0pos.value = motor['position']
                        self.foc_pos.value = float(motor['position'])

                    # Check for focuser movement completion
                    if (hasattr(self, '_movement_in_progress') and self._movement_in_progress and
                        not motor['is_moving']):
                        self._handle_focuser_movement_complete()

                elif motor['motor'] == 1:  # Filter wheel
                    if not self.m1pos.value == motor['position']:
                        self.m1pos.value = motor['position']

                    # Check for filter movement completion
                    if self.filter_moving and not motor['is_moving']:
                       self._handle_filter_movement_complete()

            # Update neon status
            if neon_status is not None:
                if not self.neon.value == neon_status:
                    self.neon.value = neon_status

    def _handle_filter_movement_complete(self):
        """Handle filter movement completion (hardware supports positions 0-5)."""
        logging.info(f"Filter movement completed at position {self.m1pos.value}")

        # Find which filter position we're closest to
        target_positions = [
            self.f0pos.value, self.f1pos.value, self.f2pos.value,
            self.f3pos.value, self.f4pos.value, self.f5pos.value
        ]

        closest_filter = 0
        closest_distance = abs(self.m1pos.value - target_positions[0])

        for i in range(1, len(target_positions)):
            distance = abs(self.m1pos.value - target_positions[i])
            if distance < closest_distance:
                closest_distance = distance
                closest_filter = i

        # Update filter position
        self.filter_num = closest_filter
        self.filter.value = closest_filter

        # Reset device state
        self.filter_moving = False
        self.movement_completed()

    def _handle_focuser_movement_complete(self):
        """Handle focuser movement completion."""
        logging.info(f"Focuser movement completed at position {self.m0pos.value}")

        # Call the focuser mixin's end_focusing method
        self.end_focusing()

    # ========== FilterMixin Implementation ==========

    def get_filter_num(self) -> int:
        """Get current filter number (from filterd_ovis.py)."""
        return self.filter_num

    def set_filter_num(self, new_filter):
        """Set filter wheel position (hardware supports positions 0-5)."""
        # Validate filter number
        if new_filter < 0 or new_filter >= 6:
            logging.error(f"Invalid filter number: {new_filter}")
            return -1

        # Check if already at this position
        if new_filter == self.filter_num:
            logging.info(f"Filter already at position {new_filter}")
            return 0

        # Check if ready
        if not self.serial_comm or not self.motor_initialized:
            logging.error("Cannot move filter: device not initialized")
            return -1

        # Get target position
        target_positions = [
            self.f0pos.value, self.f1pos.value, self.f2pos.value,
            self.f3pos.value, self.f4pos.value, self.f5pos.value
        ]

        target_position = target_positions[new_filter]
        logging.info(f"Moving filter to position {new_filter}, motor position {target_position}")

        # Update device state to show movement
        self.set_state(
            self._state | self.FILTERD_MOVE,
            f"Moving to filter {new_filter}",
            self.BOP_EXPOSURE
        )

        # Mark as moving
        with self.motor_status_lock:
            self.filter_moving = True

        # Send movement command
        cmd = f"M 1 ABS {target_position}"
        self.serial_comm.send_command(cmd)

        # Movement completion will be detected by status updates
        return 0

    def home_filter(self):
        """Home the filter wheel (from filterd_ovis.py)."""
        if not self.serial_comm:
            logging.error("Cannot home filter: device not connected")
            return -1

        logging.info("Homing filter wheel")

        # Set device state to show movement
        self.set_state(
            self._state | self.FILTERD_MOVE,
            "Homing filter wheel",
            self.BOP_EXPOSURE
        )

        # Send home command with configured timeout
        response = self.serial_comm.send_command("M 1 HOM", True, self.home_timeout)

        if not response or "OK" not in response:
            logging.error("Failed to home filter wheel")
            self.set_state(self._state & ~(self.FILTERD_MOVE), "Homing failed", 0)
            return -1

        # Homing successful
        logging.info("Filter wheel homed successfully")

        # Update filter position
        self.filter_num = 0
        self.filter.value = 0

        # Reset state
        self.set_state(self._state & ~(self.FILTERD_MOVE), "Filter wheel homed", 0)
        return 0

    # ========== FocuserMixin Implementation ==========

    def set_to(self, position: float) -> int:
        """Move focuser to target position."""
        try:
            if not self.serial_comm:
                logging.error("Cannot move focuser: device not connected")
                return -1

            # Check if already at target position (within tolerance)
            current_pos = self.get_position()
            position_tolerance = 10.0  # Motor steps tolerance
            if abs(current_pos - position) <= position_tolerance:
                logging.info(f"Focuser already at target position {position} (current: {current_pos})")
                # Still need to mark movement as complete for state management
                self._handle_focuser_movement_complete()
                return 0

            # Send movement command to motor 0 (focuser)
            cmd = f"M 0 ABS {int(position)}"
            self.serial_comm.send_command(cmd)
            logging.info(f"Moving focuser to position {position}")
            return 0
        except Exception as e:
            logging.error(f"Error moving focuser: {e}")
            return -1

    def is_at_start_position(self) -> bool:
        """Check if focuser is at start position."""
        return abs(self.get_position()) < 10.0

    def home_focuser(self) -> int:
        """Home the focuser."""
        if not self.serial_comm:
            logging.error("Cannot home focuser: device not connected")
            return -1

        logging.info("Homing focuser")

        # Set device state to show movement
        self.set_state(
            self._state | self.FOC_FOCUSING,
            "Homing focuser",
            self.BOP_EXPOSURE
        )

        # Send home command
        response = self.serial_comm.send_command("M 0 HOM", True, self.home_timeout)

        if not response or "OK" not in response:
            logging.error("Failed to home focuser")
            self.set_state(self._state & ~(self.FOC_FOCUSING), "Focuser homing failed", 0)
            return -1

        # Homing successful
        logging.info("Focuser homed successfully")

        # Update focuser position
        self.foc_pos.value = 0.0
        self.foc_tar.value = 0.0

        # Reset state
        self.set_state(self._state & ~(self.FOC_FOCUSING), "Focuser homed", 0)
        return 0

    # ========== Device Methods ==========

    def info(self):
        """Update device information."""
        super().info()
        # Motor positions are updated via status callback

        # Update filter and focuser info
        self.filter_info_update()
        self.focuser_info_update()

    def idle(self):
        """Combined idle processing for both functions."""
        # Call parent idle
        result = super().idle()

        # Call mixin idle methods
        filter_result = self.filter_idle()
        focuser_result = self.focuser_idle()

        # Return most restrictive timing
        results = [r for r in [result, filter_result, focuser_result] if r is not None]
        return min(results) if results else None

    def on_value_changed_from_client(self, value, old_value, new_value):
        """Handle value changes for both functions."""
        try:
            # Handle neon lamp (from filterd_ovis.py)
            if value.name == "NEON":
                self._set_neon(new_value)
                return 0
            if value.name.startswith("F") and value.name.endswith("POS"):
                filter_idx = int(value.name[1])
                if filter_idx == self.filter.value:
                    # Update current filter position
                    self.serial_comm.send_command(f"M 1 ABS {new_value}")
                return 0
            # Let mixins handle their values
            filter_result = FilterMixin.on_value_changed_from_client(self, value, old_value, new_value)
            focuser_result = FocuserMixin.on_value_changed_from_client(self, value, old_value, new_value)
            return min(focuser_result,filter_result)
        except Exception as e:
            logging.error(f"Error handling value change: {e}")
            return -1

    def _set_neon(self, new_value):
        """Set neon lamp state (from filterd_ovis.py)."""
        if not self.serial_comm:
            return

        if new_value == 0:  # OFF
            self.serial_comm.send_command("S ON")
            self.serial_comm.send_command("S IN")
            self.serial_comm.send_command("R OFF")
        else:  # ON
            self.serial_comm.send_command("S ON")
            self.serial_comm.send_command("S OUT")
            self.serial_comm.send_command("R ON")

        logging.info(f"Neon lamp set to {'ON' if new_value else 'OFF'}")

def main():
    """Main entry point for OVIS multi-function device."""
    # Create application
    app = App(description='OVIS Multi-function Device (Filter + Focuser)')

    # Register device options
    app.register_device_options(OvisMultiFunction)

    # Parse arguments
    args = app.parse_args()

    # Create device
    device = app.create_device(OvisMultiFunction)

    # Show config summary if debug enabled
    if getattr(args, 'debug', False):
        print("\nOVIS Multi-function Configuration:")
        print("=" * 50)
        print(device.get_config_summary())
        print("=" * 50)

    logging.info("Starting OVIS Multi-function Device (Filter + Focuser)")

    # Run application
    try:
        app.run()
        return 0
    except KeyboardInterrupt:
        logging.info("Shutting down OVIS multi-function device")
        return 0
    except Exception as e:
        logging.error(f"Fatal error in OVIS device: {e}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
