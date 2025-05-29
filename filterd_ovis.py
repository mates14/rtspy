!/usr/bin/python3
"""
RTS2-Python OVIS Filter Wheel Driver

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

"""

import time
import logging
import threading
import serial

from value import (
    ValueSelection, ValueInteger, ValueBool, ValueString, ValueTime
)
from filterd import Filterd
from constants import DeviceType
from app import App

class SerialCommunicator:
    """Simple serial device communicator."""

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
#                logging.debug(f"Sending command: {cmd.strip()}")
                self.serial_conn.write(cmd.encode())
                self.serial_conn.flush()

                if expect_response:
                    orig_timeout = self.serial_conn.timeout
                    self.serial_conn.timeout = timeout
                    try:
                        response = self.serial_conn.readline().decode().strip()
#                        logging.debug(f"Received response: {response}")
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

            # Parse motor 0 status (first line)
            motor_info = response.split()
            if len(motor_info) >= 5 and motor_info[0] == "M" and motor_info[1] == "0":
                motor_statuses.append({
                    'motor': 0,
                    'position': int(motor_info[2]),
                    'is_moving': int(motor_info[3]),
                    'speed': int(float(motor_info[4])),
                    'acceleration': int(float(motor_info[5])) if len(motor_info) > 5 else 0
                })

            # Read motor 1 status (second line)
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


class Ovis(Filterd):
    """OVIS (Otevrena Veda Imaging Spectrograph) filter wheel driver."""

    @classmethod
    def register_options(cls, parser):
        """Register command line options."""
        super().register_options(parser)
        parser.add_argument('-f', '--device-file',
                          help='Serial port device file')

    @classmethod
    def process_args(cls, device, args):
        """Process command line arguments."""
        super().process_args(device, args)
        if hasattr(args, 'device_file') and args.device_file:
            device.device_file = args.device_file

    def __init__(self, device_name="W0", port=0):
        """Initialize the Ovis filter wheel device."""
        super().__init__(device_name, port)

        # Serial connection
        self.device_file = None
        self.serial_comm = None

        # Filter and motor state
        self.filter_num = 0
        self.filter_moving = False
        self.motor_initialized = False
        self.motor_position = 0
        self.motor_status_lock = threading.RLock()

        # Create device values
        self.m1pos = ValueInteger("M1POS", "[int] motor position", write_to_fits=True)
        self.focpos = ValueInteger("FOCPOS", "[int] focuser position", write_to_fits=True)
        self.focpos.set_writable()

        self.neon = ValueSelection("NEON", "[on/off] neon lamp status", write_to_fits=True)
        self.neon.set_writable()
        self.neon.add_sel_val("off")
        self.neon.add_sel_val("on")

        # Filter positions
        self.f0pos = ValueInteger("F0POS", "[int] filter 0 position", write_to_fits=True)
        self.f1pos = ValueInteger("F1POS", "[int] filter 1 position", write_to_fits=True)
        self.f2pos = ValueInteger("F2POS", "[int] filter 2 position", write_to_fits=True)
        self.f3pos = ValueInteger("F3POS", "[int] filter 3 position", write_to_fits=True)
        self.f4pos = ValueInteger("F4POS", "[int] filter 4 position", write_to_fits=True)
        self.f5pos = ValueInteger("F5POS", "[int] filter 5 position", write_to_fits=True)
        self.f6pos = ValueInteger("F6POS", "[int] filter 6 position", write_to_fits=True)
        self.f7pos = ValueInteger("F7POS", "[int] filter 7 position", write_to_fits=True)

        # Make filter positions writable
        self.f0pos.set_writable()
        self.f1pos.set_writable()
        self.f2pos.set_writable()
        self.f3pos.set_writable()
        self.f4pos.set_writable()
        self.f5pos.set_writable()
        self.f6pos.set_writable()
        self.f7pos.set_writable()

        # Default filter positions
        self.f0pos.value = 2000
        self.f1pos.value = 54500
        self.f2pos.value = 107000
        self.f3pos.value = 159500
        self.f4pos.value = 212000
        self.f5pos.value = 292000  # grism
        self.f6pos.value = 300000  # unused
        self.f7pos.value = 300000  # limit

        # Initialize not ready until motors are initialized
        self.set_state(self.STATE_IDLE | self.NOT_READY, "Initializing hardware")

    def start(self):
        """Start the device."""
        super().start()

        if not self.device_file:
            logging.error("No device file specified")
            self.set_state(self.STATE_IDLE | self.ERROR_HW, "No device file specified")
            return

        try:
            # Create serial communicator
            self.serial_comm = SerialCommunicator(self.device_file)
            self.serial_comm.set_status_callback(self._handle_status_update)
            self.serial_comm.start()

            # Initialize the device
            logging.info("Initializing Ovis filter wheel")

            # Sequence 1: Turn on motor and home
            if not self.serial_comm.send_command("M 1 ON"):
                logging.error("Failed to power on filter wheel motor")
                self.set_state(self.STATE_IDLE | self.ERROR_HW, "Failed to power on motor")
                return

            # Set default speed/accel
            self.serial_comm.send_command("M 1 SPD 100000")
            self.serial_comm.send_command("M 1 ACC 100000")

            # Home the filter wheel
            logging.info("Homing filter wheel")

            # Sequence 2: Home the motor
            self.set_state(self._state | self.FILTERD_MOVE, "Homing filter wheel",
                            self.BOP_EXPOSURE)

            # Send home command with long timeout
            response = self.serial_comm.send_command("M 1 HOM", True, 30.0)

            if not response or "OK" not in response:
                logging.error("Failed to home filter wheel")
                self.set_state(self.STATE_IDLE | self.ERROR_HW, "Failed to home filter wheel")
                return

            # Homing successful
            logging.info("Filter wheel homed successfully")
            self.filter_num = 0
            self.motor_initialized = True

            # Sequence 3: Move to filter 1
            self.set_filter_num(0)

            # Mark device as ready
            self.set_state(self.FILTERD_IDLE, "Filter wheel idle", 0)
            self.set_ready("Filter wheel initialized and ready")

        except Exception as e:
            logging.error(f"Error initializing device: {e}")
            self.set_state(self.STATE_IDLE | self.ERROR_HW, f"Initialization error: {e}")

    def stop(self):
        """Stop the device."""
        if self.serial_comm:
            try:
                # Turn off the motors
                self.serial_comm.send_command("M 0 OFF")
                self.serial_comm.send_command("M 1 OFF")
            except:
                pass

            self.serial_comm.stop()
            self.serial_comm = None

        super().stop()

    def _handle_status_update(self, motor_statuses, neon_status):
        """Handle status updates from the device."""
        # Update filter wheel motor status
        m1_status = next((m for m in motor_statuses if m['motor'] == 1), None)
        if m1_status:
            # Get position and moving state
            current_pos = m1_status['position']
            is_moving = bool(m1_status['is_moving'])

            with self.motor_status_lock:
                # Store values
                old_moving = self.filter_moving and self.motor_position != current_pos
                self.motor_position = current_pos
                self.m1pos.value = current_pos

                # Check if movement completed
                if old_moving and not is_moving:
                    self._handle_movement_complete()

        # Update neon status
        if neon_status is not None:
            self.neon.value = neon_status

    def _handle_movement_complete(self):
        """Handle filter movement completion."""
        logging.info(f"Filter movement completed at position {self.motor_position}")

        # Find which filter position we're closest to
        target_positions = [
            self.f0pos.value, self.f1pos.value,
            self.f2pos.value, self.f3pos.value,
            self.f4pos.value, self.f5pos.value,
            self.f6pos.value, self.f7pos.value
        ]

        closest_filter = 0
        closest_distance = abs(self.motor_position - target_positions[0])

        for i in range(1, len(target_positions)):
            distance = abs(self.motor_position - target_positions[i])
            if distance < closest_distance:
                closest_distance = distance
                closest_filter = i

        # Update filter position
        self.filter_num = closest_filter
        self.filter.value = closest_filter

        # Reset device state
        self.filter_moving = False
        self.movement_completed()
        self.set_state(self._state & ~(self.FILTERD_MOVE), "Filter wheel idle", 0)

    def get_filter_num(self):
        """Get current filter number."""
        return self.filter_num

    def set_filter_num(self, new_filter):
        """Set filter wheel position."""
        # Validate filter number
        if new_filter < 0 or new_filter >= 8:
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
            self.f0pos.value, self.f1pos.value,
            self.f2pos.value, self.f3pos.value,
            self.f4pos.value, self.f5pos.value,
            self.f6pos.value, self.f7pos.value
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
        """Home the filter wheel."""
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

        # Send home command with long timeout
        response = self.serial_comm.send_command("M 1 HOM", True, 30.0)

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

    def on_value_changed_from_client(self, value, old_value, new_value):
        """Handle value changes from client."""
        try:
            # Handle specific values
            if value.name == "NEON":
                self._set_neon(new_value)
            elif value.name == "FOCPOS":
                self.serial_comm.send_command(f"F 0 ABS {new_value}")
            elif value.name.startswith("F") and value.name.endswith("POS"):
                # If this is the current filter, update position
                filter_idx = int(value.name[1])
                if filter_idx == self.filter_num:
                    self.serial_comm.send_command(f"M 1 ABS {new_value}")
        except Exception as e:
            logging.error(f"Error handling value change {value.name}: {e}")

        # Call parent handler
        super().on_value_changed_from_client(value, old_value, new_value)

    def _set_neon(self, new_value):
        """Set neon lamp state."""
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


if __name__ == "__main__":
    # Create application
    app = App(description='OVIS Filter Wheel Driver')

    # Register device-specific options
    app.register_device_options(Ovis)

    # Parse command line arguments
    args = app.parse_args()

    # Create and configure device
    device = app.create_device(Ovis)

    # Run application main loop
    app.run()
