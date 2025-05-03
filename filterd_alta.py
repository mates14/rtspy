#!/usr/bin/python3

import time
import logging
import threading
import serial
from typing import Dict, List, Any, Callable, Optional, Tuple

from value import (
    ValueSelection, ValueInteger, ValueBool, ValueString, ValueTime
)
from filterd import Filterd
from constants import DeviceType, ConnectionState
from app import App

class Alta(Filterd):
    """
    Alta filter wheel driver for spectrograph.

    This is a Python implementation of the Alta filter wheel driver from RTS2.
    """

    @classmethod
    def register_options(cls, parser):
        """Register Alta-specific command line options."""
        super().register_options(parser)
        parser.add_argument('-f', '--device-file',
                          help='Serial port with the module (usually /dev/ttyUSB for Arduino USB serial connection)')

    @classmethod
    def process_args(cls, device, args):
        """Process arguments for this specific device."""
        super().process_args(device, args)
        if hasattr(args, 'device_file') and args.device_file:
            device.device_file = args.device_file

    def __init__(self, device_name="W0", port=0):
        """Initialize the Alta filter wheel device."""
        super().__init__(device_name, port)

        # Serial connection
        self.device_file = None
        self.serial_conn = None

        # Current filter position
        self.filter_num = 0

        # Create values
        self.focpos = ValueInteger("FOCPOS", "[int] focuser position", write_to_fits=True)
        self.focpos.set_writable()

        self.neon = ValueSelection("NEON", "[on/off] neon lamp status", write_to_fits=True)
        self.neon.set_writable()
        self.neon.add_sel_val("off")
        self.neon.add_sel_val("on")

        # Filter position values
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

        # Motor 0 values (focuser)
        self.m0pwr = ValueSelection("M0PWR", "[true/false] motor power status", write_to_fits=True)
        self.m0pwr.set_writable()
        self.m0pwr.add_sel_val("off")
        self.m0pwr.add_sel_val("on")

        self.m0hom = ValueSelection("M0HOM", "[true/false] motor homing status", write_to_fits=True)
        self.m0hom.add_sel_val("false")
        self.m0hom.add_sel_val("true")

        self.m0pos = ValueInteger("M0POS", "[int] motor position", write_to_fits=True)
        self.m0spd = ValueInteger("M0SPD", "[int] motor max speed", write_to_fits=True)
        self.m0acc = ValueInteger("M0ACC", "[int] motor acceleration", write_to_fits=True)

        self.m0pos.set_writable()
        self.m0spd.set_writable()
        self.m0acc.set_writable()

        # Motor 1 values (filter wheel)
        self.m1pwr = ValueSelection("M1PWR", "[true/false] motor power status", write_to_fits=True)
        self.m1pwr.set_writable()
        self.m1pwr.add_sel_val("off")
        self.m1pwr.add_sel_val("on")

        self.m1hom = ValueSelection("M1HOM", "[true/false] motor homing status", write_to_fits=True)
        self.m1hom.add_sel_val("false")
        self.m1hom.add_sel_val("true")

        self.m1pos = ValueInteger("M1POS", "[int] motor position", write_to_fits=True)
        self.m1spd = ValueInteger("M1SPD", "[int] motor max speed", write_to_fits=True)
        self.m1acc = ValueInteger("M1ACC", "[int] motor acceleration", write_to_fits=True)

        self.m1pos.set_writable()
        self.m1spd.set_writable()
        self.m1acc.set_writable()

        # Set default values similar to the C++ implementation
        self.f0pos.value = 2000
        self.f1pos.value = 54500
        self.f2pos.value = 107000
        self.f3pos.value = 159500
        self.f4pos.value = 212000
        self.f5pos.value = 292000  # grism
        self.f6pos.value = 300000  # unused
        self.f7pos.value = 300000  # limit

        self.m0hom.value = 0
        self.m1hom.value = 0

        # Register all values with the device
        self.register_value(self.focpos)
        self.register_value(self.neon)
        self.register_value(self.f0pos)
        self.register_value(self.f1pos)
        self.register_value(self.f2pos)
        self.register_value(self.f3pos)
        self.register_value(self.f4pos)
        self.register_value(self.f5pos)
        self.register_value(self.f6pos)
        self.register_value(self.f7pos)
        self.register_value(self.m0pwr)
        self.register_value(self.m0hom)
        self.register_value(self.m0pos)
        self.register_value(self.m0spd)
        self.register_value(self.m0acc)
        self.register_value(self.m1pwr)
        self.register_value(self.m1hom)
        self.register_value(self.m1pos)
        self.register_value(self.m1spd)
        self.register_value(self.m1acc)

    def start(self):
        """Start the device."""
        super().start()

        # Try to initialize the device if we have a serial port
        if self.device_file:
            try:
                self._init_serial()
                self._check_device_id()
                self._update_status()
            except Exception as e:
                logging.error(f"Error initializing device: {e}")
                self.set_state(self.STATE_IDLE | self.ERROR_HW, "Initialization error")
                return

        # Set device ready after initialization
        self.set_ready("Alta filter wheel ready")

    def stop(self):
        """Stop the device."""
        # Close serial connection
        if self.serial_conn:
            self.serial_conn.close()
            self.serial_conn = None

        super().stop()

    def _init_serial(self):
        """Initialize serial connection."""
        if not self.device_file:
            raise ValueError("No device file specified")

        logging.debug(f"Opening serial connection to {self.device_file}")
        self.serial_conn = serial.Serial(
            port=self.device_file,
            baudrate=9600,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            timeout=5
        )

        # Clear any buffered data
        self.serial_conn.reset_input_buffer()
        self.serial_conn.reset_output_buffer()

        # Wait for device to initialize
        time.sleep(1)

    def _send_command(self, cmd):
        """Send a command to the device."""
        if not self.serial_conn:
            raise RuntimeError("No serial connection established")

        # Ensure command ends with newline
        if not cmd.endswith('\n'):
            cmd += '\n'

        logging.debug(f"Sending command: {cmd.strip()}")
        self.serial_conn.write(cmd.encode())
        return True

    def _read_response(self, timeout=5):
        """Read a response line from the device."""
        if not self.serial_conn:
            raise RuntimeError("No serial connection established")

        # Store original timeout
        original_timeout = self.serial_conn.timeout
        self.serial_conn.timeout = timeout

        try:
            response = self.serial_conn.readline().decode().strip()
            logging.debug(f"Received response: {response}")
            return response
        finally:
            # Restore original timeout
            self.serial_conn.timeout = original_timeout

    def _check_device_id(self):
        """Check if we have the correct device."""
        self._send_command("ID")
        response = self._read_response()

        if response != "RPI_PICO_SPECTRAL_AVCR":
            raise RuntimeError(f"Invalid device ID: {response}")

        logging.info(f"Connected to device: {response}")
        return True

    def _update_status(self):
        """Update device status by querying the hardware."""
        self._send_command("STATUS")

        # Read motor 0 status
        response = self._read_response()
        motor_info = response.split()
        if len(motor_info) >= 5 and motor_info[0] == "M" and motor_info[1] == "0":
            self.m0pwr.value = int(motor_info[2])
            self.m0pos.value = int(motor_info[3])
            self.m0spd.value = int(float(motor_info[4]))
            self.m0acc.value = int(float(motor_info[5])) if len(motor_info) > 5 else 0

        # Read motor 1 status
        response = self._read_response()
        motor_info = response.split()
        if len(motor_info) >= 5 and motor_info[0] == "M" and motor_info[1] == "1":
            self.m1pwr.value = int(motor_info[2])
            self.m1pos.value = int(motor_info[3])
            self.m1spd.value = int(float(motor_info[4]))
            self.m1acc.value = int(float(motor_info[5])) if len(motor_info) > 5 else 0

        # Read motor 2 status (if present)
        response = self._read_response()

        # Read shutter status
        response = self._read_response()

        # Read neon status
        response = self._read_response()
        neon_info = response.split()
        if len(neon_info) >= 2 and neon_info[0] == "R":
            self.neon.value = int(neon_info[1])

        return True

    def get_filter_num(self):
        """Get current filter number."""
        return self.filter_num

    def set_filter_num(self, new_filter):
        """
        Set filter number (position).

        Args:
            new_filter: New filter position

        Returns:
            0 on success, -1 on error
        """
        # Check filter number validity
        if new_filter < 0 or new_filter >= 8:
            return -1

        # Get the target position for this filter
        target_positions = [
            self.f0pos.value,
            self.f1pos.value,
            self.f2pos.value,
            self.f3pos.value,
            self.f4pos.value,
            self.f5pos.value,
            self.f6pos.value,
            self.f7pos.value
        ]

        target_position = target_positions[new_filter]
        logging.info(f"Moving filter to position {new_filter}, motor position {target_position}")

        try:
            # Send the position command to motor 1
            cmd = f"M 1 ABS {target_position}"
            self._send_command(cmd)
            response = self._read_response()

            if "OK" not in response:
                logging.error(f"Error setting filter position: {response}")
                return -1

            # Update internal filter number
            self.filter_num = new_filter

            # Call parent method to handle state changes
            return super().set_filter_num(new_filter)

        except Exception as e:
            logging.error(f"Error setting filter: {e}")
            return -1

    def home_filter(self):
        """
        Home the filter wheel by moving to filter 1.

        Returns:
            0 on success, -1 on error
        """
        return self.set_filter_num(1)

    def info(self):
        """Update device information."""
        try:
            self._update_status()
        except Exception as e:
            logging.error(f"Error updating status: {e}")

        return super().info()

    def on_value_changed_from_client(self, value, old_value, new_value):
        """
        Handle value changes from client.

        This is called when a client changes a value via the network.
        """
        # Handle special cases for different values
        value_name = value.name

        if value_name == "NEON":
            self._handle_neon_change(new_value)
        elif value_name == "FOCPOS":
            self._handle_focus_position_change(new_value)
        elif value_name == "M0POS":
            self._handle_motor0_position_change(new_value)
        elif value_name == "M1POS":
            self._handle_motor1_position_change(new_value)
        elif value_name == "M0PWR":
            self._handle_motor0_power_change(new_value)
        elif value_name == "M1PWR":
            self._handle_motor1_power_change(new_value)
        elif value_name == "M0SPD":
            self._handle_motor0_speed_change(new_value)
        elif value_name == "M1SPD":
            self._handle_motor1_speed_change(new_value)
        elif value_name == "M0ACC":
            self._handle_motor0_acceleration_change(new_value)
        elif value_name == "M1ACC":
            self._handle_motor1_acceleration_change(new_value)
        elif value_name.startswith("F") and value_name.endswith("POS"):
            # Handle filter position change - might need to update actual position if this is the current filter
            filter_idx = int(value_name[1])
            if filter_idx == self.filter_num:
                self._handle_current_filter_position_change(new_value)

        # Call parent handler
        super().on_value_changed_from_client(value, old_value, new_value)

    def _handle_neon_change(self, new_value):
        """Handle neon lamp setting change."""
        try:
            if new_value == 0:  # OFF
                self._send_command("S ON")
                self._send_command("S IN")
                self._send_command("R OFF")
            else:  # ON
                self._send_command("S ON")
                self._send_command("S OUT")
                self._send_command("R ON")

            # Read responses
            for _ in range(3):
                self._read_response()

        except Exception as e:
            logging.error(f"Error changing neon state: {e}")

    def _handle_focus_position_change(self, new_value):
        """Handle focus position change."""
        try:
            cmd = f"F 0 ABS {new_value}"
            self._send_command(cmd)
            self._read_response()
        except Exception as e:
            logging.error(f"Error changing focus position: {e}")

    def _handle_motor0_position_change(self, new_value):
        """Handle motor 0 position change."""
        try:
            cmd = f"M 0 ABS {new_value}"
            self._send_command(cmd)
            self._read_response()
        except Exception as e:
            logging.error(f"Error changing motor 0 position: {e}")

    def _handle_motor1_position_change(self, new_value):
        """Handle motor 1 position change."""
        try:
            cmd = f"M 1 ABS {new_value}"
            self._send_command(cmd)
            self._read_response()
        except Exception as e:
            logging.error(f"Error changing motor 1 position: {e}")

    def _handle_motor0_power_change(self, new_value):
        """Handle motor 0 power state change."""
        try:
            if new_value == 1:  # ON
                self._send_command("M 0 ON")
                self._read_response()

                # Wait for motor to initialize
                time.sleep(0.1)

                # Home the motor
                self._send_command("M 0 HOM")
                response = self._read_response()

                if "OK" in response:
                    self.m0hom.value = 1
                else:
                    self.m0hom.value = 0
            else:  # OFF
                self._send_command("M 0 OFF")
                self._read_response()
                self.m0hom.value = 0
        except Exception as e:
            logging.error(f"Error changing motor 0 power: {e}")

    def _handle_motor1_power_change(self, new_value):
        """Handle motor 1 power state change."""
        try:
            if new_value == 1:  # ON
                self._send_command("M 1 ON")
                self._read_response()

                # Wait for motor to initialize
                time.sleep(0.1)

                # Home the motor
                self._send_command("M 1 HOM")
                response = self._read_response()

                if "OK" in response:
                    self.m1hom.value = 1
                else:
                    self.m1hom.value = 0
            else:  # OFF
                self._send_command("M 1 OFF")
                self._read_response()
                self.m1hom.value = 0
        except Exception as e:
            logging.error(f"Error changing motor 1 power: {e}")

    def _handle_motor0_speed_change(self, new_value):
        """Handle motor 0 speed change."""
        try:
            cmd = f"M 0 SPD {new_value}"
            self._send_command(cmd)
            self._read_response()
        except Exception as e:
            logging.error(f"Error changing motor 0 speed: {e}")

    def _handle_motor1_speed_change(self, new_value):
        """Handle motor 1 speed change."""
        try:
            cmd = f"M 1 SPD {new_value}"
            self._send_command(cmd)
            self._read_response()
        except Exception as e:
            logging.error(f"Error changing motor 1 speed: {e}")

    def _handle_motor0_acceleration_change(self, new_value):
        """Handle motor 0 acceleration change."""
        try:
            cmd = f"M 0 ACC {new_value}"
            self._send_command(cmd)
            self._read_response()
        except Exception as e:
            logging.error(f"Error changing motor 0 acceleration: {e}")

    def _handle_motor1_acceleration_change(self, new_value):
        """Handle motor 1 acceleration change."""
        try:
            cmd = f"M 1 ACC {new_value}"
            self._send_command(cmd)
            self._read_response()
        except Exception as e:
            logging.error(f"Error changing motor 1 acceleration: {e}")

    def _handle_current_filter_position_change(self, new_value):
        """Handle change to current filter position."""
        try:
            cmd = f"M 1 ABS {new_value}"
            self._send_command(cmd)
            self._read_response()
        except Exception as e:
            logging.error(f"Error changing current filter position: {e}")


if __name__ == "__main__":
    # Create application
    app = App(description='Alta Filter Wheel Driver')

    # Register device-specific options
    app.register_device_options(Alta)

    # Parse command line arguments
    args = app.parse_args()

    # Create and configure device
    device = app.create_device(Alta)

    # Run application main loop
    app.run()
