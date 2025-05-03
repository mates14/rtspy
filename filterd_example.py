#!/usr/bin/python3

import time
import logging
import threading
from typing import Optional

from app import App
from filterd import Filterd
from value import ValueString, ValueDouble, ValueInteger


class CustomFilterWheel(Filterd):
    """
    Example custom filter wheel implementation.

    This serves as a template for implementing your own filter wheel driver.
    Replace the hardware communication methods with your own code.
    """

    @classmethod
    def register_options(cls, parser):
        """Register CustomFilterWheel-specific command line options."""
        super().register_options(parser)
        parser.add_argument('--device-id',
                           help='Hardware device ID or path (e.g., /dev/ttyUSB0)')

    @classmethod
    def process_args(cls, device, args):
        """Process arguments for this specific device."""
        super().process_args(device, args)
        if args.device_id:
            device.device_id = args.device_id

    def __init__(self, device_name="W0", port=0):
        """Initialize the custom filter wheel."""
        super().__init__(device_name, port)

        # Current filter position
        self.filter_num = 0

        # Hardware connection state
        self.is_connected = False
        self.device_id = None

        # Add hardware connection parameters
        self.connection_string = ValueString("connection", "hardware connection string")
        self.connection_string.set_writable()
        self.register_value(self.connection_string)

        # Movement timeout value
        self.movement_timeout = ValueDouble("move_timeout", "movement timeout [s]")
        self.movement_timeout.set_writable()
        self.movement_timeout.value = 10.0
        self.register_value(self.movement_timeout)

        # Initialize with some default filters
        self.filter_names = ValueString("filter_names", "filter names")
        self.filter_names.value = "Clear:Red:Green:Blue"
        self.register_value(self.filter_names)

        self.set_filters(self.filter_names.value)

    def start(self):
        """Start the filter wheel device."""
        super().start()

        # Connect to your hardware
        self.connect_to_hardware()

        # Set device ready
        self.set_ready("Filter wheel ready")

    def stop(self):
        """Stop the filter wheel device."""
        # Disconnect from hardware
        self.disconnect_from_hardware()

        # Call parent
        super().stop()

    def connect_to_hardware(self):
        """
        Connect to your filter wheel hardware.

        Returns:
            True on success, False on error
        """
        # Implement your hardware connection code here
        # For example:
        # - Connect to a USB device
        # - Initialize a GPIO interface
        # - Open a network connection

        try:
            # Simulate hardware connection
            logging.info("Connecting to filter wheel hardware...")

            # If device_id is set, use it for connection
            connection_target = self.device_id or "default_device"
            logging.info(f"Connecting to device: {connection_target}")

            time.sleep(1)  # Simulate connection time

            # Set connection state
            self.is_connected = True

            # Get initial position
            self.filter_num = self.read_position_from_hardware()

            logging.info(f"Connected to filter wheel, position: {self.filter_num}")
            return True

        except Exception as e:
            logging.error(f"Error connecting to hardware: {e}")
            self.set_state(
                self._state | self.ERROR_HW,
                f"Error connecting to hardware: {e}"
            )
            return False

    def disconnect_from_hardware(self):
        """Disconnect from the hardware."""
        if self.is_connected:
            # Implement your hardware disconnection code here
            logging.info("Disconnecting from filter wheel hardware...")
            self.is_connected = False

    def read_position_from_hardware(self):
        """
        Read current position from hardware.

        Returns:
            Current filter position or 0 on error
        """
        # Implement your hardware position reading code here
        # This is a placeholder implementation
        return 0

    def write_position_to_hardware(self, position):
        """
        Write position to hardware.

        Args:
            position: New filter position

        Returns:
            True on success, False on error
        """
        # Implement your hardware position setting code here
        # For example:
        # - Send command to motor controller
        # - Set GPIO pins
        # - Write to serial port

        # This is a placeholder implementation
        logging.info(f"Moving hardware to position {position}")

        # Set device state to show filter is moving
        self.set_state(
            self._state | self.FILTERD_MOVE,
            f"Moving to filter {position} ({self.filter.get_sel_name(position)})",
            self.BOP_EXPOSURE
        )

        time.sleep(1)  # Simulate movement time

        # Update internal position
        self.filter_num = position

        # Clear moving state
        self.set_state(
            self._state & ~(self.FILTERD_MOVE),
            "Filter wheel idle",
            0
        )

        return True

    def get_filter_num(self):
        """
        Get current filter number from hardware.

        Returns:
            Current filter position or -1 on error
        """
        if not self.is_connected:
            return -1

        # Read position from hardware
        return self.read_position_from_hardware()

    def set_filter_num(self, new_filter):
        """
        Set the filter number on the hardware.

        Args:
            new_filter: New filter position

        Returns:
            0 on success, -1 on error
        """
        if not self.is_connected:
            return -1

        # Validate filter number
        if new_filter < 0 or new_filter >= self.filter.sel_size():
            return -1

        # Write position to hardware
        if not self.write_position_to_hardware(new_filter):
            return -1

        # Update filter position
        self.filter_num = new_filter

        # Call parent implementation to update value
        return super().set_filter_num(new_filter)

    def home_filter(self):
        """
        Home the filter wheel.

        Returns:
            0 on success, -1 on error
        """
        if not self.is_connected:
            return -1

        logging.info("Homing filter wheel")

        # Set device state to show filter is moving
        self.set_state(
            self._state | self.FILTERD_MOVE,
            "Homing filter wheel",
            self.BOP_EXPOSURE
        )

        # Implement your homing code here
        time.sleep(1)  # Simulate homing time

        # Clear moving state
        self.set_state(
            self._state & ~(self.FILTERD_MOVE),
            "Filter wheel idle",
            0
        )

        return self.set_filter_num(0)

    def info(self):
        """Update device information."""
        if self.is_connected:
            # Read current position from hardware
            position = self.get_filter_num()
            if position >= 0 and position != self.filter.value:
                self.filter.value = position

    def on_state_changed(self, old_state, new_state, message):
        """Handle device state changes."""
        logging.info(f"Filter state changed from {old_state:x} to {new_state:x}: {message}")


# Example usage with command line options
if __name__ == "__main__":
    # Create application
    app = App(description='Custom Filter Wheel Driver')

    # Register device-specific options
    app.register_device_options(CustomFilterWheel)

    # Parse command line arguments
    args = app.parse_args()

    # Create and configure device
    device = app.create_device(CustomFilterWheel)

    # Run application main loop
    app.run()
