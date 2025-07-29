#!/usr/bin/python3

import time
import logging
import threading
from typing import Optional, Any, Dict

from value import ValueDouble, ValueString
from device import Device
from config import DeviceConfig
from constants import DeviceType
from app import App

class WatcherDevice(Device, DeviceConfig):
    """Simple device that watches state and values of another device."""

    def setup_config(self, config):
        """Register WatcherDevice-specific options."""
        config.add_argument('--watch-value', default="centrald.sun_alt",
                          help='Name of a value to be watched (format: device_name.value_name)')
        config.add_argument('--watch-device', default="centrald",
                          help='Name of a device to watch state changes')

    def apply_config(self, config: Dict[str, Any]):
        """Process arguments for this device."""
        self.watch_value = args.watch_value
        self.watch_device = args.watch_device

    def __init__(self, device_name="WATCH", port=0):
        """Initialize the watcher device."""
        super().__init__(device_name, DeviceType.LOGD, port)

        # Default values for watched device/variable
        self.watch_value = "centrald.sun_alt"  # Default value to watch
        self.watch_device = "centrald"  # Default device to watch

        # Create a value to store the watched variable's value
        self.watched_value = ValueDouble("watched_value", "Value being watched")

        # Store the device state
        self.watched_device_state = ValueString("watched_device_state", "State of watched device")

        # Set initial state
        self.set_state(self.STATE_IDLE, "Initializing device")

    def _on_centrald_connected(self, conn_id):
        """Called when connected to centrald."""
        # Call parent implementation first
        logging.info("Connected to centrald")
        super()._on_centrald_connected(conn_id)

    def _on_value_update(self, value_data):
        """
        Handle value update from watched device.

        Args:
            value_data: Value as string
        """
        try:
            # Parse value
            logging.debug(f"Received value update: {value_data}")

            # Store the value
            try:
                # Try to convert to float
                self.watched_value.value = float(value_data)
            except ValueError:
                # If not a float, store as string
                self.watched_value.value = float('nan')
                logging.debug(f"Could not convert '{value_data}' to float")

        except Exception as e:
            logging.error(f"Error processing value update: {e}")

    def _on_device_state_changed(self, device_name, state, bop_state, message):
        """
        Handle device state change.

        Args:
            device_name: Device name
            state: Device state
            bop_state: Block operation state
            message: Status message
        """
        logging.debug(f"Device state changed: {device_name} state={state:x}, bop={bop_state:x}, msg='{message}'")

        # Store state information
        self.watched_device_state.value = f"{state:x}:{bop_state:x}:{message}"

    def start(self):
        """Start the device."""
        super().start()

        # Register centrald interests BEFORE connection to catch initial state flood
        # CENTRALD QUIRK: centrald sends all values immediately after 'register'
        # before authentication, so we must register interests early

        # Register interest in device state changes
        logging.debug(f"Registering interest in {self.watch_device} state")
        self.network.register_state_interest(
            self.watch_device,
            self._on_device_state_changed
        )

        # Parse the watched value into device and value components
        device, value = self.watch_value.split(".")

        # Register interest in the specified value
        logging.debug(f"Registering interest in {device}.{value} value")
        self.network.register_interest_in_value(
            device,
            value,
            self._on_value_update
        )

        logging.info(f"Watcher device started, watching {self.watch_value} and {self.watch_device} state")
        self.set_ready("Watcher ready")

if __name__ == "__main__":
    # Create application
    app = App(description='RTS2 Device Watcher')

    # Register device-specific options
    app.register_device_options(WatcherDevice)

    # Parse command line arguments
    args = app.parse_args()

    # Create and configure device
    device = app.create_device(WatcherDevice)

    # Run application main loop
    app.run()
