#!/usr/bin/python3

import time
import random
import logging
import threading
from typing import Dict, Any

from value import ValueDouble, ValueInteger, ValueTime
from device import Device
from constants import DeviceType
from config import SimpleDeviceConfig
from app import App


class TemperatureSensor(Device, SimpleDeviceConfig):
    """Simple temperature sensor device + configuration support."""

    def setup_config(self, config):
        """
        Set up temperature sensor configuration arguments.

        This method is called automatically by the configuration system.
        The 'config' parameter is a DeviceConfigRegistry instance that provides
        an argparse-like interface for defining configuration options.
        """
        # Device-specific arguments
        config.add_argument('--initial-temp', type=float, default=20.0,
                          help='Initial temperature value in Celsius')

        # Optional: You can specify sections for config files
        config.add_argument('--temp-range', type=float, default=5.0,
                          help='Temperature variation range', section='sensor')
        config.add_argument('--update-interval', type=float, default=2.0,
                          help='Temperature update interval in seconds', section='sensor')

    def __init__(self, device_name="TEMP", port=0):
        """Initialize the temperature sensor device."""
        super().__init__(device_name, DeviceType.SENSOR, port)

        # These will be set by apply_config() after configuration is processed
        self.temp_range = 5.0
        self.update_interval = 2.0

        # Create device values
        self.temperature = ValueDouble("TEMP", "Temperature in Celsius", write_to_fits=True, initial=20.0)
        self.number = ValueInteger("NUMBER", "Testing Integer", write_to_fits=True, writable=True, initial=20)

        # Initialize device state
        self.set_state(self.STATE_IDLE, "Initializing device")

        # Hardware simulation
        self.simulation_running = False
        self.simulation_thread = None

        # Set initial state
        self.set_state(self.STATE_IDLE, "Initializing temperature sensor")

    def apply_config(self, config: Dict[str, Any]):
        """
        Apply temperature sensor configuration.

        This method is called automatically after all configuration sources
        (command line, config files, environment variables) have been processed.
        The 'config' parameter is a flat dictionary with all resolved values.
        """
        # Get configuration values with defaults
        self.temp_range = config.get('temp_range', 5.0)
        self.update_interval = config.get('update_interval', 2.0)

        # Set initial temperature
        self.temperature.value = config.get('initial_temp', 20.0)

        logging.info(f"Temperature sensor configured:")
        logging.info(f"  Initial temperature: {self.temperature.value}°C")
        logging.info(f"  Temperature range: ±{self.temp_range}°C")
        logging.info(f"  Update interval: {self.update_interval}s")

    def on_number_changed(self, old_value, new_value):
        """Handle temperature value change from client."""
        logging.info(f"Number changed from {old_value} to {new_value}")
        # Here you would typically implement hardware-specific logic
        # For example, if this were a thermostat, you might:
        # self.set_target_temperature(new_value)

    def start(self):
        """Start the device."""
        super().start()

        # Start simulation thread
        self.simulation_running = True

        # Timer for the random value
        self.simulation_thread = threading.Thread(
            target=self._simulation_loop,
            name="TempSensorSim",
            daemon=True
        )
        self.simulation_thread.start()

        # Set device ready after initialization
        self.set_ready("Temperature sensor ready")
        logging.info(f"Temperature sensor {self.device_name} started")

    def stop(self):
        """Stop the device."""
        self.simulation_running = False
        if self.simulation_thread:
            self.simulation_thread.join(timeout=2.0)
        super().stop()

    def info(self):
        """Update device information."""
        super().info()
        # Update info time
        self.infotime.value = time.time()

    def _simulation_loop(self):
        """Timer loop to update random value."""
        time.sleep(1)  # Initial delay

        while self.simulation_running:
            # Simulate temperature changes
            self.temperature.value += random.uniform(-0.5, 0.5)
            time.sleep(2)

    def on_state_changed(self, old_state, new_state, message):
        """Handle device state changes."""
        logging.info(f"State changed from {old_state:x} to {new_state:x}: {message}")

if __name__ == "__main__":
    # Create application
    app = App(description='Temperature Sensor Device')

    # Register device options (this calls setup_config() internally)
    app.register_device_options(TemperatureSensor)

    # Parse arguments
    args = app.parse_args()

    # Create device (this calls apply_config() internally)
    device = app.create_device(TemperatureSensor)

    # Show config if debug enabled
    if getattr(args, 'debug', False):
        print("\nConfiguration Summary:")
        print("=" * 50)
        print(device.get_config_summary())
        print("=" * 50)

    # Run application
    app.run()

