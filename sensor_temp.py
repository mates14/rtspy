#!/usr/bin/python3

import time
import random
import logging
import threading
import math
#from enum import Enum, IntFlag

from value import (
    ValueDouble, ValueInteger, ValueBool, ValueString,
    ValueTime
    #ValueSelection, ValueStat, ValueFlags
)
from device import Device
from constants import DeviceType
from app import App

class TemperatureSensor(Device):
    """Example temperature sensor device."""

    @classmethod
    def register_options(cls, parser):
        """Register TemperatureSensor-specific options."""
        parser.add_argument('--initial-temp', type=float, default=20.0,
                          help='Initial temperature value')

    @classmethod
    def process_args(cls, device, args):
        """Process arguments for this device."""
        if hasattr(args, 'initial_temp'):
            device.temperature.value = args.initial_temp

    def __init__(self, device_name="TEMP", port=0):
        """Initialize the temperature sensor device."""
        super().__init__(device_name, DeviceType.SENSOR, port)

        # Sensor temperature data
        self.temperature = ValueDouble("TEMP", "Temperature in Celsius", write_to_fits=True)
        self.temperature.value = 20.0

        self.number = ValueInteger("NUMBER", "Testing Integer",
                                     write_to_fits=True)
        self.number.value = 20
        self.number.set_writable()  # Make this value writable

        # Register values with device
#        self.register_value(self.infotime)
#        self.register_value(self.uptime)
#        self.register_value(self.temperature)

        # Initialize device state
        self.set_state(self.STATE_IDLE, "Initializing device")

        # Hardware simulation
        self.simulation_running = False
        self.timer_thread = None
        self.simulation_thread = None

    def on_number_changed(self, old_value, new_value):
        """Handle temperature value change from client."""
        logging.info(f"Number changed from {old_value} to {new_value}")
        # Here you would typically implement hardware-specific logic
        # For example, if this were a thermostat, you might:
        # self.set_target_temperature(new_value)

    def start(self):
        """Start the device."""
        super().start()

        # Start timer threads
        self.simulation_running = True

        # Timer for the random value
        self.simulation_thread = threading.Thread(
            target=self._simulation_loop,
            name="RandomLoop",
            daemon=True
        )
        self.simulation_thread.start()

        # Connect to centrald
#        self.connect_to_centrald("localhost", 617)

        # Set device ready after initialization
        self.set_ready("Temperature sensor ready")

        # Mask BOP state to block exposure
        self.set_full_bop_state(0)

    def stop(self):
        """Stop the device."""
        self.simulation_running = False
        if self.simulation_thread:
            self.simulation_thread.join(timeout=2.0)
        if self.timer_thread:
            self.timer_thread.join(timeout=2.0)
        super().stop()

    def info(self):
        """Update device information."""
        # Update info time
        self.infotime.value = time.time()


    def _timer_loop(self):
        """Timer loop to increment timer_count every 5 seconds."""
        time.sleep(1)  # Initial delay

        while self.simulation_running:
            # Sleep for 5 seconds
            time.sleep(5)

    def _simulation_loop(self):
        """Timer loop to update random value."""
        time.sleep(1)  # Initial delay

        while self.simulation_running:
            # Simulate temperature changes
#             self.infotime.value = time.time()
            self.temperature.value += random.uniform(-0.5, 0.5)

            # Distribute changes
#            self.distribute_value(self.infotime)
#            self.distribute_value(self.uptime)
#            self.distribute_value(self.temperature)
#            self.distribute_value(self.humidity)
#            self.distribute_value(self.pressure)

            # Sleep
            time.sleep(2)

    def on_state_changed(self, old_state, new_state, message):
        """Handle device state changes."""
        logging.info(f"State changed from {old_state:x} to {new_state:x}: {message}")

    def should_queue_value(self, value):
        """Check if a value change should be queued based on device state."""
        # Queue value changes when device is not ready
#        if self.get_state() & self.NOT_READY:
#            return True
        return False

if __name__ == "__main__":
    app = App(description='Temperature Sensor Device')
    app.register_device_options(TemperatureSensor)
    args = app.parse_args()
    device = app.create_device(TemperatureSensor)
    app.run()

if False:
    # Example usage
    if __name__ == "__main__":
        logging.basicConfig(level=logging.DEBUG)

        # Create and start device
        temp_sensor = TemperatureSensor()
        temp_sensor.start()

        try:
            # Keep main thread alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Shutting down...")
        finally:
            temp_sensor.stop()
