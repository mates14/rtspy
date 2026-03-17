#!/usr/bin/python3

import time
import random
import logging
import threading
from typing import Dict, Any

from rtspy.core.value import ValueDouble, ValueInteger, ValueTime
from rtspy.core.device import Device
from rtspy.core.constants import DeviceType
from rtspy.core.config import DeviceConfig
from rtspy.core.device_caring_mixin import DeviceCaringMixin
from rtspy.core.app import App


class TemperatureSensor(Device, DeviceCaringMixin, DeviceConfig):
    """Temperature sensor device with caring loop support."""

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
        
        # Add caring loop configuration
        self.setup_caring_config(config)

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

        # Hardware simulation (for compatibility with old version)
        self.simulation_running = False
        self.simulation_thread = None
        
        # Caring loop state
        self.temp_drift_direction = 1  # 1 for increasing, -1 for decreasing
        self.temp_target = 20.0  # Target temperature for drift
        self.last_temp_update = time.time()

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
        self.temp_target = self.temperature.value

        # Apply caring configuration
        self.apply_caring_config(config)

        logging.info(f"Temperature sensor configured:")
        logging.info(f"  Initial temperature: {self.temperature.value}°C")
        logging.info(f"  Temperature range: ±{self.temp_range}°C")
        logging.info(f"  Update interval: {self.update_interval}s")
        logging.info(f"  Caring loop interval: {self.caring_interval}s")

    def on_number_changed(self, old_value, new_value):
        """Handle temperature value change from client."""
        logging.info(f"Number changed from {old_value} to {new_value}")
        # Here you would typically implement hardware-specific logic
        # For example, if this were a thermostat, you might:
        # self.set_target_temperature(new_value)

    def start(self):
        """Start the device."""
        super().start()

        # Start caring loop instead of old simulation thread
        if not self.start_caring_loop():
            logging.error("Failed to start caring loop, falling back to simulation thread")
            # Fallback to old simulation for compatibility
            self.simulation_running = True
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
        # Stop caring loop
        self.stop_caring_loop()
        
        # Stop old simulation thread if running
        self.simulation_running = False
        if self.simulation_thread:
            self.simulation_thread.join(timeout=2.0)
        
        super().stop()

    def info(self):
        """Update device information."""
        super().info()
        # Update info time
        self.infotime.value = time.time()

    def device_care_update(self):
        """
        Caring loop implementation for temperature sensor.
        
        This replaces the old _simulation_loop with more sophisticated
        temperature simulation that demonstrates caring loop capabilities.
        """
        current_time = time.time()
        
        # Check if it's time to update temperature based on update_interval
        if current_time - self.last_temp_update >= self.update_interval:
            # Simulate realistic temperature drift with trends
            current_temp = self.temperature.value
            
            # Change direction if we've reached the limits
            if current_temp >= self.temp_target + self.temp_range:
                self.temp_drift_direction = -1
                # Set new random target within range
                self.temp_target = random.uniform(
                    self.temp_target - self.temp_range, 
                    self.temp_target
                )
            elif current_temp <= self.temp_target - self.temp_range:
                self.temp_drift_direction = 1
                # Set new random target within range
                self.temp_target = random.uniform(
                    self.temp_target, 
                    self.temp_target + self.temp_range
                )
            
            # Apply temperature change
            temp_change = self.temp_drift_direction * random.uniform(0.1, 0.3)
            
            # Add some random noise
            noise = random.uniform(-0.05, 0.05)
            
            new_temp = current_temp + temp_change + noise
            
            # Bounds checking
            abs_min_temp = -50.0
            abs_max_temp = 100.0
            new_temp = max(abs_min_temp, min(abs_max_temp, new_temp))
            
            self.temperature.value = new_temp
            self.last_temp_update = current_time
            
            # Log significant temperature changes
            if abs(temp_change) > 0.2:
                logging.debug(f"Temperature changed: {current_temp:.2f}°C -> {new_temp:.2f}°C (target: {self.temp_target:.2f}°C)")
        
        # Demonstrate additional caring loop capabilities
        # Check for "alarm" conditions
        if self.temperature.value > self.temp_target + self.temp_range * 0.9:
            # Could set warning state or trigger actions
            pass
        elif self.temperature.value < self.temp_target - self.temp_range * 0.9:
            # Could set warning state or trigger actions
            pass
        
        # Update caring statistics - demonstrate loop monitoring
        # This shows how a real device might track performance
        
    def _simulation_loop(self):
        """Legacy simulation loop for fallback compatibility."""
        time.sleep(1)  # Initial delay

        while self.simulation_running:
            # Simple temperature changes (old behavior)
            self.temperature.value += random.uniform(-0.5, 0.5)
            time.sleep(2)

    def on_state_changed(self, old_state, new_state, message):
        """Handle device state changes."""
        logging.info(f"State changed from {old_state:x} to {new_state:x}: {message}")

    def on_value_changed_from_client(self, value, old_value, new_value):
        """Handle value changes from clients."""
        # Call parent handlers
        super().on_value_changed_from_client(value, old_value, new_value)
        
        # Handle temperature sensor specific changes
        if value == self.number:
            self.on_number_changed(old_value, new_value)
        elif value == self.temperature:
            # If temperature is set manually, adjust our target
            self.temp_target = new_value
            logging.info(f"Temperature target updated to {new_value}°C")


if __name__ == "__main__":
    # Create application
    app = App(description='Temperature Sensor Device with Caring Loop')

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
        
        # Show caring status
        print("\nCaring Loop Status:")
        print("=" * 50)
        for key, value in device.get_caring_status().items():
            print(f"  {key}: {value}")
        print("=" * 50)

    # Run application
    app.run()
