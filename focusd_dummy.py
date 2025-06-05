#!/usr/bin/env python3
"""
Dummy Focuser for RTS2 Python Framework

A simple dummy focuser implementation that simulates focuser movement.
This is useful for testing and as a template for new focuser drivers.
"""

import time
import logging
import math
import threading
from typing import Dict, Any, Optional

from focusd import Focusd
from value import ValueDouble, ValueInteger
from app import App


class DummyFocuser(Focusd):
    """
    Dummy focuser implementation for testing and development.
    
    This focuser simulates movement by gradually changing position
    toward the target at a configurable rate.
    """
    
    def setup_config(self, config):
        """Set up dummy focuser configuration."""
        super().setup_config(config)
        
        # Dummy-specific options
        config.add_argument('--foc-steps', type=float, default=1.0,
                          help='Focuser step size per update', section='focuser')
        config.add_argument('--simulate-temperature', action='store_true',
                          help='Simulate temperature sensor', section='focuser')
        config.add_argument('--temp-amplitude', type=float, default=5.0,
                          help='Temperature simulation amplitude', section='focuser')
        config.add_argument('--temp-period', type=float, default=3600.0,
                          help='Temperature simulation period (seconds)', section='focuser')
        
    def __init__(self, device_name="F0", port=0):
        """Initialize dummy focuser."""
        super().__init__(device_name, port)
        
        # Set focuser type
        self.focuser_type = "Dummy"
        
        # Dummy-specific values (will be created after apply_config)
        self.focstep = None
        self.foc_min = None
        self.foc_max = None
        
        # Internal state
        self._current_position = 0.0
        self._simulate_temp = False
        self._temp_amplitude = 5.0
        self._temp_period = 3600.0
        self._start_time = time.time()
        
    def apply_config(self, config: Dict[str, Any]):
        """Apply dummy focuser configuration."""
        super().apply_config(config)
        
        # Create dummy-specific values after base initialization
        self.focstep = ValueDouble("focstep", "focuser steps (step size per update)", 
                                 write_to_fits=False, writable=True, initial=1.0)
        self.foc_min = ValueDouble("foc_min", "minimal focuser value", 
                                 write_to_fits=False, writable=True, initial=float('nan'))
        self.foc_max = ValueDouble("foc_max", "maximal focuser value", 
                                 write_to_fits=False, writable=True, initial=float('nan'))
        
        # Apply dummy-specific config
        if config.get('foc_steps'):
            self.focstep.value = config['foc_steps']
            
        self._simulate_temp = config.get('simulate_temperature', False)
        self._temp_amplitude = config.get('temp_amplitude', 5.0)
        self._temp_period = config.get('temp_period', 3600.0)
        
        # Set focus limits if provided
        foc_min = config.get('foc_min')
        foc_max = config.get('foc_max')
        if foc_min is not None:
            self.foc_min.value = foc_min
            self._focus_min = foc_min
        if foc_max is not None:
            self.foc_max.value = foc_max
            self._focus_max = foc_max
            
        # Create temperature sensor if simulation enabled
        if self._simulate_temp:
            self.create_temperature()
            self.foc_temp.value = 20.0  # Start at 20°C
            
        # Set initial position
        self.foc_pos.value = self._current_position
        
    def start(self):
        """Start the dummy focuser."""
        super().start()
        
        logging.info("Dummy focuser started")
        logging.info(f"  Position: {self._current_position}")
        logging.info(f"  Step size: {self.focstep.value}")
        logging.info(f"  Temperature simulation: {self._simulate_temp}")
        
        if self._simulate_temp:
            logging.info(f"  Temperature amplitude: ±{self._temp_amplitude}°C")
            logging.info(f"  Temperature period: {self._temp_period}s")
            
        # Set initial state
        self.set_state(self.FOC_SLEEPING, "Dummy focuser ready")
            
    def info(self):
        """Update focuser information."""
        super().info()
        
        # Update position
        self.foc_pos.value = self._current_position
        
        # Update simulated temperature if enabled
        if self._simulate_temp and self.foc_temp:
            elapsed = time.time() - self._start_time
            # Simulate temperature variation with some randomness
            base_temp = 20.0
            seasonal_variation = self._temp_amplitude * math.sin(2 * math.pi * elapsed / self._temp_period)
            # Add small random fluctuations
            import random
            noise = random.uniform(-0.5, 0.5)
            self.foc_temp.value = base_temp + seasonal_variation + noise
            
    def set_to(self, position: float) -> int:
        """
        Start movement to target position.
        
        Args:
            position: Target position
            
        Returns:
            0 on success, -1 on error
        """
        try:
            # Check limits
            if not math.isnan(self.foc_min.value) and position < self.foc_min.value:
                logging.error(f"Position {position} below minimum {self.foc_min.value}")
                return -1
                
            if not math.isnan(self.foc_max.value) and position > self.foc_max.value:
                logging.error(f"Position {position} above maximum {self.foc_max.value}")
                return -1
                
            # Set step direction
            if self._current_position > position:
                self.focstep.value = -abs(self.focstep.value)
            else:
                self.focstep.value = abs(self.focstep.value)
                
            logging.info(f"Dummy focuser starting movement from {self._current_position} to {position}")
            return 0
            
        except Exception as e:
            logging.error(f"Error in dummy focuser set_to: {e}")
            return -1
            
    def is_focusing(self) -> int:
        """
        Simulate focusing by gradually moving toward target.
        
        Returns:
            >= 0: Still focusing (seconds to wait)
            -1: Error occurred  
            -2: Focusing completed
        """
        # Call parent timeout/error checking
        result = super().is_focusing()
        if result == -1:
            return -1  # Error (timeout, etc.)
            
        target = self.get_target()
        
        # Check if we're close enough to target
        if abs(self._current_position - target) < abs(self.focstep.value):
            # Close enough - snap to target and finish
            self._current_position = target
            self.foc_pos.value = self._current_position
            return -2  # Completed
            
        # Move one step toward target
        self._current_position += self.focstep.value
        self.foc_pos.value = self._current_position
        
        # Send position update to clients
        self.network.distribute_value_immediate(self.foc_pos)
        
        # Continue focusing - check again in 1 second
        return 1
        
    def is_at_start_position(self) -> bool:
        """Check if at start position."""
        # For dummy, assume any position near 0 is start
        return abs(self._current_position) < 1.0
        
    def write_position(self, new_position: float) -> int:
        """
        Write position directly (if FOC_POS is made writable).
        
        Args:
            new_position: New position value
            
        Returns:
            0 on success, -1 on error
        """
        try:
            # Check limits
            if not math.isnan(self.foc_min.value) and new_position < self.foc_min.value:
                logging.error(f"Position {new_position} below minimum {self.foc_min.value}")
                return -1
                
            if not math.isnan(self.foc_max.value) and new_position > self.foc_max.value:
                logging.error(f"Position {new_position} above maximum {self.foc_max.value}")
                return -1
                
            # Set position directly
            old_pos = self._current_position
            self._current_position = new_position
            self.foc_pos.value = self._current_position
            
            # Also update target and default to match
            self.foc_tar.value = new_position
            self.foc_def.value = new_position
            
            # Send updates to clients
            self.network.distribute_value_immediate(self.foc_pos)
            self.network.distribute_value_immediate(self.foc_tar)
            self.network.distribute_value_immediate(self.foc_def)
            
            logging.info(f"Dummy focuser position set directly from {old_pos} to {new_position}")
            return 0
            
        except Exception as e:
            logging.error(f"Error writing position: {e}")
            return -1
        
    def home_focuser(self) -> int:
        """
        Home the focuser by moving to position 0.

        Returns:
            0 on success, -1 on error
        """
        logging.info("Homing dummy focuser")

        # Set device state to show focuser is moving
        self.set_state(
            self._state | self.FOC_FOCUSING,
            "Homing focuser",
            self.BOP_EXPOSURE
        )

        def home_movement():
            # Simulate homing operation with timeout
            time.sleep(self.focstep.value * 1.5)  # Home takes a bit longer

            # Update focuser position to 0 (home)
            self._current_position = 0.0
            self.foc_pos.value = 0.0
            self.foc_tar.value = 0.0

            # Send updated values to clients
            self.network.distribute_value_immediate(self.foc_pos)
            self.network.distribute_value_immediate(self.foc_tar)

            # Reset state
            self.set_state(
                self._state & ~self.FOC_FOCUSING,
                "Focuser homed",
                0
            )

        # Start homing in a separate thread
        threading.Thread(target=home_movement, daemon=True).start()
        return 0
        
    def on_value_changed_from_client(self, value, old_value, new_value):
        """Handle value changes from clients."""
        try:
            # Handle limit changes
            if value.name == "foc_min":
                self._focus_min = new_value
                self.set_focus_extent(new_value, self.foc_max.value)
                return 0
            elif value.name == "foc_max":
                self._focus_max = new_value  
                self.set_focus_extent(self.foc_min.value, new_value)
                return 0
            elif value.name == "focstep":
                # Allow changing step size
                logging.info(f"Focuser step size changed to {new_value}")
                return 0
            else:
                # Let parent handle focuser values
                return super().on_value_changed_from_client(value, old_value, new_value)
        except Exception as e:
            logging.error(f"Error handling value change in dummy focuser: {e}")
            return -1


def main():
    """Main entry point for dummy focuser."""
    # Create application
    app = App(description='RTS2 Dummy Focuser')
    
    # Register device options
    app.register_device_options(DummyFocuser)
    
    # Parse arguments
    args = app.parse_args()
    
    # Create device
    device = app.create_device(DummyFocuser)
    
    # Show configuration if debug enabled
    if getattr(args, 'debug', False):
        print("\nDummy Focuser Configuration:")
        print("=" * 40)
        print(device.get_config_summary())
        print("=" * 40)
        
    logging.info("Starting RTS2 Dummy Focuser")
    
    # Run application
    try:
        app.run()
        return 0
    except KeyboardInterrupt:
        logging.info("Shutting down dummy focuser")
        return 0
    except Exception as e:
        logging.error(f"Fatal error in dummy focuser: {e}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
