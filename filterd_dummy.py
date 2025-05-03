#!/usr/bin/python3

import time
import logging
import threading
from typing import Optional

from value import ValueString, ValueDouble
from filterd import Filterd
from app import App

class DummyFilter(Filterd):
    """Dummy filter wheel implementation for testing.
    This simulates a filter wheel with configurable filters and movement time."""
    
    @classmethod
    def register_options(cls, parser):
        """Register DummyFilter-specific command line options."""
        super().register_options(parser)
        parser.add_argument('-s', '--sleep', type=float, default=1.0,
                          help='Filter movement time in seconds')
    
    @classmethod
    def process_args(cls, device, args):
        """Process arguments for this specific device."""
        super().process_args(device, args)
        if args.sleep:
            device.filter_sleep.value = args.sleep

    def __init__(self, device_name="W0", port=0):
        """Initialize the dummy filter wheel."""
        
        super().__init__(device_name, port)

        # Current filter position
        self.filter_num = 0
        
        # Simulated time to change filters
        self.filter_sleep = ValueDouble("filter_sleep", "Time to change filter [s]")
        self.filter_sleep.set_writable()
        self.filter_sleep.value = 3.0
        
        # Centrald connection parameters
        self.centrald_host = "localhost"
        self.centrald_port = 617
        
        # Add filter names string for dynamic configuration
        self.filter_names = ValueString("filter_names", "filter names")
        self.filter_names.value="Clear:Red:Green:Blue:Ha:SII:OIII"

        self.set_filters(self.filter_names.value)

        #self.start()

    def on_filter_sleep_changed(self, old_value, new_value):
        return True

    def get_filter_num(self):
        """Get current filter number."""
        return self.filter_num

    def set_filter_num(self, new_filter):
        """
        Set the filter number with simulated movement time.
        
        Args:
            new_filter: New filter position
            
        Returns:
            0 on success, -1 on error
        """
        # Validate filter number
        if new_filter < 0 or new_filter >= self.filter.sel_size():
            return -1
            
        # Simulate filter movement time
        time.sleep(self.filter_sleep.value)
        
        # Update filter position
        self.filter_num = new_filter
        
        # Call parent implementation to update clients
        return super().set_filter_num(new_filter)

    def __ex_home_filter(self):
        """
        Home the filter wheel by moving to position 0.
        
        Returns:
            0 on success, -1 on error
        """
        logging.info("Homing filter wheel")
        return self.set_filter_num_mask(0)

    def home_filter(self):
        """
        Home the filter wheel by moving to position 0.

        Returns:
            0 on success, -1 on error
        """
        logging.info("Homing filter wheel")

        # Set device state to show filter is moving
        self.set_state(
            self._state | self.FILTERD_MOVE,
            "Homing filter wheel",
            self.BOP_EXPOSURE
        )

        # Simulate homing operation with timeout
        time.sleep(self.filter_sleep.value * 1.5)  # Home takes a bit longer

        # Update filter position to 0 (home)
        self.filter_num = 0
        self.filter.value = 0

        # Send updated filter value to clients
        self.network.distribute_value_immediate(self.filter)

        # Reset state
        self.set_state(
            self._state & ~self.FILTERD_MOVE,
            "Filter wheel homed",
            0
        )

        return 0


if __name__ == "__main__":
    # Create application
    app = App(description='Dummy Filter Wheel Driver')
    
    # Register device-specific options
    app.register_device_options(DummyFilter)
    
    # Parse command line arguments
    args = app.parse_args()
    
    # Create and configure device
    device = app.create_device(DummyFilter)
    
    # Run application main loop
    app.run()


