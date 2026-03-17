#!/usr/bin/python3

import time
import logging
import threading
from typing import Optional,Dict,Any

from rtspy.core.value import ValueString, ValueDouble
from rtspy.core.filterd import Filterd
from rtspy.core.app import App

class DummyFilter(Filterd):
    """Dummy filter wheel implementation for testing.
    This simulates a filter wheel with configurable filters and movement time."""

    def setup_config(self, config):
        """Register DummyFilter-specific command line options."""
        super().setup_config(config)
        config.add_argument('-s', '--sleep', type=float, default=1.0,
                          help='Filter movement time in seconds')

    def apply_config(self, config: Dict[str, Any]):
        """Process arguments for this specific device."""
        super().apply_config(config)
        self.filter_sleep.value = config.get('sleep', 1.0)

    def __init__(self, device_name="W0", port=0):
        """Initialize the dummy filter wheel."""

        super().__init__(device_name, port)

        # Current filter position
        self.filter_num = 0

        # Simulated time to change filters
        self.filter_sleep = ValueDouble("filter_sleep", "Time to change filter [s]", writable=True, initial=1.0)

        # Centrald connection parameters
        self.centrald_host = "localhost"
        self.centrald_port = 617

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

        def move_filter():
            # Simulate filter movement time
            time.sleep(self.filter_sleep.value)
            # Update filter position
            self.filter_num = new_filter
            self.movement_completed()
        threading.Thread(target=move_filter, daemon=True).start()

        # Call parent implementation to update clients
        return super().set_filter_num(new_filter)

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


def main():
    """Entry point for rts2-filterd-dummy daemon."""
    # Create application
    app = App(description='Dummy Filter Wheel Driver')

    # Register device-specific options
    app.register_device_options(DummyFilter)

    # Parse command line arguments
    args = app.parse_args()

    # Create and configure device
    device = app.create_device(DummyFilter)

    # Show config summary if debug enabled
    if getattr(args, 'debug', False):
        print("\nDummyFilter Configuration Summary:")
        print("=" * 50)
        print(device.get_config_summary())
        print("=" * 50)

    # Run application main loop
    app.run()


if __name__ == "__main__":
    main()


