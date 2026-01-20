#!/usr/bin/env python3
"""
RTS2 Python Filter Wheel Base Class

This module provides the base Filterd class that implements the RTS2 filter wheel
interface. Like the focuser, it can be used as a standalone base or as a mixin
for multi-function devices.
"""

import time
import logging
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod

from rtspy.core.device import Device
from rtspy.core.config import DeviceConfig
from rtspy.core.constants import DeviceType
from rtspy.core.value import ValueSelection, ValueInteger, ValueString, ValueTime


class FilterMixin(DeviceConfig):
    """
    Mixin class that provides complete filter wheel functionality.

    This can be used either as a standalone base (for pure filter wheels)
    or as a mixin for multi-function devices. Includes all filter control,
    state management, and command handling.
    """

    # Filter wheel state constants
    FILTERD_MASK = 0x002
    FILTERD_IDLE = 0x000
    FILTERD_MOVE = 0x002

    def setup_filter_config(self, config):
        """Register filter wheel-specific configuration arguments."""
        config.add_argument('-F', '--filters',
                          help='Filter names (colon-separated)')
        config.add_argument('--default-filter',
                          help='Default filter', default=0)
        config.add_argument('--daytime-filter',
                          help='Daytime filter', default=0)

    def apply_filter_config(self, config: Dict[str, Any]):
        """Apply filter wheel-specific configuration."""
        # Process filters string
        filters_str = config.get('filters', 'J:H:K')  # default
        self.filter = ValueSelection("filter", "used filter", writable=True)
        self.set_filters(self.filter, filters_str)

        # Default and daytime filters
        self.default_filter = None
        default_filter_arg = config.get('default_filter')
        if default_filter_arg:
            self.default_filter = ValueSelection("def_filter", "default filter", writable=True)
            self.set_filters(self.default_filter, filters_str)
            self.default_filter.set_value_char_arr(default_filter_arg)

        self.arg_default_filter = None
        daytime_filter_arg = config.get('daytime_filter')
        if daytime_filter_arg:
            self.daytime_filter = ValueSelection("day_filter", "daytime filter", writable=True)
            self.set_filters(self.daytime_filter, filters_str)
            self.daytime_filter.set_value_char_arr(daytime_filter_arg)

        # CCD integration
        self.associated_ccd = None
        self.ccd_exposing = False

        # Movement state
        self.pending_filter_connection = None
        self.movement_in_progress = False
        self._movement_start_time = None
        self._target_filter = None

        # Store arguments for later processing
        self.arg_default_filter = config.get('default_filter')
        self.arg_daytime_filter = config.get('daytime_filter')

    def is_filter_moving(self) -> bool:
        """Check if filter wheel is currently moving."""
        return bool(self._state & self.FILTERD_MOVE)

    def on_value_changed_from_client(self, value, old_value, new_value):
        """Handle value changes from network clients."""
        try:
            if value.name == "filter":
                return self.on_filter_changed(old_value, new_value)
        except Exception as e:
            logging.error(f"Error handling filter value change: {e}")
            return -1
        return 0

    def filter_idle(self):
        """Filter wheel-specific idle processing."""
        # Override in subclasses if periodic status checking is needed
        return None

    def _register_filter_commands(self):
        """Register filter-specific command handlers with the network."""
        # Create and register filter-specific command handler
        filter_handler = FilterCommands(self)
        self.network.command_registry.register_handler(filter_handler)


    def filter_info_update(self):
        """Update filter information from hardware."""
        # Update filter position from hardware
        current_filter = self.get_filter_num()
        if current_filter != self.filter.value:
            self.filter.value = current_filter

    def script_ends_filter(self):
        """Called when a script ends - handle filter-specific cleanup."""
        if self.default_filter:
            return self.set_filter_num_mask(self.default_filter.value)
        return 0

    def on_filter_state_changed(self, old_state, new_state, message):
        """Handle device state changes for filter-specific logic."""
        logging.debug(f"Filter state changed from {old_state:x} to {new_state:x}: {message}")

        # Check for day/night transition
        # bullshit, this is in centrald state, not here and that would be done really differently
        #try:
        #    if self.daytime_filter and (new_state & 0xFF00) == 0x0400:  # DAY state
        #        self.set_filter_num_mask(self.daytime_filter.value)
        #except AttributeError:
        #    pass

    # @abstractmethod could be... 
    def set_filter_num(self, new_filter):
        """
        Set filter number (position).

        This is the method that subclasses should override to implement
        hardware-specific filter wheel control.

        Args:
            new_filter: New filter position

        Returns:
            0 on success, -1 on error
        """
        return 0

    def get_filter_num(self) -> int:
        """
        Get current filter number (position).

        Subclasses should override this to report the actual hardware position.

        Returns:
            Current filter position
        """
        return self.filter.value if self.filter.value is not None else 0

    def on_filter_changed(self, old_value, new_value):
        """
        Handle filter value change from client.

        Args:
            old_value: Previous filter position
            new_value: New filter position
        """
        # This will be called when a client changes the filter
        return self.set_filter_num_mask(new_value)

    def set_filter_num_mask(self, new_filter):
        """Set filter with appropriate state masking."""
        # Set device state to show filter is moving
        self.set_state(
            self._state | self.FILTERD_MOVE,
            "filter move started",
            self.BOP_EXPOSURE
        )

        # Log movement
        logging.info(f"moving filter from #{self.filter.value} ({self.filter.get_sel_name()}) "
                    f"to #{new_filter} ({self.filter.get_sel_name(new_filter)})")

        # Mark that movement is in progress
        self.movement_in_progress = True
        self._movement_start_time = time.time()

        # Actually move the filter
        ret = self.set_filter_num(new_filter)

        if ret == 0:
            # Record the target filter (will be used for completion)
            self._target_filter = new_filter
            # Do NOT update state or values yet - wait for movement completion
            # Return success to caller
            return ret
        else:
            # Error occurred
            self.movement_in_progress = False
            if ret == -1:
                self.set_state(self._state | self.ERROR_HW, "filter movement failed", 0)
            return ret

    def movement_completed(self):
        """Called when filter movement has completed."""
        if not self.movement_in_progress:
            return

        # Update the filter value
        if hasattr(self, '_target_filter') and self._target_filter is not None:
            self.filter.value = self._target_filter
            self._target_filter = None

        # Record movement end time
        if self._movement_start_time:
            duration = time.time() - self._movement_start_time
            logging.info(f"Filter movement completed in {duration:.1f}s to position {self.get_filter_num()}")
            self._movement_start_time = None

        # Clear the movement flag
        self.movement_in_progress = False

        # Reset device state
        self.set_state(self._state & ~(self.FILTERD_MOVE), "Filter wheel idle", 0)

        # Send response to pending command if present
        if self.pending_filter_connection:
            self.network._send_ok_response(self.pending_filter_connection)
            self.pending_filter_connection = None

    def home_filter(self):
        """
        Home the filter wheel.

        Subclasses should override this for hardware implementations.

        Returns:
            0 on success, -1 if not implemented
        """
        return -1

    def set_filters(self, filter_selval, filters_str):
        """
        Set filter names from a string.

        Args:
            filters_str: String containing filter names separated by colons

        Returns:
            0 on success, -1 on error
        """
        filter_list = []

        # Split by colon, handling quotes
        tf = filters_str

        while tf:
            # Skip leading spaces and separators
            tf = tf.lstrip(':"\' ')
            if not tf:
                break

            # Find end of filter name
            pos = tf.find(':')
            if pos == -1:
                # Last filter
                filter_list.append(tf)
                break

            # Add filter and continue
            filter_list.append(tf[:pos])
            tf = tf[pos+1:]

        # If no filters found, return error
        if not filter_list:
            return -1

        # Clear and add all filters
        filter_selval.clear_selection()
        for f in filter_list:
            filter_selval.add_sel_val(f)

        return 0

    def add_filter(self, new_filter):
        """Add a filter name to the selection."""
        self.filter.add_sel_val(new_filter)

    def get_filter_num_from_name(self, filter_name):
        """Get filter number from name."""
        return self.filter.get_sel_index(filter_name)

    def get_filter_name_from_num(self, num):
        """Get filter name from number."""
        return self.filter.get_sel_name(num)

    def get_filter_name(self, filter_num: Optional[int] = None) -> str:
        """Get filter name by number."""
        if filter_num is None:
            filter_num = self.get_filter_num()
        return self.filter.get_sel_name(filter_num)

    def send_filter_names(self):
        """Send filter names to clients."""
        self.network.update_meta_informations(self.filter)

    def should_queue_value(self, value):
        """Check if a value change should be queued."""
        # Queue value changes when filter is moving
        #if (self._state & self.FILTERD_MASK) == self.FILTERD_MOVE:
        #    return True
        return False

    def set_associated_ccd(self, ccd_name):
        """Set the associated CCD device for state monitoring."""
        self.associated_ccd = ccd_name

        # Register interest in CCD state updates
        self.network.register_state_interest(ccd_name, self._handle_ccd_state_update)
        logging.debug(f"Registered interest in state updates from CCD device {ccd_name}")

    def _handle_ccd_state_update(self, device_name, state, bop_state, message):
        """Handle state updates from the associated CCD."""
        # Check if CCD is exposing based on BOP state
        ccd_exposing = bool(bop_state & self.BOP_EXPOSURE)

        # Only take action if exposure state has changed
        if ccd_exposing != self.ccd_exposing:
            self.ccd_exposing = ccd_exposing
            logging.debug(f"CCD {device_name} exposure state changed to: {ccd_exposing}")

            if ccd_exposing:
                # CCD started exposing - ensure we don't move filter wheel
                logging.debug("Blocking filter wheel movement due to CCD exposure")
                # You might set a special state here or just use the flag
            else:
                # CCD finished exposing - filter wheel can move again
                logging.debug("Filter wheel movement unblocked")

class FilterCommands:
    """
    Handler for filter wheel-specific commands.
    """

    def __init__(self, filter_device):
        self.filter_device = filter_device
        # Map of command -> handler method
        self.handlers = {
            "filter": self.handle_filter,
            "home": self.handle_home,
            "killall": self.handle_killall,
            "killall_wse": self.handle_killall_wse,
            "script_ends": self.handle_script_ends
        }
        # Commands that need responses
        self.needs_response = {
            "filter": True,
            "home": True,
            "killall": True,
            "killall_wse": True,
            "script_ends": True
        }

    def get_commands(self):
        """Get the list of commands this handler can process."""
        return list(self.handlers.keys())

    def can_handle(self, command):
        """Check if this handler can process a command."""
        return command in self.handlers

    def needs_response_for(self, command):
        """Check if a command needs a response."""
        return self.needs_response.get(command, True)

    def handle(self, command, conn, params):
        """Dispatch to the appropriate handler method."""
        if command in self.handlers:
            return self.handlers[command](conn, params)
        return False

    def handle_filter(self, conn, params):
        """Handle 'filter' command to set filter wheel position."""
        try:
            # Parse filter number
            filter_num = int(params.strip())

            # Check if filter number is valid
            if filter_num < 0 or filter_num >= self.filter_device.filter.sel_size():
                self.filter_device.network._send_error_response(
                    conn, f"Invalid filter number: {filter_num}")
                return False

            # Set filter - don't complete the command until movement is done
            # Store the connection to respond to when movement completes
            self.filter_device.pending_filter_connection = conn

            # Start the filter movement
            ret = self.filter_device.set_filter_num_mask(filter_num)

            if ret != 0:
                # Error - send error response immediately
                self.filter_device.network._send_error_response(
                    conn, f"Error setting filter to position {filter_num}")
                self.filter_device.pending_filter_connection = None
                return False

            # No response sent yet - it will be sent when movement completes
            return True

        except ValueError:
            self.filter_device.network._send_error_response(
                conn, f"Invalid filter number: {params}")
            return False

    def handle_home(self, conn, params):
        """Handle 'home' command to home the filter wheel."""
        try:
            # Call home_filter method on the device
            ret = self.filter_device.home_filter()

            if ret == 0:
                # Success
                self.filter_device.network._send_ok_response(conn)
                return True
            elif ret == -1:
                # Not implemented
                self.filter_device.network._send_error_response(
                    conn, "Home operation not implemented for this filter wheel")
                return False
            else:
                # Other error
                self.filter_device.network._send_error_response(
                    conn, f"Error homing filter wheel")
                return False

        except Exception as e:
            logging.error(f"Error handling home command: {e}")
            self.filter_device.network._send_error_response(conn, f"Error: {str(e)}")
            return False

    def handle_killall(self, conn, params):
        """Handle 'killall' command to reset all errors and end scripts."""
        try:
            # Clear any error states
            self.filter_device.set_state(
                self.filter_device._state & ~self.filter_device.ERROR_MASK,
                "Errors cleared by killall"
            )

            # Call script_ends to perform any cleanup
            if hasattr(self.filter_device, 'script_ends_filter'):
                self.filter_device.script_ends_filter()

            # Send OK response
            self.filter_device.network._send_ok_response(conn)
            return True

        except Exception as e:
            logging.error(f"Error handling killall command: {e}")
            self.filter_device.network._send_error_response(conn, f"Error: {str(e)}")
            return False

    def handle_killall_wse(self, conn, params):
        """Handle 'killall_wse' command to reset errors without calling script_ends."""
        try:
            # Clear any error states without calling script_ends
            self.filter_device.set_state(
                self.filter_device._state & ~self.filter_device.ERROR_MASK,
                "Errors cleared by killall_wse"
            )

            # Send OK response
            self.filter_device.network._send_ok_response(conn)
            return True

        except Exception as e:
            logging.error(f"Error handling killall_wse command: {e}")
            self.filter_device.network._send_error_response(conn, f"Error: {str(e)}")
            return False

    def handle_script_ends(self, conn, params):
        """Handle 'script_ends' command to notify device that script execution has ended."""
        try:
            # Call script_ends method on the device
            ret = 0
            if hasattr(self.filter_device, 'script_ends_filter'):
                ret = self.filter_device.script_ends_filter()

            if ret == 0:
                # Success
                self.filter_device.network._send_ok_response(conn)
                return True
            else:
                # Error
                self.filter_device.network._send_error_response(
                    conn, f"Error in script_ends handler")
                return False

        except Exception as e:
            logging.error(f"Error handling script_ends command: {e}")
            self.filter_device.network._send_error_response(conn, f"Error: {str(e)}")
            return False


class Filterd(Device, FilterMixin):
    """
    RTS2 Filter Wheel Device - standalone filter wheel implementation.

    This class combines the Device base with FilterMixin to create
    a complete filter wheel device.
    """

    def setup_config(self, config):
        """Set up filter wheel configuration."""
        # super().setup_config(config)
        self.setup_filter_config(config)

    def __init__(self, device_name="W0", port=0):
        """Initialize filter wheel device."""
        super().__init__(device_name, DeviceType.FW, port)

    def apply_config(self, config: Dict[str, Any]):
        """Apply configuration."""
        super().apply_config(config)
        self.apply_filter_config(config)

    def _register_device_commands(self):
        """Register device command handlers with the network."""
        # Create and register the device command handler from parent
        super()._register_device_commands()

        # Register filter-specific commands
        self._register_filter_commands()

    def start(self):
        """Start the filter wheel device."""
        super().start()

        logging.info(f"Filter wheel {self.device_name} started with {self.filter.sel_size()} positions")

        # Set initial state
        self.set_state(self.FILTERD_IDLE, "Filter wheel ready")

    def info(self):
        """Update filter wheel information - override in subclasses."""
        super().info()
        self.filter_info_update()

    def idle(self):
        """Device idle processing."""
        # Call parent idle first
        result = super().idle()

        # Then filter-specific idle
        filter_result = self.filter_idle()

        # Return the most restrictive timing
        if filter_result is not None:
            if result is None:
                return filter_result
            else:
                return min(result, filter_result)
        return result

    def script_ends(self):
        """Called when a script ends."""
        result = super().script_ends()
        filter_result = self.script_ends_filter()

        # Return error if either failed
        if result != 0:
            return result
        return filter_result

    def on_state_changed(self, old_state, new_state, message):
        """Handle device state changes."""
        super().on_state_changed(old_state, new_state, message)
        self.on_filter_state_changed(old_state, new_state, message)
