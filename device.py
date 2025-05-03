from enum import IntFlag, Enum, auto
from typing import Dict, List, Optional, Set, Any, Tuple, Union
import socket
import select
import logging
import os
import time
import errno
import signal
import fcntl
import math
from dataclasses import dataclass

from value import ValueTime
from netman import NetworkManager

class Device:
    """
    Base class for RTS2 devices with integrated NetworkManager.

    This provides a single integrated interface for hardware drivers
    to interact with the RTS2 network without caring about the details.
    """

    _instance = None  # Singleton instance

    # RTS2 Device states
    STATE_IDLE = 0x000
    STATE_RUNNING = 0x001
    STATE_EXPOSING = 0x002

    # Error bits
    ERROR_HW = 0x00020000
    ERROR_MASK = 0x000f0000
    ERROR_KILL = 0x00010000

    # Not ready bit
    NOT_READY = 0x00040000

    # Block operation bits
    BOP_EXPOSURE = 0x01000000
    BOP_READOUT = 0x02000000
    BOP_TEL_MOVE = 0x04000000
    BOP_WILL_EXPOSE = 0x08000000
    BOP_TRIG_EXPOSE = 0x10000000

    # Weather state
    GOOD_WEATHER = 0x00000000
    BAD_WEATHER = 0x80000000
    WEATHER_MASK = 0x80000000

    # Stop mask
    STOP_EVERYTHING = 0x40000000
    CAN_MOVE = 0x00000000
    STOP_MASK = 0x40000000

    # Device blocking bits
    DEVICE_BLOCK_OPEN = 0x00002000
    DEVICE_BLOCK_CLOSE = 0x00004000

    # Weather reason bits
    WR_RAIN = 0x00100000
    WR_WIND = 0x00200000
    WR_HUMIDITY = 0x00400000
    WR_CLOUD = 0x00800000

    @classmethod
    def get_instance(cls):
        """Get the singleton device instance."""
        return cls._instance

    def __init__(self, device_name, device_type, port=0):
        """Initialize the device."""
        # Set singleton instance
        if Device._instance is not None:
            logging.warning("Creating multiple Device instances is not recommended")
        Device._instance = self

        self.device_name = device_name
        self.device_type = device_type

        # Create network manager
        self.network = NetworkManager(device_name, device_type)

        # Set callback handlers
        self.network.state_changed_callback = self._on_state_changed
        self.network.info_callback = self.info
        self.network.centrald_connected_callback = self._on_centrald_connected

        # Value queuing system
        self.queued_values = {}

        # Initialize in NOT_READY state
        self._state = self.STATE_IDLE # | self.NOT_READY
        self._bop_state = 0

        # Track expected progress times
        self.state_start = float('nan')
        self.state_expected_end = float('nan')

        # Weather timeout
        self.weather_timeout = 0
        self.weather_reason = None

         # Register device command handlers - this should happen after network initialization
        self._register_device_commands()

        # Create mandatory system values
        self.infotime = ValueTime("infotime", "time of last update")
        self.uptime = ValueTime("uptime", "daemon uptime")
        self.infotime.value = time.time()
        self.uptime.value = time.time()


    def _register_device_commands(self):
        """Register device command handlers with the network."""
        # Create and register the device command handler
        device_handler = DeviceCommands(self.network)
        self.network.command_registry.register_handler(device_handler)

    def start(self):
        """Start the device network services."""
        # not here, will be started in App
        # self.network.start()

        # Set initial state
        logging.info(f"Device {self.device_name} started")
        self.set_state(self.STATE_IDLE, "Initializing")

    def stop(self):
        """Stop the device."""
        self.network.stop()

    def _on_centrald_connected(self, conn_id):
        """Called when a centrald connection is established and authenticated."""
        # Now we can send our status and metadata
        logging.debug("Connected to centrald, sending device status")

        # Send state after connection
        # This should match the C++ behavior
        self.set_state(self._state) # , "Connected to centrald")

        # Set BOP state
        self.set_full_bop_state(self._bop_state)

    def _on_state_changed(self, old_state, new_state, message):
        """Handle device state changes."""
        # Check queued values that might now be executable
        self.check_queued_values()

        # Call user-defined state changed handler
        self.on_state_changed(old_state, new_state, message)

    def on_state_changed(self, old_state, new_state, message):
        """State change handler - override in subclasses."""
        pass

    def set_state(self, new_state, description=None, new_bop=None):
        """
        Set device state with optional BOP state.

        Args:
            new_state: The new state to set
            description: Optional text description of the state change
            new_bop: Optional BOP state to set (if None, BOP state remains unchanged)
        """
        logging.debug(f"Device.set_state({new_state:x}, '{description}', {new_bop if new_bop is not None else 'None'})")

        # Store old states for notifications
        old_state = self._state

        # Update internal state
        self._state = new_state

        # Process any queued values first - these will be sent immediately
        # This ensures all value changes happen before state changes
        self.check_queued_values()

        # Update BOP state if provided
        if new_bop is not None:
            self.set_full_bop_state(new_bop)  # This will handle BOP state changes
        else:
            # Only state changed - use S command
            self.network.set_device_state(new_state, description)

        # Check queued values that might now be executable
        self.check_queued_values()

        # Call user-defined state changed handler if state changed
        #if old_state != new_state:
        #    self.on_state_changed(old_state, new_state, description)

    def set_ready(self, message="Device ready"):
        """Set device ready."""
        if self._state & self.NOT_READY:
            self.set_state(self._state & ~self.NOT_READY, message)

    def set_full_bop_state(self, new_bop_state):
        """
        Set the block operation state.

        This is a special state used for coordinating operations between
        different devices in RTS2.
        """
        # Skip if BOP state hasn't changed
        if self._bop_state == new_bop_state:
            return

        # Adjust BOP state for queued values
        for value, op, new_value in self.queued_values.values():
            if hasattr(self, 'mask_que_value_bop_state'):
                new_bop_state = self.mask_que_value_bop_state(new_bop_state, value.get_que_condition())

        # Store old state values
        old_state = self._state
        old_bop = self._bop_state

        # Update internal BOP state
        self._bop_state = new_bop_state

        # Propagate to network - use B command for combined state + BOP update
        self.network.set_bop_state(self._state, new_bop_state)

        # Check queued values now that BOP state has changed
        self.check_queued_values()

    def register_value(self, value):
        """Register a value with the device."""
        self.network.register_value(value)

    def unregister_value(self, value_name):
        """Unregister a value."""
        self.network.unregister_value(value_name)

    def distribute_value(self, value):
        """Distribute a value to all connections."""
        if value.need_send():
            self.network.broadcast_value(value)
            value.reset_need_send()

    def connect_to_centrald(self, host, port):
        """Connect to a centrald server."""
        return self.network.connect_to_centrald(host, port)

    def should_queue_value(self, value):
        """
        Check if a value change should be queued.

        This method should be overridden by subclasses to implement
        queuing logic based on device state.
        """
        # Default implementation - can be overridden by subclasses
        # For example, queue changes when device is busy
        return False

    def queue_value_change(self, value, op, new_value):
        """Queue a value change."""
        key = value.name
        self.queued_values[key] = (value, op, new_value)

    def check_queued_values(self):
        """Check queued values that may now be executable."""
        # Process all queued values
        keys_to_remove = []

        for key, (value, op, new_value) in self.queued_values.items():
            # Check if we can apply the change now
            if not self.should_queue_value(value):
                # Apply the change
                try:
                    value._value = new_value
                    value.changed()
                    self.distribute_value(value)
                    keys_to_remove.append(key)
                except Exception as e:
                    logging.error(f"Error updating queued value {value.name}: {e}")

        # Remove processed values
        for key in keys_to_remove:
            del self.queued_values[key]

    def _handle_info_command(self, conn, params):
        """Handle 'info' command."""
        try:
            # Call info method to update values
            self.info()

            # Send OK response - the values will be sent by network manager
            self.network._send_ok_response(conn)
            return True
        except Exception as e:
            logging.error(f"Error handling info command: {e}", exc_info=True)
            self.network._send_error_response(conn, f"Error: {str(e)}")
            return False

    def _handle_base_info(self, conn, params):
        """Handle 'base_info' command."""
        try:
            # Base info is related to sending constant values
            # Send OK response - the network manager will handle sending values
            self.network._send_ok_response(conn)
            return True
        except Exception as e:
            logging.error(f"Error handling base_info command: {e}", exc_info=True)
            self.network._send_error_response(conn, f"Error: {str(e)}")
            return False

    def _handle_status_info(self, conn, params):
        """Handle 'status_info' command."""
        # Send current device state
        self.network._send_status(conn)
        self.network._send_ok_response(conn)
        return True

    def on_value_changed_from_client(self, value, old_value, new_value):
        """
        Called when a value is changed by a client command.

        This method provides a hook for device implementations to react
        to value changes from the network. This is particularly useful
        for action values that should trigger hardware actions.

        Args:
            value: The Value object that changed
            old_value: Previous value
            new_value: New value
        """
        # Log the change
        logging.debug(f"Value {value.name} changed from {old_value} to {new_value} by client")

        # Call device-specific handler if available
        method_name = f"on_{value.name.lower()}_changed"
        if hasattr(self, method_name) and callable(getattr(self, method_name)):
            try:
                handler = getattr(self, method_name)
                handler(old_value, new_value)
            except Exception as e:
                logging.error(f"Error in value change handler for {value.name}: {e}")

    def info(self):
        """
        Update device information.

        This method should be overridden by subclasses to update
        device values before they are sent to clients.
        """
        pass

class DeviceCommands:
    """
    Handler for device-related commands (info, base_info, etc.).

    This class handles all commands related to device information and status.
    """

    def __init__(self, network_manager):
        self.network = network_manager
        # Map of command -> handler method
        self.handlers = {
            "info": self.handle_info,
            "base_info": self.handle_base_info,
            "device_status": self.handle_device_status
        }
        # Commands that need responses
        self.needs_response = {
            "info": True,
            "base_info": True,
            "device_status": True
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

    def handle_info(self, conn, params):
        """Handle 'info' command."""
        try:
            # Update device information if callback is defined
            if hasattr(self.network, 'info_callback') and callable(self.network.info_callback):
                self.network.info_callback()

            self.network.values['infotime']._value = time.time()
            logging.info(f"{self.network.values}")

            # Send all values
            with self.network._lock:
                for value_name, value in self.network.values.items():
                    self.network._send_value(conn, value)

            # Send current state
            self.network._send_status(conn)

            return True

        except Exception as e:
            logging.error(f"Error handling info command: {e}", exc_info=True)
            return False

    def handle_base_info(self, conn, params):
        """Handle 'base_info' command."""
        try:
            # Base info is related to sending constant values
            return True
        except Exception as e:
            logging.error(f"Error handling base_info command: {e}", exc_info=True)
            return False

    def handle_device_status(self, conn, params):
        """Handle 'device_status' command."""
        # Send current device state
        self.network._send_status(conn)
        return True

