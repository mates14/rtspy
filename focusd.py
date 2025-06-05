#!/usr/bin/env python3
"""
RTS2 Python Focuser Base Class

This module provides the base Focusd class that implements the RTS2 focuser
interface, compatible with the C++ implementation but following Python
RTS2 framework standards.

The class can be used as a base for specific focuser implementations or
as a mixin for multi-function devices.
"""

import time
import logging
import math
from typing import Dict, Any, Optional, Callable
from abc import ABC, abstractmethod

from device import Device
from config import DeviceConfig
from constants import DeviceType
from value import ValueDouble, ValueBool, ValueString, ValueInteger, ValueSelection


class FocuserMixin(DeviceConfig):
    """
    Mixin class that provides complete focuser functionality.
    
    This can be used either as a standalone base (for pure focusers)
    or as a mixin for multi-function devices. Includes all focus control,
    state management, and command handling.
    """
    
    # Focuser state constants (matching C++ implementation)
    FOC_SLEEPING = 0x0000
    FOC_FOCUSING = 0x0001
    FOC_MASK_FOCUSING = 0x0001
    
    def setup_focuser_config(self, config):
        """Register focuser-specific configuration arguments."""
        config.add_argument('--start-position', type=float, 
                          help='Focuser start position', section='focuser')
        config.add_argument('--foc-min', type=float, 
                          help='Minimum focuser position', section='focuser')
        config.add_argument('--foc-max', type=float, 
                          help='Maximum focuser position', section='focuser')
        config.add_argument('--foc-speed', type=float, default=1.0,
                          help='Focuser speed (steps/sec)', section='focuser')
        config.add_argument('--temperature-variable', 
                          help='External temperature variable (DEVICE.VARIABLE)', 
                          section='focuser')
    
    def apply_focuser_config(self, config: Dict[str, Any]):
        """Apply focuser-specific configuration."""
        # Initialize focuser values first
        self.init_focuser_values()
        
        # Apply speed
        if config.get('foc_speed'):
            self.foc_speed.value = config['foc_speed']
            
        # Apply position limits
        foc_min = config.get('foc_min')
        foc_max = config.get('foc_max')
        if foc_min is not None and foc_max is not None:
            self.set_focus_extent(foc_min, foc_max)
            
        # Apply start position
        start_pos = config.get('start_position')
        if start_pos is not None:
            self.foc_def.value = start_pos
            
        # Set up external temperature if specified
        temp_var = config.get('temperature_variable')
        if temp_var:
            self.create_external_temperature(temp_var)
            
        # Set initial focuser type if not set by subclass
        if not hasattr(self, 'focuser_type'):
            self.focuser_type = "Generic"
            
    def init_focuser_values(self):
        """Initialize focuser values."""
        # Core focuser values (matching C++ interface)
        self.foc_pos = ValueDouble("FOC_POS", "focuser position", write_to_fits=True)
        self.foc_tar = ValueDouble("FOC_TAR", "focuser target position", 
                                 write_to_fits=True, writable=True)
        self.foc_def = ValueDouble("FOC_DEF", "default target value", 
                                 write_to_fits=True, writable=True)
        
        # Offset values
        self.foc_filteroff = ValueDouble("FOC_FILTEROFF", "offset related to actual filter", 
                                       write_to_fits=True, writable=True, initial=0.0)
        self.foc_foff = ValueDouble("FOC_FOFF", "offset from focusing routine", 
                                  write_to_fits=True, writable=True, initial=0.0)
        self.foc_toff = ValueDouble("FOC_TOFF", "temporary offset for focusing", 
                                  write_to_fits=True, writable=True, initial=0.0)
        
        # Speed and type
        self.foc_speed = ValueDouble("foc_speed", "[steps/sec] speed of focuser movement", 
                                   write_to_fits=False, initial=1.0)
        self.foc_type = ValueString("FOC_TYPE", "focuser type", write_to_fits=False)
        
        # Optional temperature (created by create_temperature if needed)
        self.foc_temp = None
        
        # Linear offset parameters (for temperature compensation)
        self.linear_offset = None
        self.linear_slope = None
        self.linear_intercept = None
        self.linear_nightonly = None
        
        # External temperature monitoring
        self.ext_temp_device = None
        self.ext_temp_variable = None
        
        # Internal state
        self._focusing_start_time = None
        self._focus_timeout = 60.0  # Default timeout
        self._target_position = None
        self._movement_in_progress = False
        self.pending_focus_connection = None
        
        # Initialize default values
        self.foc_pos.value = 0.0
        self.foc_tar.value = 0.0
        self.foc_def.value = float('nan')  # Will be set during initialization
        
    def create_temperature(self):
        """Create temperature value for internal sensor."""
        if self.foc_temp is None:
            self.foc_temp = ValueDouble("FOC_TEMP", "focuser temperature", write_to_fits=True)
            
    def create_external_temperature(self, variable_spec: str):
        """
        Set up external temperature monitoring.
        
        Args:
            variable_spec: Format "DEVICE.VARIABLE" (e.g., "TEMP.OUT")
        """
        try:
            device_name, var_name = variable_spec.split('.', 1)
            
            # Store for connection management
            self.ext_temp_device = device_name
            self.ext_temp_variable = var_name
            
            # Create temperature value
            self.create_temperature()
            
            # Create linear offset values as well
            self.create_linear_offset()
            
            # Register interest in the external temperature value
            if hasattr(self, 'network'):
                self.network.register_interest_in_value(
                    device_name, var_name, self._on_external_temperature_update
                )
                
            logging.info(f"Monitoring external temperature: {variable_spec}")
            
        except ValueError:
            logging.error(f"Invalid temperature variable format: {variable_spec}. Use DEVICE.VARIABLE")
            
    def create_linear_offset(self):
        """Create linear temperature compensation values."""
        self.linear_offset = ValueBool("linear_offset", "linear offset", 
                                     write_to_fits=False, writable=True, initial=False)
        self.linear_slope = ValueDouble("linear_slope", 
                                      "slope parameter for linear function to fit temperature sensor", 
                                      write_to_fits=False, writable=True, initial=float('nan'))
        self.linear_intercept = ValueDouble("linear_intercept", 
                                          "intercept parameter for 0 value of temperature sensor", 
                                          write_to_fits=False, writable=True, initial=float('nan'))
        self.linear_nightonly = ValueBool("linear_nightonly", 
                                        "performs temperature based focusing only during night", 
                                        write_to_fits=False, writable=True, initial=True)
                                        
    def _on_external_temperature_update(self, temperature_str: str):
        """Handle external temperature updates."""
        try:
            if self.foc_temp:
                temp_value = float(temperature_str)
                old_temp = self.foc_temp.value
                self.foc_temp.value = temp_value
                logging.debug(f"External temperature updated: {old_temp} -> {temp_value}")
        except ValueError:
            logging.warning(f"Invalid temperature value: {temperature_str}")
            
    def set_focus_extent(self, foc_min: float, foc_max: float):
        """Set focuser position limits."""
        logging.info(f"Focus extent set: {foc_min} to {foc_max}")
        # Store limits for validation
        self._focus_min = foc_min
        self._focus_max = foc_max
        
        # Update offset extents based on new limits
        self.update_offsets_extent()
        
    def update_offsets_extent(self):
        """Update offset value limits based on target position and focus extent."""
        if not hasattr(self, '_focus_min') or not hasattr(self, '_focus_max'):
            return
            
        current_target = self.foc_tar.value
        target_diff_min = self._focus_min - current_target
        target_diff_max = self._focus_max - current_target
        
        # Update each offset's allowable range
        # This ensures that target + offsets stays within focus limits
        for offset_val in [self.foc_filteroff, self.foc_foff, self.foc_toff]:
            if offset_val:
                current_offset = offset_val.value
                # Calculate new min/max for this offset
                new_min = target_diff_min + current_offset
                new_max = target_diff_max + current_offset
                logging.debug(f"Updated {offset_val.name} extent: [{new_min:.1f}, {new_max:.1f}]")
        
    def get_position(self) -> float:
        """Get current focuser position."""
        return self.foc_pos.value if self.foc_pos.value is not None else 0.0
        
    def get_target(self) -> float:
        """Get target focuser position.""" 
        return self.foc_tar.value if self.foc_tar.value is not None else 0.0
        
    def tc_offset(self) -> float:
        """
        Temperature compensation offset.
        Override in subclasses if needed.
        """
        return 0.0
        
    def estimate_offset_duration(self, offset: float) -> float:
        """Estimate time needed for focusing movement."""
        if self.foc_speed.value > 0:
            return abs(offset) / self.foc_speed.value
        return 0.0
        
    def set_position(self, target: float) -> int:
        """
        Move focuser to target position.
        
        Args:
            target: Target position
            
        Returns:
            0 on success, -1 on error
        """
        try:
            # Check position limits
            if hasattr(self, '_focus_min') and not math.isnan(self._focus_min) and target < self._focus_min:
                logging.error(f"Target position {target} below minimum {self._focus_min}")
                return -1
                
            if hasattr(self, '_focus_max') and not math.isnan(self._focus_max) and target > self._focus_max:
                logging.error(f"Target position {target} above maximum {self._focus_max}")
                return -1
            
            # Update target value
            self.foc_tar.value = target
            
            # Set focusing state with progress estimation
            duration = self.estimate_offset_duration(abs(target - self.get_position()))
            self.set_state(
                self._state | self.FOC_FOCUSING,
                f"Moving to position {target}",
                self.BOP_EXPOSURE
            )
            
            # Set progress information if duration is available
            if duration > 0:
                start_time = time.time()
                self.set_progress_state(
                    self._state | self.FOC_FOCUSING,
                    start_time,
                    start_time + duration,
                    f"Moving to position {target}"
                )
            
            # Store target and start time
            self._target_position = target
            self._focusing_start_time = time.time()
            self._movement_in_progress = True
            
            # Call hardware-specific movement function
            result = self.set_to(target)
            
            if result != 0:
                # Movement failed
                self._movement_in_progress = False
                self.set_state(
                    self._state & ~self.FOC_FOCUSING | self.ERROR_HW,
                    "Focus movement failed"
                )
                return -1
                
            # Update offset extents after target change
            self.update_offsets_extent()
                
            logging.info(f"Focuser movement started to position {target}")
            return 0
            
        except Exception as e:
            logging.error(f"Error setting focuser position: {e}")
            self._movement_in_progress = False
            self.set_state(
                self._state & ~self.FOC_FOCUSING | self.ERROR_HW,
                f"Focus error: {e}"
            )
            return -1
            
    @abstractmethod
    def set_to(self, position: float) -> int:
        """
        Hardware-specific position setting.
        
        Args:
            position: Target position
            
        Returns:
            0 on success, non-zero on error
        """
        pass
        
    @abstractmethod
    def is_at_start_position(self) -> bool:
        """Check if focuser is at start/home position."""
        pass
        
    def is_focusing(self) -> int:
        """
        Check focusing status.
        
        Returns:
            >= 0: Still focusing (seconds to wait)
            -1: Error occurred
            -2: Focusing completed
        """
        # Check timeout
        if self._focusing_start_time:
            elapsed = time.time() - self._focusing_start_time
            if elapsed > self._focus_timeout:
                logging.error(f"Focus operation timed out after {elapsed:.1f} seconds")
                return -1
                
        # Update position from hardware
        try:
            self.info()
        except Exception as e:
            logging.error(f"Error updating focuser info: {e}")
            return -1
            
        # Check if target reached
        if self._target_position is not None:
            current_pos = self.get_position()
            # Use a small tolerance for position comparison
            position_tolerance = 0.1
            if abs(current_pos - self._target_position) <= position_tolerance:
                return -2  # Completed
                
        return 1  # Still moving, check again in 1 second
        
    def end_focusing(self) -> int:
        """Complete focusing operation."""
        self._focusing_start_time = None
        self._target_position = None
        self._movement_in_progress = False
        
        # Update state
        self.set_state(
            self._state & ~self.FOC_FOCUSING,
            "Focusing completed"
        )
        
        # Send response to pending command if present
        if self.pending_focus_connection:
            self.network._send_ok_response(self.pending_focus_connection)
            self.pending_focus_connection = None
        
        current_pos = self.get_position()
        logging.info(f"Focuser moved to {current_pos}")
        return 0
        
    def script_ends_focuser(self):
        """Called when script ends - reset temporary offset."""
        if hasattr(self, 'foc_toff'):
            self.foc_toff.value = 0.0
            
        # Move to default position plus offsets
        total_offset = (
            self.foc_def.value +
            self.foc_filteroff.value +
            self.foc_foff.value +
            self.foc_toff.value +
            self.tc_offset()
        )
        
        # Only move if position is valid
        if not math.isnan(total_offset):
            result = self.set_position(total_offset)
            return 0 if result == 0 else -1
        return 0
        
    def on_value_changed_from_client(self, value, old_value, new_value):
        """Handle value changes from network clients."""
        try:
            if value.name == "FOC_TAR":
                return self.set_position(new_value)
            elif value.name == "FOC_DEF":
                total_offset = (
                    new_value +
                    self.foc_filteroff.value +
                    self.foc_foff.value +
                    self.foc_toff.value +
                    self.tc_offset()
                )
                return self.set_position(total_offset)
            elif value.name == "FOC_FILTEROFF":
                total_offset = (
                    self.foc_def.value +
                    new_value +
                    self.foc_foff.value +
                    self.foc_toff.value +
                    self.tc_offset()
                )
                return self.set_position(total_offset)
            elif value.name == "FOC_FOFF":
                total_offset = (
                    self.foc_def.value +
                    self.foc_filteroff.value +
                    new_value +
                    self.foc_toff.value +
                    self.tc_offset()
                )
                return self.set_position(total_offset)
            elif value.name == "FOC_TOFF":
                total_offset = (
                    self.foc_def.value +
                    self.foc_filteroff.value +
                    self.foc_foff.value +
                    new_value +
                    self.tc_offset()
                )
                return self.set_position(total_offset)
            elif value.name == "FOC_POS":
                # Direct position write (if allowed)
                return self.write_position(new_value)
        except Exception as e:
            logging.error(f"Error handling focuser value change: {e}")
            return -1
        return 0
        
    def write_position(self, new_position: float) -> int:
        """
        Write position directly to focuser hardware.
        
        This should only be called if position is made writable.
        Override in subclasses that support direct position writing.
        
        Args:
            new_position: New position value
            
        Returns:
            0 on success, -1 if not supported
        """
        return -1  # Not supported by default
        
    def focuser_idle(self):
        """Focuser-specific idle processing."""
        # Handle ongoing focusing
        if self._state & self.FOC_FOCUSING:
            result = self.is_focusing()
            
            if result >= 0:
                # Still focusing - schedule next check
                return result
            else:
                # Focusing finished or error
                if result == -2:
                    # Completed successfully
                    self.end_focusing()
                else:
                    # Error occurred
                    self._movement_in_progress = False
                    self.set_state(
                        self._state & ~self.FOC_FOCUSING | self.ERROR_HW,
                        "Focusing failed"
                    )
                    
                    # Send error response to pending command if present
                    if self.pending_focus_connection:
                        self.network._send_error_response(self.pending_focus_connection, "Focusing failed")
                        self.pending_focus_connection = None
                        
        # Handle linear temperature compensation
        if (self.linear_offset and self.linear_slope and self.linear_intercept and 
            self.foc_temp and self.linear_nightonly):
            
            if (self.linear_offset.value and
                not math.isnan(self.linear_slope.value) and
                not math.isnan(self.linear_intercept.value) and
                not math.isnan(self.foc_temp.value)):
                
                # Check if we should apply temperature compensation
                should_compensate = True
                if self.linear_nightonly.value:
                    # TODO: Check if system is in night mode
                    # This would require checking centrald state
                    # should_compensate = (getMasterState() == (SERVERD_NIGHT | SERVERD_ON))
                    pass
                    
                if should_compensate:
                    # Calculate new position based on temperature
                    new_pos = (self.linear_slope.value * self.foc_temp.value + 
                             self.linear_intercept.value)
                    
                    current_pos = self.get_position()
                    if abs(new_pos - current_pos) > 0.1:
                        logging.info(f"Temperature compensation: moving from {current_pos} "
                                   f"to {new_pos} at temperature {self.foc_temp.value}")
                        self.set_position(new_pos)
                        
        return None  # No specific idle timing needed

    def _register_focuser_commands(self):
        """Register focuser-specific command handlers with the network."""
        # Create and register focuser-specific command handler
        focuser_handler = FocuserCommands(self)
        self.network.command_registry.register_handler(focuser_handler)

    def should_queue_value(self, value):
        """Check if a value change should be queued."""
        # Queue value changes when focuser is moving
        if (self._state & self.FOC_MASK_FOCUSING) == self.FOC_FOCUSING:
            return True
        return False

    def focuser_info_update(self):
        """Update focuser information from hardware."""
        # Subclasses should override this to update foc_pos and foc_temp
        # from hardware
        pass


class FocuserCommands:
    """
    Handler for focuser-specific commands.
    """

    def __init__(self, focuser_device):
        self.focuser_device = focuser_device
        # Map of command -> handler method
        self.handlers = {
            "move": self.handle_move,
            "step": self.handle_step,
            "home": self.handle_home,
            "killall": self.handle_killall,
            "killall_wse": self.handle_killall_wse,
            "script_ends": self.handle_script_ends
        }
        # Commands that need responses
        self.needs_response = {
            "move": True,
            "step": True,
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

    def handle_move(self, conn, params):
        """Handle 'move' command to set focuser position."""
        try:
            # Parse target position
            target = float(params.strip())

            # Store the connection to respond to when movement completes
            self.focuser_device.pending_focus_connection = conn

            # Start the focuser movement
            ret = self.focuser_device.set_position(target)

            if ret != 0:
                # Error - send error response immediately
                self.focuser_device.network._send_error_response(
                    conn, f"Error moving focuser to position {target}")
                self.focuser_device.pending_focus_connection = None
                return False

            # No response sent yet - it will be sent when movement completes
            return True

        except ValueError:
            self.focuser_device.network._send_error_response(
                conn, f"Invalid position: {params}")
            return False

    def handle_step(self, conn, params):
        """Handle 'step' command for relative movement."""
        try:
            # Parse step size (positive or negative)
            step = float(params.strip())
            
            # Calculate new target position
            current_pos = self.focuser_device.get_position()
            new_target = current_pos + step

            # Store the connection to respond to when movement completes
            self.focuser_device.pending_focus_connection = conn

            # Start the focuser movement
            ret = self.focuser_device.set_position(new_target)

            if ret != 0:
                # Error - send error response immediately
                self.focuser_device.network._send_error_response(
                    conn, f"Error stepping focuser by {step}")
                self.focuser_device.pending_focus_connection = None
                return False

            # No response sent yet - it will be sent when movement completes
            return True

        except ValueError:
            self.focuser_device.network._send_error_response(
                conn, f"Invalid step size: {params}")
            return False

    def handle_home(self, conn, params):
        """Handle 'home' command to home the focuser."""
        try:
            # Check if focuser supports homing
            if not hasattr(self.focuser_device, 'home_focuser'):
                self.focuser_device.network._send_error_response(
                    conn, "Home operation not implemented for this focuser")
                return False

            # Call home_focuser method on the device
            ret = self.focuser_device.home_focuser()

            if ret == 0:
                # Success
                self.focuser_device.network._send_ok_response(conn)
                return True
            elif ret == -1:
                # Not implemented
                self.focuser_device.network._send_error_response(
                    conn, "Home operation not implemented for this focuser")
                return False
            else:
                # Other error
                self.focuser_device.network._send_error_response(
                    conn, f"Error homing focuser")
                return False

        except Exception as e:
            logging.error(f"Error handling home command: {e}")
            self.focuser_device.network._send_error_response(conn, f"Error: {str(e)}")
            return False

    def handle_killall(self, conn, params):
        """Handle 'killall' command to reset all errors and end scripts."""
        try:
            # Clear any error states
            self.focuser_device.set_state(
                self.focuser_device._state & ~self.focuser_device.ERROR_MASK,
                "Errors cleared by killall"
            )

            # Call script_ends to perform any cleanup
            if hasattr(self.focuser_device, 'script_ends_focuser'):
                self.focuser_device.script_ends_focuser()

            # Send OK response
            self.focuser_device.network._send_ok_response(conn)
            return True

        except Exception as e:
            logging.error(f"Error handling killall command: {e}")
            self.focuser_device.network._send_error_response(conn, f"Error: {str(e)}")
            return False

    def handle_killall_wse(self, conn, params):
        """Handle 'killall_wse' command to reset errors without calling script_ends."""
        try:
            # Clear any error states without calling script_ends
            self.focuser_device.set_state(
                self.focuser_device._state & ~self.focuser_device.ERROR_MASK,
                "Errors cleared by killall_wse"
            )

            # Send OK response
            self.focuser_device.network._send_ok_response(conn)
            return True

        except Exception as e:
            logging.error(f"Error handling killall_wse command: {e}")
            self.focuser_device.network._send_error_response(conn, f"Error: {str(e)}")
            return False

    def handle_script_ends(self, conn, params):
        """Handle 'script_ends' command to notify device that script execution has ended."""
        try:
            # Call script_ends method on the device
            ret = 0
            if hasattr(self.focuser_device, 'script_ends_focuser'):
                ret = self.focuser_device.script_ends_focuser()

            if ret == 0:
                # Success
                self.focuser_device.network._send_ok_response(conn)
                return True
            else:
                # Error
                self.focuser_device.network._send_error_response(
                    conn, f"Error in script_ends handler")
                return False

        except Exception as e:
            logging.error(f"Error handling script_ends command: {e}")
            self.focuser_device.network._send_error_response(conn, f"Error: {str(e)}")
            return False


class Focusd(Device, FocuserMixin):
    """
    RTS2 Focuser Device - standalone focuser implementation.
    
    This class combines the Device base with FocuserMixin to create
    a complete focuser device.
    """
    
    def setup_config(self, config):
        """Set up focuser configuration."""
        self.setup_focuser_config(config)
        
    def __init__(self, device_name="F0", port=0):
        """Initialize focuser device."""
        super().__init__(device_name, DeviceType.FOCUS, port)
        
    def apply_config(self, config: Dict[str, Any]):
        """Apply configuration."""
        super().apply_config(config)
        self.apply_focuser_config(config)
        
    def _register_device_commands(self):
        """Register device command handlers with the network."""
        # Create and register the device command handler from parent
        super()._register_device_commands()

        # Register focuser-specific commands
        self._register_focuser_commands()
        
    def start(self):
        """Start the focuser device."""
        super().start()
        
        # Set focuser type
        self.foc_type.value = getattr(self, 'focuser_type', 'Generic')
        
        # Initialize position if not set
        if math.isnan(self.foc_def.value):
            # Get current position from hardware and set as default
            try:
                self.info()
                current_pos = self.get_position()
                self.foc_tar.value = current_pos
                self.foc_def.value = current_pos
                logging.info(f"Initialized focuser at position {current_pos}")
            except:
                # If we can't read position, use 0
                self.foc_pos.value = 0.0
                self.foc_tar.value = 0.0
                self.foc_def.value = 0.0
                logging.warning("Could not read initial position, using 0")
        elif not self.is_at_start_position():
            # Move to default position
            logging.info(f"Moving to default position {self.foc_def.value}")
            self.set_position(self.foc_def.value)
        else:
            # Already at start, just update target
            current_pos = self.get_position()
            self.foc_tar.value = current_pos
            logging.info(f"Focuser already at start position {current_pos}")
            
        logging.info(f"Focuser {self.device_name} started")
        
    def info(self):
        """Update focuser information - override in subclasses."""
        super().info()
        # Update focuser-specific information
        self.focuser_info_update()
        
    def idle(self):
        """Device idle processing."""
        # Call parent idle first
        result = super().idle()
        
        # Then focuser-specific idle
        focuser_result = self.focuser_idle()
        
        # Return the most restrictive timing
        if focuser_result is not None:
            if result is None:
                return focuser_result
            else:
                return min(result, focuser_result)
        return result
        
    def script_ends(self):
        """Called when a script ends."""
        result = super().script_ends()
        focuser_result = self.script_ends_focuser()

        # Return error if either failed
        if result != 0:
            return result
        return focuser_result

    def on_state_changed(self, old_state, new_state, message):
        """Handle device state changes."""
        super().on_state_changed(old_state, new_state, message)
        # Add any focuser-specific state change handling here if needed