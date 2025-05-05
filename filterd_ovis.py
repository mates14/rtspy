#!/usr/bin/python3

import time
import logging
import threading
import queue
import serial
import os
import select

from typing import Dict, List, Any, Callable, Optional, Tuple, Union

from value import (
    ValueSelection, ValueInteger, ValueBool, ValueString, ValueTime
)
from filterd import Filterd
from constants import DeviceType, ConnectionState
from app import App


class SerialCommand:
    """Represents a command to be sent to the serial device."""

    # Command types
    TYPE_NORESPONSE = 0  # Command without response
    TYPE_MOVEMENT = 1    # Movement command completed via status polling
    TYPE_HOMING = 2      # Homing command with longer timeout

    def __init__(self, command: str, callback: Optional[Callable] = None,
                 timeout: float = 5.0, cmd_type: int = TYPE_NORESPONSE):
        """
        Initialize a command.

        Args:
            command: Command string to send to device
            callback: Optional callback to call with result
            timeout: Timeout in seconds
            cmd_type: Command type (NORMAL, NORESPONSE, MOVEMENT)
        """

        # Adjust timeout based on command type
        if cmd_type == SerialCommand.TYPE_HOMING:
            # Use longer timeout for homing operations
            self.timeout = 20.0  # 20 seconds for homing
        else:
            self.timeout = timeout

        self.command = command
        self.callback = callback
        self.timeout = timeout
        self.timestamp = time.time()
        self.response = None
        self.completed = False
        self.event = threading.Event()
        self.cmd_type = cmd_type

    def complete(self, response: str) -> None:
        """Mark command as completed with response."""
        self.response = response
        self.completed = True

        # Notify any waiters
        self.event.set()

        # Call callback if defined
        if self.callback:
            try:
                self.callback(self.command, response)
            except Exception as e:
                logging.error(f"Error in command callback: {e}")

    def wait(self, timeout: Optional[float] = None) -> str:
        """
        Wait for command completion.

        Args:
            timeout: Timeout in seconds (or None to use command timeout)

        Returns:
            Command response or None on timeout
        """
        if timeout is None:
            timeout = self.timeout

        if self.event.wait(timeout):
            return self.response
        return None

    def is_timed_out(self) -> bool:
        """Check if command has timed out."""
        return not self.completed and (time.time() - self.timestamp) > self.timeout


class SerialCommunicator:
    """Handles communication with a serial device."""

    def __init__(self, device_file: str, baudrate: int = 9600):
        """
        Initialize the serial communicator.

        Args:
            device_file: Serial device path
            baudrate: Serial baudrate
        """
        self.device_file = device_file
        self.baudrate = baudrate
        self.serial_conn = None

        # Threading and synchronization
        self.running = False
        self.thread = None
        self.command_queue = queue.Queue()
        self.lock = threading.RLock()

        # Status tracking
        self.last_status_time = 0
        self.status_interval = 0.25  # seconds
        self.last_error_time = 0
        self.error_backoff = 10.0 # seconds

        # Command currently being processed
        self.current_command = None
        self.homing = False

        # Wake-up pipe for notifications
        self.wake_r, self.wake_w = os.pipe()

        # Status callback
        self.status_callback = None

        # Flag to check if connected
        self.connected = False

    def start(self) -> bool:
        """
        Start the serial communicator thread.

        Returns:
            True if successfully started, False otherwise
        """
        if self.thread and self.thread.is_alive():
            return True

        self.running = True
        self.thread = threading.Thread(
            target=self._communication_loop,
            name="SerialCommunicator",
            daemon=True
        )
        self.thread.start()
        return True

    def stop(self) -> None:
        """Stop the serial communicator."""
        self.running = False

        # Wake up the thread
        try:
            os.write(self.wake_w, b'x')
        except:
            pass

        # Wait for thread to terminate
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2.0)

        # Close serial connection
        self._close_connection()

        # Close pipes
        try:
            os.close(self.wake_r)
            os.close(self.wake_w)
        except:
            pass

    def _connect(self) -> bool:
        """
        Connect to the serial device.

        Returns:
            True if connected successfully, False otherwise
        """
        if not self.device_file:
            logging.error("No device file specified")
            return False

        try:
            logging.info(f"Opening serial connection to {self.device_file}")
            self.serial_conn = serial.Serial(
                port=self.device_file,
                baudrate=self.baudrate,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=1.0  # Short timeout for non-blocking reads
            )

            # Clear any buffered data
            self.serial_conn.reset_input_buffer()
            self.serial_conn.reset_output_buffer()

            # Wait for device to initialize
            time.sleep(0.5)

            # Check if device is responsive
            if not self._check_device_id():
                logging.error("Device not responsive or incorrect device type")
                self._close_connection()
                return False

            logging.info("Serial connection established successfully")
            self.connected = True
            return True

        except Exception as e:
            logging.error(f"Error connecting to serial device: {e}")
            self._close_connection()
            return False

    def _close_connection(self) -> None:
        """Close the serial connection."""
        if self.serial_conn:
            try:
                self.serial_conn.close()
            except:
                pass
            self.serial_conn = None
        self.connected = False

    def _check_device_id(self) -> bool:
        """
        Check if we're connected to the correct device.

        Returns:
            True if correct device, False otherwise
        """
        # Get device ID
        response = self._send_command_internal("ID")
        if not response:
            return False

        # Check if it's the correct device
        if response != "RPI_PICO_SPECTRAL_AVCR":
            logging.error(f"Invalid device ID: {response}")
            return False

        logging.info(f"Connected to device: {response}")
        return True

    def _communication_loop(self) -> None:
        """Main communication loop for serial thread."""
        logging.info("Serial communication thread starting")

        connect_retry_time = 0
        connect_retry_interval = 5.0  # seconds

        while self.running:
            try:
                # Check if we need to connect
                if not self.serial_conn or not self.connected:
                    current_time = time.time()
                    if current_time - connect_retry_time >= connect_retry_interval:
                        self._connect()
                        connect_retry_time = current_time
                    else:
                        time.sleep(0.1)
                        continue

                # Determine wait time until next status check
                current_time = time.time()
                time_until_status = max(0, self.last_status_time + self.status_interval - current_time)

                # Determine if we need to process a command or wait
                if not self.command_queue.empty():
                    # Process next command
                    self._process_next_command()
                elif time_until_status <= 0:
                    # Update status
                    self._update_status()
                    self.last_status_time = time.time()
                else:
                    # Wait for either wakeup or status interval
                    try:
                        # Use shorter of the two timeouts
                        timeout = min(time_until_status, 0.1)

                        # Wait with timeout for wakeup notification
                        r_ready, _, _ = select.select([self.wake_r], [], [], timeout)

                        if self.wake_r in r_ready:
                            # Clear wakeup byte
                            os.read(self.wake_r, 1024)
                    except Exception as e:
                        # Don't log select errors, just use a sleep instead
                        time.sleep(0.1)

            except Exception as e:
                logging.error(f"Error in serial communication loop: {e}")
                # Brief sleep to avoid tight loop on error
                time.sleep(0.5)

        logging.info("Serial communication thread stopping")

    def _process_next_command(self) -> None:
        """Process the next command in the queue."""
        if not self.serial_conn:
            return

        try:
            # Get next command
            command = self.command_queue.get_nowait()
            self.current_command = command

            # Check if timed out while in queue
            if command.is_timed_out():
                logging.warning(f"Command timed out in queue: {command.command}")
                command.complete("ERROR: Timed out in queue")
                self.current_command = None
                return

            # Send command
            cmd = command.command
            logging.info(f"Sending command: {cmd} (type {command.cmd_type})")
            self.serial_conn.write((cmd + '\n').encode())
            self.serial_conn.flush()  # Ensure command is sent immediately

            # Handle based on command type
            if command.cmd_type == SerialCommand.TYPE_NORESPONSE:
                # No response expected, complete immediately with "OK"
                time.sleep(0.1)  # Brief wait to allow command to be processed
                command.complete("OK")
                self.current_command = None

            elif command.cmd_type == SerialCommand.TYPE_MOVEMENT:
                # Don't clear current_command - it's needed for status updates
                pass

            else:
                # Normal command - read response
                try:
                    response = self.serial_conn.readline().decode().strip()

                    if not response:
                        logging.warning(f"No response received for command_: {cmd}")
                        command.complete("ERROR: No response")
                    else:
                        logging.info(f"Received response: {response}")
                        command.complete(response)
                except Exception as e:
                    logging.error(f"Error reading response: {e}")
                    command.complete(f"ERROR: {str(e)}")

                self.current_command = None

        except queue.Empty:
            pass
        except Exception as e:
            logging.error(f"Error processing command: {e}")
            if self.current_command:
                self.current_command.complete(f"ERROR: {str(e)}")
                self.current_command = None

    def _send_command_internal(self, cmd: str, wait_for_ok=False) -> Optional[str]:
        """
        Send a command to the device and return the response.

        Args:
            cmd: Command string to send
            wait_for_ok: If True, wait until device responds with OK

        Returns:
            Response string or None on error
        """
        if not self.serial_conn:
            return None

        # Ensure command ends with newline
        if not cmd.endswith('\n'):
            cmd += '\n'

        try:
            # Send command
        #    logging.info(f"Sending command: {cmd.strip()}")
            self.serial_conn.write(cmd.encode())
            self.serial_conn.flush()  # Ensure command is sent immediately

            # For commands that need to wait for OK (like HOM)
            if wait_for_ok or "HOM" in cmd:
                # Set a longer timeout for homing
                original_timeout = self.serial_conn.timeout
                self.serial_conn.timeout = 30.0  # 30 seconds for homing

                # Read until we get OK or timeout
                response = self.serial_conn.readline().decode().strip()

                # Restore original timeout
                self.serial_conn.timeout = original_timeout

                if not response:
                    logging.warning(f"No response received for command: {cmd.strip()}")
                    return None

                logging.info(f"Received response for homing: {response}")
                return response

            # For other commands that expect responses
            if cmd.strip() in ["STATUS", "ID"]:
                response = self.serial_conn.readline().decode().strip()

                if not response:
                    logging.warning(f"No response received for command: {cmd.strip()}")
                    return None

                #logging.info(f"Received response: {response}")
                return response
            else:
                # Most motor commands don't need responses
                return "OK"

        except Exception as e:
            logging.error(f"Error sending command: {e}")
            self.connected = False
            return None

    def _update_status(self) -> None:
        """Update device status."""
        if not self.serial_conn or self.homing:
            return

        try:
            # Send status command
            motor_statuses = []

            # Send STATUS command
            response = self._send_command_internal("STATUS")
            if not response:
                self.connected = False
                return

            # Parse motor 0 status (first line)
            motor_info = response.split()
            if len(motor_info) >= 5 and motor_info[0] == "M" and motor_info[1] == "0":
                # Format: M 0 power position is_moving speed acceleration
                # The fourth value is is_moving (0 or 1)
                is_moving = int(motor_info[3])
                motor_statuses.append({
                    'motor': 0,
                    'position': int(motor_info[2]),
                    'is_moving': is_moving,  # Added flag for movement
                    'speed': int(float(motor_info[4])),
                    'acceleration': int(float(motor_info[5]))
                })

            # Read motor 1 status (second line)
            response = self.serial_conn.readline().decode().strip()
            motor_info = response.split()
            if len(motor_info) >= 5 and motor_info[0] == "M" and motor_info[1] == "1":
                # Format: M 1 power position is_moving speed acceleration
                # The fourth value is is_moving (0 or 1)
                is_moving = int(motor_info[3])
                motor_statuses.append({
                    'motor': 1,
                    'position': int(motor_info[2]),
                    'is_moving': is_moving,  # Added flag for movement
                    'speed': int(float(motor_info[4])),
                    'acceleration': int(float(motor_info[5]))
                })
            #logging.info(f"Motor statuses: {motor_statuses}")

            # Read motor 2 status if present (third line)
            response = self.serial_conn.readline().decode().strip()

            # Read shutter status (fourth line)
            response = self.serial_conn.readline().decode().strip()

            # Read neon status (fifth line)
            response = self.serial_conn.readline().decode().strip()
            neon_info = response.split()
            neon_status = None
            if len(neon_info) >= 2 and neon_info[0] == "R":
                neon_status = int(neon_info[1])

            # Check if we have a movement command in progress
            if self.current_command and self.current_command.cmd_type == SerialCommand.TYPE_MOVEMENT:
                # Find motor 1 status
                motor1_status = next((m for m in motor_statuses if m['motor'] == 1), None)

                # Check if motor 1 is stopped (movement complete)
                if motor1_status and not motor1_status['is_moving']:
                    logging.info(f"Motor 1 movement complete at position {motor1_status['position']}")
                    # Complete the command
                    self.current_command.complete(f"OK {motor1_status['position']}")
                    self.current_command = None

            # Call status callback if registered
            if self.status_callback:
                self.status_callback(motor_statuses, neon_status)

        except Exception as e:
            logging.error(f"Error updating status: {e}")
            self.connected = False

    def send_command(self, cmd: str, callback: Optional[Callable] = None,
                     timeout: float = 5.0, wait: bool = False,
                     cmd_type: int = SerialCommand.TYPE_NORESPONSE) -> Optional[str]:
        """
        Send a command to the device.

        Args:
            cmd: Command string to send
            callback: Optional callback for when command completes
            timeout: Command timeout in seconds
            wait: If True, wait for completion and return response
            cmd_type: Command type (NORMAL, NORESPONSE, MOVEMENT)

        Returns:
            Response string if wait=True, else None
        """
        # Create command object
        command = SerialCommand(cmd, callback, timeout, cmd_type)

        # Add to queue
        self.command_queue.put(command)

        # Wake up thread to process command
        try:
            os.write(self.wake_w, b'x')
        except:
            pass

        # Wait for response if requested
        if wait:
            return command.wait(timeout)
        return None

    def set_status_callback(self, callback: Callable) -> None:
        """Set the status callback function."""
        self.status_callback = callback

    def is_connected(self) -> bool:
        """Check if connected to device."""
        return self.serial_conn is not None and self.connected


class Alta(Filterd):
    """
    Alta filter wheel driver for spectrograph.

    This is a Python implementation of the Alta filter wheel driver from RTS2.
    """

    @classmethod
    def register_options(cls, parser):
        """Register Alta-specific command line options."""
        super().register_options(parser)
        parser.add_argument('-f', '--device-file',
                          help='Serial port with the module (usually /dev/ttyUSB for Arduino USB serial connection)')

    @classmethod
    def process_args(cls, device, args):
        """Process arguments for this specific device."""
        super().process_args(device, args)
        if hasattr(args, 'device_file') and args.device_file:
            device.device_file = args.device_file

    def __init__(self, device_name="W0", port=0):
        """Initialize the Alta filter wheel device."""
        super().__init__(device_name, port)

        # Serial connection
        self.device_file = None
        self.serial_comm = None

        # Current filter position
        self.filter_num = 0

        # Status tracking
        self.motor_initialized = False
        self.filter_moving = False
        self.motor_position = 0
        self.is_motor_moving = False
        self.target_position = None
        self.motor_status_lock = threading.RLock()

        # Create values
        self.m1pos = ValueInteger("M1POS", "[int] motor position", write_to_fits=True)
        self.focpos = ValueInteger("FOCPOS", "[int] focuser position", write_to_fits=True)
        self.focpos.set_writable()

        self.neon = ValueSelection("NEON", "[on/off] neon lamp status", write_to_fits=True)
        self.neon.set_writable()
        self.neon.add_sel_val("off")
        self.neon.add_sel_val("on")

        # Filter position values
        self.f0pos = ValueInteger("F0POS", "[int] filter 0 position", write_to_fits=True)
        self.f1pos = ValueInteger("F1POS", "[int] filter 1 position", write_to_fits=True)
        self.f2pos = ValueInteger("F2POS", "[int] filter 2 position", write_to_fits=True)
        self.f3pos = ValueInteger("F3POS", "[int] filter 3 position", write_to_fits=True)
        self.f4pos = ValueInteger("F4POS", "[int] filter 4 position", write_to_fits=True)
        self.f5pos = ValueInteger("F5POS", "[int] filter 5 position", write_to_fits=True)
        self.f6pos = ValueInteger("F6POS", "[int] filter 6 position", write_to_fits=True)
        self.f7pos = ValueInteger("F7POS", "[int] filter 7 position", write_to_fits=True)

        # Make filter positions writable
        self.f0pos.set_writable()
        self.f1pos.set_writable()
        self.f2pos.set_writable()
        self.f3pos.set_writable()
        self.f4pos.set_writable()
        self.f5pos.set_writable()
        self.f6pos.set_writable()
        self.f7pos.set_writable()

        # Motor 0 values (focuser)
        self.m0hom = ValueSelection("M0HOM", "[true/false] motor homing status", write_to_fits=True)
        self.m0hom.add_sel_val("false")
        self.m0hom.add_sel_val("true")

        self.m0pos = ValueInteger("M0POS", "[int] motor position", write_to_fits=True)
        self.m0spd = ValueInteger("M0SPD", "[int] motor max speed", write_to_fits=True)
        self.m0acc = ValueInteger("M0ACC", "[int] motor acceleration", write_to_fits=True)

        self.m0pos.set_writable()
        self.m0spd.set_writable()
        self.m0acc.set_writable()

        # Motor 1 values (filter wheel)
        self.m1hom = ValueSelection("M1HOM", "[true/false] motor homing status", write_to_fits=True)
        self.m1hom.add_sel_val("false")
        self.m1hom.add_sel_val("true")

        self.m1spd = ValueInteger("M1SPD", "[int] motor max speed", write_to_fits=True)
        self.m1acc = ValueInteger("M1ACC", "[int] motor acceleration", write_to_fits=True)

        self.m1pos.set_writable()
        self.m1spd.set_writable()
        self.m1acc.set_writable()

        # Set default values similar to the C++ implementation
        self.f0pos.value = 2000
        self.f1pos.value = 54500
        self.f2pos.value = 107000
        self.f3pos.value = 159500
        self.f4pos.value = 212000
        self.f5pos.value = 292000  # grism
        self.f6pos.value = 300000  # unused
        self.f7pos.value = 300000  # limit

        self.m0hom.value = 0
        self.m1hom.value = 0

        # Default speeds and accelerations
        self.m0spd.value = 100000
        self.m0acc.value = 100000
        self.m1spd.value = 100000
        self.m1acc.value = 100000

        # Initialize in NOT_READY state until motors are initialized
        self.set_state(self.STATE_IDLE | self.NOT_READY, "Initializing hardware")

    def start(self):
        """Start the device."""
        super().start()

        # Try to initialize the device if we have a serial port
        if self.device_file:
            try:
                # Create and start serial communicator
                self.serial_comm = SerialCommunicator(self.device_file)
                self.serial_comm.set_status_callback(self._handle_status_update)
                if not self.serial_comm.start():
                    logging.error("Failed to start serial communicator")
                    self.set_state(self.STATE_IDLE | self.ERROR_HW, "Failed to start serial communication")
                    return

                # Initialize the device (async)
                self._initialize_device()
            except Exception as e:
                logging.error(f"Error initializing device: {e}")
                self.set_state(self.STATE_IDLE | self.ERROR_HW, "Initialization error")
                return
        else:
            logging.error("No device file specified")
            self.set_state(self.STATE_IDLE | self.ERROR_HW, "No device file specified")
            return

        # Set device ready after initialization
        # Note: The actual ready state will be set after motor initialization completes
        logging.info("Alta filter wheel starting, waiting for motor initialization")

    def stop(self):
        """Stop the device."""
        # Turn off motors before stopping
        if self.serial_comm and self.serial_comm.is_connected():
            try:
                logging.info("Turning off motors during shutdown")
                self.serial_comm.send_command("M 0 OFF")
                self.serial_comm.send_command("M 1 OFF")
            except Exception as e:
                logging.error(f"Error turning off motors: {e}")

        # Stop serial communicator
        if self.serial_comm:
            self.serial_comm.stop()
            self.serial_comm = None

        super().stop()

    def _initialize_device(self):
        """Initialize the device."""
        if not self.serial_comm:
            return

        # Make sure we're connected
        n=0
        while not self.serial_comm.is_connected():
            time.sleep(0.1)
            if n % 10 == 0:
                logging.info("Waiting for device connection...")
            if n > 50: # at my PC it takes ~0.5s -> a 5s timeout seems decent
                logging.error("Device connection timeout after 30 seconds")
                self.set_state(self.STATE_IDLE | self.NOT_READY, "Waiting for device connection ...")
                return

        # Set motor speeds and accelerations
        self.serial_comm.send_command(f"M 0 SPD {self.m0spd.value}")
        self.serial_comm.send_command(f"M 0 ACC {self.m0acc.value}")
        self.serial_comm.send_command(f"M 1 SPD {self.m1spd.value}")
        self.serial_comm.send_command(f"M 1 ACC {self.m1acc.value}")

        # Turn on motor 1 (filter wheel) and then home it in sequence
        logging.info("Powering on filter wheel motor...")
        self.serial_comm.send_command("M 1 ON", self._handle_motor_on_response)

    def _handle_motor_on_response(self, cmd, response):
        """Handle response from turning motor on."""
        logging.info("Motor 1 powered on successfully")

        # Now home the motor with explicit wait for completion
        logging.info("Homing filter wheel motor...")

        # Set device state to show we're homing
        self.set_state(
            self._state | self.FILTERD_MOVE,
            "Homing filter wheel",
            self.BOP_EXPOSURE
        )

        # Send command directly and wait for OK
        result = self.serial_comm._send_command_internal("M 1 HOM", wait_for_ok=True)

        if result and "OK" in result:
            logging.info("Motor 1 homed successfully")
            self.m1hom.value = 1
            self.motor_initialized = True
            self.set_ready("Motor initialized and ready")
            self.set_state(self._state & ~(self.FILTERD_MOVE), "Filter wheel idle", 0)
        else:
            logging.error(f"Error homing motor 1: {result}")
            self.set_state(self.STATE_IDLE | self.ERROR_HW, "Error homing motor")

    def _handle_status_update(self, motor_statuses, neon_status):
        # Process each motor status
        for motor in motor_statuses:
            motor_num = motor['motor']

            if motor_num == 0:  # Focuser motor
                # These Value objects handle their own thread safety
                if 'position' in motor:
                    self.m0pos.value = motor['position']
                if 'speed' in motor:
                    self.m0spd.value = motor['speed']
                if 'acceleration' in motor:
                    self.m0acc.value = motor['acceleration']

            elif motor_num == 1:  # Filter wheel motor
                # Capture needed values
                old_position = None
                new_position = None
                old_moving = None
                new_moving = None

                # Update shared state under lock
                with self.motor_status_lock:
                    old_position = self.motor_position
                    if 'position' in motor:
                        new_position = motor['position']
                        self.motor_position = new_position

                    old_moving = self.is_motor_moving
                    if 'is_moving' in motor:
                        new_moving = bool(motor['is_moving'])
                        self.is_motor_moving = new_moving

                # Updates to Value objects (thread-safe)
                if new_position is not None:
                    self.m1pos.value = new_position

                if 'speed' in motor:
                    self.m1spd.value = motor['speed']
                if 'acceleration' in motor:
                    self.m1acc.value = motor['acceleration']

                # Check for movement completion
                movement_completed = False
                with self.motor_status_lock:
                    if (self.filter_moving and
                        new_moving is not None and
                        old_moving and not new_moving):
                        movement_completed = True

                # Handle movement completion outside lock
                if movement_completed:
                    self._handle_filter_movement_complete()

        # Update neon status (thread-safe)
        if neon_status is not None and self.neon.value != neon_status:
            self.neon.value = neon_status

    def _handle_filter_movement_complete(self):
        """Handle completion of filter movement."""
        with self.motor_status_lock:
            # Mark movement as completed
            self.filter_moving = False
            current_pos = self.motor_position
            self.target_position = None

        # Find which filter position is closest to the current motor position
        target_positions = [ self.f0pos.value, self.f1pos.value,
            self.f2pos.value, self.f3pos.value, self.f4pos.value,
            self.f5pos.value, self.f6pos.value, self.f7pos.value ]

        closest_filter = 0
        closest_distance = abs(current_pos - target_positions[0])

        for i in range(1, len(target_positions)):
            distance = abs(current_pos - target_positions[i])
            if distance < closest_distance:
                closest_distance = distance
                closest_filter = i

        # Update filter position
        with self.motor_status_lock:
            self.filter_num = closest_filter
            self.filter.value = closest_filter

        # Update device state - done outside the lock
        self.set_state(self._state & ~(self.FILTERD_MOVE), "Filter wheel idle", 0)

    def get_filter_num(self):
        """Get current filter number."""
        return self.filter_num

    def set_filter_num(self, new_filter):
        """
        Set filter number (position).

        Args:
            new_filter: New filter position

        Returns:
            0 on success, -1 on error
        """
        # Check filter number validity
        if new_filter < 0 or new_filter >= 8:
            logging.error(f"Invalid filter number: {new_filter}")
            return -1

        # Check if serial communicator is initialized
        if not self.serial_comm or not self.serial_comm.is_connected():
            logging.error("Cannot set filter: Serial communicator not connected")
            return -1

        # Check if motor is initialized
        if not self.motor_initialized:
            logging.error("Cannot set filter: Motor not initialized")
            return -1

        # Already at this position?
        if new_filter == self.filter_num:
            logging.info(f"Filter already at position {new_filter}, no movement needed")
            return 0

        # Get the target position for this filter
        target_positions = [
            self.f0pos.value,
            self.f1pos.value,
            self.f2pos.value,
            self.f3pos.value,
            self.f4pos.value,
            self.f5pos.value,
            self.f6pos.value,
            self.f7pos.value
        ]

        target_position = target_positions[new_filter]
        logging.info(f"Moving filter to position {new_filter}, motor position {target_position}")

        try:
            # Update device state to show moving FIRST, before sending command
            # This ensures any clients see the movement state
            self.set_state( self._state | self.FILTERD_MOVE,
                f"Moving to filter {new_filter}", self.BOP_EXPOSURE)

            # Start filter movement - mark as moving internally
            with self.motor_status_lock:
                self.filter_moving = True
                self.target_position = target_position

            # Send the position command to motor 1
            cmd = f"M 1 ABS {target_position}"
            self.serial_comm.send_command(cmd,
                self._handle_filter_move_response,
                cmd_type=SerialCommand.TYPE_MOVEMENT,
                timeout=30.0)  # Longer timeout for movement

            # The actual completion will be handled by the status updates
            return 0

        except Exception as e:
            logging.error(f"Error setting filter: {e}")
            self.filter_moving = False
            # Clear the moving state on error
            self.set_state(self._state & ~self.FILTERD_MOVE, "Filter movement failed", 0)
            return -1

    def _handle_filter_move_response(self, cmd, response):
        """Handle response from filter move command."""
        if "OK" in response:
            logging.info(f"Filter move command accepted: {cmd}")
        else:
            logging.error(f"Filter move command failed: {response}")
            self.filter_moving = False
            self.set_state(self._state & ~(self.FILTERD_MOVE), "Filter movement failed", 0)

    def home_filter(self):
        """Home the filter wheel."""
        if not self.serial_comm or not self.serial_comm.is_connected():
            logging.error("Cannot home filter: Serial communicator not connected")
            return -1

        if not self.motor_initialized:
            logging.error("Cannot home filter: Motor not initialized")
            return -1

        logging.info("Homing filter wheel")

        # Set device state to show filter is moving
        self.set_state(
            self._state | self.FILTERD_MOVE,
            "Homing filter wheel",
            self.BOP_EXPOSURE
        )

        with self.motor_status_lock:
            self.filter_moving = True

        try:
            self.homing = True
            # Send command directly and wait for OK - use same approach as in _handle_motor_on_response
            result = self.serial_comm._send_command_internal("M 1 HOM", wait_for_ok=True)

            if result and "OK" in result:
                logging.info("Filter wheel homed successfully")
                self.m1hom.value = 1
                # Position will be updated by status updates
                self.set_state(self._state & ~(self.FILTERD_MOVE), "Homing finished", 0)
                return 0
            else:
                logging.error(f"Error homing filter wheel: {result}")
                with self.motor_status_lock:
                    self.filter_moving = False

                self.set_state(self._state & ~(self.FILTERD_MOVE), "Homing failed", 0)
                return -1
        finally:
            self.homing = False

    def __ex_home_filter(self):
        """
        Home the filter wheel by moving to filter 0.

        Returns:
            0 on success, -1 on error
        """
        # Check if serial communicator is initialized
        if not self.serial_comm or not self.serial_comm.is_connected():
            logging.error("Cannot home filter: Serial communicator not connected")
            return -1

        # Check if motor is initialized
        if not self.motor_initialized:
            logging.error("Cannot home filter: Motor not initialized")
            return -1

        logging.info("Homing filter wheel")

        # Set device state to show filter is moving
        self.set_state( self._state | self.FILTERD_MOVE,
            "Homing filter wheel", self.BOP_EXPOSURE )

        try:
            # Start filter movement
            with self.motor_status_lock:
                self.filter_moving = True

            # Send home command with longer timeout
            self.serial_comm.send_command(
                "M 1 HOM",
                self._handle_home_response,
                cmd_type=SerialCommand.TYPE_HOMING,
                timeout=20.0  # 20 seconds timeout for homing
            )

            # Update filter position - will be completed by status updates
            return 0

        except Exception as e:
            logging.error(f"Error homing filter wheel: {e}")
            self.filter_moving = False
            self.set_state(self._state & ~(self.FILTERD_MOVE), "Homing failed", 0)
            return -1

    def _handle_home_response(self, cmd, response):
        """Handle response from home command."""
        if "OK" in response:
            logging.info("Filter home command accepted")
            self.m1hom.value = 1
        else:
            logging.error(f"Filter home command failed: {response}")
            with self.motor_status_lock:
                self.filter_moving = False
            self.set_state(self._state & ~(self.FILTERD_MOVE), "Homing failed", 0)

    def info(self):
        """Update device information."""
        # Parent method will distribute values
        super().info()

    def on_value_changed_from_client(self, value, old_value, new_value):
        """
        Handle value changes from client.

        This is called when a client changes a value via the network.
        """
        # Handle special cases for different values
        value_name = value.name

        if value_name == "NEON":
            self._handle_neon_change(new_value)
        elif value_name == "FOCPOS":
            self._handle_focus_position_change(new_value)
        elif value_name == "M0POS":
            self._handle_motor0_position_change(new_value)
        elif value_name == "M1POS":
            self._handle_motor1_position_change(new_value)
        elif value_name == "M0SPD":
            self._handle_motor0_speed_change(new_value)
        elif value_name == "M1SPD":
            self._handle_motor1_speed_change(new_value)
        elif value_name == "M0ACC":
            self._handle_motor0_acceleration_change(new_value)
        elif value_name == "M1ACC":
            self._handle_motor1_acceleration_change(new_value)
        elif value_name.startswith("F") and value_name.endswith("POS"):
            # Handle filter position change - might need to update actual position if this is the current filter
            filter_idx = int(value_name[1])
            if filter_idx == self.filter_num:
                self._handle_current_filter_position_change(new_value)

        # Call parent handler
        super().on_value_changed_from_client(value, old_value, new_value)

    def _handle_neon_change(self, new_value):
        """Handle neon lamp setting change."""
        if not self.serial_comm:
            return

        if new_value == 0:  # OFF
            self.serial_comm.send_command("S ON")
            self.serial_comm.send_command("S IN")
            self.serial_comm.send_command("R OFF")
        else:  # ON
            self.serial_comm.send_command("S ON")
            self.serial_comm.send_command("S OUT")
            self.serial_comm.send_command("R ON")
        logging.info(f"Neon lamp set to {'ON' if new_value else 'OFF'}")

    def _handle_focus_position_change(self, new_value):
        """Handle focus position change."""
        if not self.serial_comm:
            return

        cmd = f"F 0 ABS {new_value}"
        self.serial_comm.send_command(cmd)
        logging.info(f"Focus position set to {new_value}")

    def _handle_motor0_position_change(self, new_value):
        """Handle motor 0 position change."""
        if not self.serial_comm:
            return

        cmd = f"M 0 ABS {new_value}"
        self.serial_comm.send_command(cmd)
        logging.info(f"Motor 0 position set to {new_value}")

    def _handle_motor1_position_change(self, new_value):
        """Handle motor 1 position change."""
        if not self.serial_comm:
            return

        cmd = f"M 1 ABS {new_value}"
        self.serial_comm.send_command(cmd)
        logging.info(f"Motor 1 position set to {new_value}")

    def _handle_motor0_speed_change(self, new_value):
        """Handle motor 0 speed change."""
        if not self.serial_comm:
            return

        cmd = f"M 0 SPD {new_value}"
        self.serial_comm.send_command(cmd)
        logging.info(f"Motor 0 speed set to {new_value}")

    def _handle_motor1_speed_change(self, new_value):
        """Handle motor 1 speed change."""
        if not self.serial_comm:
            return

        cmd = f"M 1 SPD {new_value}"
        self.serial_comm.send_command(cmd)
        logging.info(f"Motor 1 speed set to {new_value}")

    def _handle_motor0_acceleration_change(self, new_value):
        """Handle motor 0 acceleration change."""
        if not self.serial_comm:
            return

        cmd = f"M 0 ACC {new_value}"
        self.serial_comm.send_command(cmd)
        logging.info(f"Motor 0 acceleration set to {new_value}")

    def _handle_motor1_acceleration_change(self, new_value):
        """Handle motor 1 acceleration change."""
        if not self.serial_comm:
            return

        cmd = f"M 1 ACC {new_value}"
        self.serial_comm.send_command(cmd)
        logging.info(f"Motor 1 acceleration set to {new_value}")

    def _handle_current_filter_position_change(self, new_value):
        """Handle change to current filter position."""
        if not self.serial_comm:
            return

        cmd = f"M 1 ABS {new_value}"
        self.serial_comm.send_command(cmd)
        logging.info(f"Current filter position adjusted to {new_value}")

    def should_queue_value(self, value):
        """Check if a value change should be queued."""
        # Queue value changes when filter is moving
        if (self._state & self.FILTERD_MASK) == self.FILTERD_MOVE:
            return True
        return False


if __name__ == "__main__":
    # For standalone imports

    # Create application
    app = App(description='Alta Filter Wheel Driver')

    # Register device-specific options
    app.register_device_options(Alta)

    # Parse command line arguments
    args = app.parse_args()

    # Create and configure device
    device = app.create_device(Alta)

    # Run application main loop
    app.run()
