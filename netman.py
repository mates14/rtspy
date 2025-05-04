import socket
import select
import threading
import queue
import logging
import time
import uuid
import os
import errno
import math
import fcntl

from constants import ConnectionState, DeviceType, DevTypes
from connection import Connection, ConnectionManager, QueuedCommand
from commands import CommandRegistry, ProtocolCommands, AuthCommands

class NetworkManager:
    """
    Manages network connections for RTS2 devices.

    This class handles:
    - Listening for incoming connections
    - Managing outgoing connections
    - Authentication
    - Message dispatch
    - Value distribution
    - Device-to-device communication
    """

    # Class variable to track the singleton instance
    _instance = None

    def __init__(self, device_name: str, device_type: int, port: int = 0):
        logging.debug(f"Starting NetworkManager {device_name} {device_type} {port}")
        self.device_name = device_name
        self.device_type = device_type
        self.port = port

        # Set singleton instance
        NetworkManager._instance = self

        self.centrald_host = "localhost"
        self.centrald_port = 617

        # Connection management - now using ConnectionManager
        self.connection_manager = ConnectionManager()

        # Threading setup
        self.running = False
        self.network_thread = None
        self._lock = threading.RLock()
        self.message_queue = queue.Queue()

        # Server socket
        self.server_socket = None

        # Registered values
        self.values = {}

        # Callbacks
        self.auth_callback = None
        self.centrald_connected_callback = None

        # Device state
        self.device_state = 0x0  # just fine
        self.bop_state = 0x0
        self.last_status_message = None
        self.state_changed_callback = None

        # Command progress status
        self.state_start = float('nan')
        self.state_expected_end = float('nan')

        # Command handlers dictionary - unified for all commands
        self.command_registry = CommandRegistry()

        # Registry to track known clients and devices
        self.entities = {}  # Indexed by centrald_id -> {name, type, device_type etc.}

        # Interest tracking
        self.value_interests = {}  # "device_name.value_name" -> callback
        self.state_interests = {}  # device_name -> callback
        self.pending_interests = set()  # Set of device names we're interested in
        self.connection_retry_interval = 30.0  # seconds between connection attempts
        self.device_connection_attempts = {}  # device_name -> last_attempt_time

        # Register standard command handlers
        self._register_command_handlers()

        # Create a self-pipe for waking up the select() call
        self.wake_r, self.wake_w = os.pipe()
        # Set non-blocking mode for the reading end
        fcntl.fcntl(self.wake_r, fcntl.F_SETFL, os.O_NONBLOCK)

    def _register_command_handlers(self):
        """Register command handlers with the registry."""
        # Create handler groups
        protocol_handler = ProtocolCommands(self)
        auth_handler = AuthCommands(self)

        # Register with registry
        self.command_registry.register_handler(protocol_handler)
        self.command_registry.register_handler(auth_handler)

        # Log registered commands
        commands = self.command_registry.get_all_commands()
        logging.debug(f"Registered {len(commands)} commands: {', '.join(sorted(commands))}")

    def start(self):
        """Start the network manager."""
        if self.running:
            return

        self.running = True

        # Create server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', self.port))
        self.server_socket.listen(5)
        self.server_socket.setblocking(False)

        # Get assigned port if it was 0
        if self.port == 0:
            _, self.port = self.server_socket.getsockname()

        # Start network thread
        self.network_thread = threading.Thread(
            target=self._network_loop,
            name="RTS2-Network",
            daemon=True
        )
        self.network_thread.start()

        # Start interest manager thread
        self.interest_thread = threading.Thread(
            target=self._interest_manager_loop,
            name="RTS2-Interest",
            daemon=True
        )
        self.interest_thread.start()

        self.connect_to_centrald(self.centrald_host, self.centrald_port)

    def _interest_manager_loop(self):
        """
        Thread that manages device-to-device connections based on interests.

        This continuously checks for new devices in the centrald registry that
        match our interest list, and establishes connections as needed when
        there's no existing connection to the device.
        """
        logging.debug("Interest manager thread started")

        while self.running:
            try:
                # Wait until we have a fully authenticated centrald connection
                centrald_conn = self.connection_manager.get_associated_centrald_connection()
                if not centrald_conn or centrald_conn.state != ConnectionState.AUTH_OK:
                    time.sleep(1.0)
                    continue

                # Check for pending interests to process
                current_time = time.time()

                # Process each pending interest
                for device_name in list(self.pending_interests):
                    # Skip if we already have a connection to this device
                    device_connected = False
                    for conn in self.connection_manager.connections.values():
                        if (conn.state in (ConnectionState.AUTH_OK, ConnectionState.AUTH_PENDING) and
                            hasattr(conn, 'remote_device_name') and
                            conn.remote_device_name == device_name):
                            device_connected = True
                            #logging.debug(f"Already connected/connecting to device {device_name}")
                            break

                    if device_connected:
                        continue

                    # Look for the device in the entity registry
                    device_found = False
                    device_info = None

                    for entity_id, entity in self.entities.items():
                        if (entity.get('name') == device_name and
                            entity.get('entity_type') == 'DEVICE'):
                            device_found = True
                            device_info = entity
                            break

                    if device_found and device_info:
                        # Check if we've attempted to connect recently
                        last_attempt = self.device_connection_attempts.get(device_name, 0)
                        if current_time - last_attempt < self.connection_retry_interval:
                            continue

                        # Try to connect to the device
                        host = device_info.get('host')
                        port = device_info.get('port')

                        if host and port and centrald_conn.auth_key is not None:
                            logging.info(f"Establishing connection to device {device_name} at {host}:{port}")
                            self.device_connection_attempts[device_name] = current_time

                            # Connect to the device
                            self._connect_to_device(host, port, device_name)
                        else:
                            # Either missing host/port or auth key not yet available
                            if centrald_conn.auth_key is None:
                                logging.debug(f"Waiting for auth key to connect to {device_name}")
                            else:
                                logging.warning(f"Missing host/port for device {device_name}")

                # Sleep a bit to avoid tight loop
                time.sleep(1.0)

            except Exception as e:
                logging.error(f"Error in interest manager: {e}", exc_info=True)
                time.sleep(5.0)  # Longer sleep on error

    def stop(self):
        """Stop the network manager."""
        self.running = False

        # Close self-pipe
        try:
            os.close(self.wake_r)
            os.close(self.wake_w)
        except:
            pass

        # Close all connections
        self.connection_manager.close_all_connections()

        # Close server socket
        if self.server_socket:
            self.server_socket.close()
            self.server_socket = None

        # Wait for threads to terminate
        if self.network_thread and self.network_thread.is_alive():
            self.network_thread.join(timeout=2.0)

        if self.interest_thread and self.interest_thread.is_alive():
            self.interest_thread.join(timeout=2.0)

        logging.debug("NetworkManager stopped")

    def put_message(self, msg):
        """Put a message in the queue and wake up the select loop."""
        self.message_queue.put(msg)
        # Wake up the select loop by writing a byte
        try:
            os.write(self.wake_w, b'x')
        except:
            pass

    def _accept_connection(self):
        """Accept a new incoming connection."""
        try:
            client_sock, client_addr = self.server_socket.accept()
            client_sock.setblocking(False)

            # Create unique ID for this connection
            conn_id = str(uuid.uuid4())

            # Create new connection object
            conn = Connection(conn_id, client_sock, client_addr, conn_type='client')

            # Register callbacks
            conn.register_command_callback(self._on_command_received)
            conn.register_closed_callback(self._on_connection_closed)

            # Add to connection manager
            self.connection_manager.add_connection(conn)

            logging.debug(f"New connection from {client_addr}")

            # Send metadata and values immediately for client connections
            # RTS2 protocol behavior: send E+V messages right after connection
            self._send_meta_info(conn)

        except Exception as e:
            logging.error(f"Error accepting connection: {e}")

    def _network_loop(self):
        """Main network processing loop."""
        logging.info(f"NetworkManager started on port {self.port}")
        last_cleanup_time = time.time()
        last_keepalive_check = time.time()

        while self.running:
            try:
                current_time = time.time()

                # Run connection cleanup every minute
                if current_time - last_cleanup_time > 60.0:
                    self.connection_manager.clean_stale_connections()
                    last_cleanup_time = current_time

                # Check for keepalives every 15 seconds
                if current_time - last_keepalive_check > 15.0:
                    self.connection_manager.check_all_keepalives()
                    last_keepalive_check = current_time

                readable, writable, exceptional = [], [], []

                # Prepare for select
                with self._lock:
                    # Add self-pipe for wakeup
                    readable.append(self.wake_r)

                    # Add server socket
                    if self.server_socket:
                        readable.append(self.server_socket)

                    # Add all connections to read/write sets
                    for conn in self.connection_manager.connections.values():
                        if conn.socket:
                            if conn.state == ConnectionState.CONNECTING:
                                # Sockets in connecting state should be checked for writability
                                writable.append(conn.socket)
                            else:
                                readable.append(conn.socket)

                            # Add to writable if there's data to send
                            if conn.write_buffer:
                                writable.append(conn.socket)

                # Wait for network events - this is the main blocking call
                # We can use a longer timeout now because we can be interrupted
                if self.message_queue.empty():
                    select_timeout = 1.0
                else:
                    select_timeout = 0.000001

                # Wait for network events
                if not readable and not writable:
                    # No sockets to monitor, shorter sleep to check for messages
                    time.sleep(0.1)
                    continue

                r, w, e = select.select(readable, writable, exceptional, select_timeout)

                # Handle the self-pipe (wakeup) first
                if self.wake_r in r:
                    # Read and discard the wakeup byte
                    try:
                        _ = os.read(self.wake_r, 1024)
                    except:
                        pass
                    r.remove(self.wake_r)

                # Handle readable sockets
                for sock in r:
                    if sock is self.server_socket:
                        self._accept_connection()
                    else:
                        self._handle_readable_socket(sock)

                # Handle writable sockets
                for sock in w:
                    conn = self.connection_manager.find_connection_by_socket(sock)
                    if not conn:
                        continue

                    # Special handling for CONNECTING sockets
                    if conn.state == ConnectionState.CONNECTING:
                        self._handle_connection_established(conn)
                    else:
                        conn.flush_write_buffer()

                # Process queued messages
                while not self.message_queue.empty():
                    try:
                        msg = self.message_queue.get_nowait()
                        self._process_message(msg)
                    except queue.Empty:
                        break

            except Exception as e:
                logging.error(f"Error in network loop: {e}", exc_info=True)
                time.sleep(0.1)  # Prevent tight loop on recurring errors

    def _handle_readable_socket(self, sock):
        """Handle a readable socket."""
        conn = self.connection_manager.find_connection_by_socket(sock)
        if not conn:
            logging.warning(f"Received data for unknown socket {sock}")
            sock.close()
            return

        try:
            data = sock.recv(4096)
            if not data:
                # Connection closed
                conn.close()
                return

            # Process received data
            conn.process_data(data)

        except ConnectionError:
            conn.close()
        except Exception as e:
            logging.error(f"Error reading from {conn.name}: {e}")
            conn.close()

    def _on_command_received(self, conn_id, line):
        """Handle a command received from a connection."""
        # Queue for processing
        self.put_message(('command', conn_id, line))

    def _on_connection_closed(self, conn_id):
        """Handle a connection being closed."""
        # Remove from connection manager
        self.connection_manager.remove_connection(conn_id)

    def _handle_connection_established(self, conn):
        """Handle a socket that has completed its connection attempt."""
        # Check socket error status
        err = conn.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if err != 0:
            logging.error(f"Socket error connecting to {conn.name}: {os.strerror(err)}")
            conn.close()
            return

        # Connection successful
        conn.update_state(ConnectionState.CONNECTED, "Connection established")

        # For centrald connections, send registration
        if conn.type == 'centrald' and not getattr(conn, 'registration_sent', False):
            conn.registration_sent = True
            conn.send_command(
                f"register 0 {self.device_name} {self.device_type} localhost {self.port}",
                lambda c, success, code, msg: self._handle_registration_result(c, success, code, msg)
            )
            conn.update_state(ConnectionState.AUTH_PENDING, "Registration command sent to centrald")

        # For device connections, send device authentication
        elif conn.type == 'device' and not getattr(conn, 'registration_sent', False):
            # For device-to-device connections, we need to authenticate using our auth key
            centrald_conn = self.connection_manager.get_associated_centrald_connection()
            if centrald_conn and hasattr(centrald_conn, 'auth_key') and centrald_conn.auth_key:
                conn.registration_sent = True

                # Send auth command with our device ID and auth key from centrald
                auth_cmd = f"auth {centrald_conn.device_id} {centrald_conn.centrald_num} {centrald_conn.auth_key}"
                conn.send_command(auth_cmd)
                conn.update_state(ConnectionState.AUTH_PENDING, "Authentication sent to device")
            else:
                logging.error("Cannot authenticate to device: missing centrald connection or auth key")
                conn.close()

    def _process_message(self, msg):
        """Process a queued message."""
        msg_type, *args = msg

        if msg_type == 'command':
            conn_id, line = args
            self._handle_command(conn_id, line)
        elif msg_type == 'send_value':
            value, conn_id = args
            self._handle_send_value(value, conn_id)
        elif msg_type == 'broadcast_value':
            value = args[0]
            self._handle_broadcast_value(value)

    def _handle_command(self, conn_id, line):
        """Handle a command from a connection."""
        # Get the connection
        conn = self.connection_manager.get_connection(conn_id)
        if not conn:
            logging.warning(f"Command received for unknown connection: {conn_id}")
            return

        # Split into command and parameters
        parts = line.split(maxsplit=1)
        cmd = parts[0] if parts else ""
        params = parts[1] if len(parts) > 1 else ""

        logging.debug(f"ICMD {conn.name}: '{cmd}', Params: '{params}'")

        # Special handling for this_device command - identifies device connections
        if cmd == "this_device":
            self._handle_this_device(conn, params)
            # Continue with normal command processing

        # Check if this is a fire-and-forget command that can bypass current processing
        is_immediate_command = self.command_registry.can_handle(cmd) and not self.command_registry.needs_response(cmd)

        # If it's not an immediate command and another command is in progress, queue it
        if not is_immediate_command and conn.command_in_progress:
            logging.debug(f"Command {cmd} queued - another command is in progress")
            conn.command_queue.put(QueuedCommand(f"{cmd} {params}"))
            return

        # For regular commands that need responses, set command_in_progress
        if not is_immediate_command:
            conn.command_in_progress = True

        # Dispatch to registry
        if self.command_registry.can_handle(cmd):
            success, result = self.command_registry.dispatch(cmd, conn, params)

            # Check if command was handled and still expects a response
            if not is_immediate_command and conn.command_in_progress:
                needs_response = self.command_registry.needs_response(cmd)

                if needs_response:
                    if success:
                        if isinstance(result, bool):
                            if result:
                                self._send_ok_response(conn)
                            else:
                                self._send_error_response(conn, f"Command {cmd} failed")
                        elif result is None:
                            # Default to success
                            self._send_ok_response(conn)
                    else:
                        # Error in handler
                        self._send_error_response(conn, str(result))
        else:
            # Unknown command - log and ignore
            logging.warning(f"Unknown command from {conn.name}: '{line}' - ignoring")
            conn.command_in_progress = False

        # Check for queued commands if this command has completed
        if not conn.command_in_progress and not conn.command_queue.empty():
            # Get next command from queue
            next_cmd_item = conn.command_queue.get()

            # Extract command and parameters
            next_cmd = next_cmd_item.command.split(maxsplit=1)[0]
            next_params = next_cmd_item.command.split(maxsplit=1)[1] if " " in next_cmd_item.command else ""

            self._process_next_command(conn, next_cmd, next_params)

    def _handle_this_device(self, conn, params):
        """
        Handle this_device info command that identifies a device connection.

        Args:
            conn: Connection object
            params: Parameters containing device_name and device_type
        """
        parts = params.split()
        if len(parts) < 2:
            return

        device_name = parts[0]
        device_type = int(parts[1])

        # Update connection info
        conn.remote_device_name = device_name
        conn.remote_device_type = device_type

        # Mark this as a device connection
        conn.type = 'device'
        conn.is_device_connection = True

        # Update descriptive name
#        conn.update_name()

        logging.debug(f"Identified device connection: {device_name}, type: {device_type}")

        # Check if we have registered interest in this device
        if device_name in self.pending_interests:
            logging.info(f"Connected to device of interest: {device_name}")

            # If connection is authenticated, request info
            if conn.state == ConnectionState.AUTH_OK:
                conn.send_command("info")

    def _process_next_command(self, conn, cmd, params):
        """Process the next command from the queue."""
        conn.command_in_progress = True
        logging.debug(f"Processing queued command for {conn.name}: {cmd}")
        self._handle_command(conn.id, f"{cmd} {params}".strip())

    def connect_to_centrald(self, host, port):
        """Connect to a centrald server."""
        try:
            # Create socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)

            # Start non-blocking connect
            err = sock.connect_ex((host, port))
            if err != errno.EINPROGRESS and err != 0:
                logging.error(f"Error connecting to centrald: {os.strerror(err)}")
                return None

            # Create new connection object
            conn_id = str(uuid.uuid4())
            conn = Connection(conn_id, sock, (host, port), conn_type='centrald')

            # Register callbacks
            conn.register_command_callback(self._on_command_received)
            conn.register_closed_callback(self._on_connection_closed)

            # Update state to connecting
            conn.update_state(ConnectionState.CONNECTING, "Connecting to centrald")

            # Add to connection manager
            self.connection_manager.add_connection(conn)

            logging.debug(f"Connecting to centrald at {host}:{port}")
            return conn_id

        except Exception as e:
            logging.error(f"Exception in connect_to_centrald: {e}")
            return None

    def _connect_to_device(self, host, port, device_name=None):
        """
        Connect to another RTS2 device.

        Args:
            host: Hostname or IP address of the device
            port: TCP port number
            device_name: Optional name of the target device for tracking

        Returns:
            Connection ID on success, None on error
        """
        try:
            # Check if we have a valid centrald connection
            centrald_conn = self.connection_manager.get_associated_centrald_connection()
            if not centrald_conn or centrald_conn.state != ConnectionState.AUTH_OK:
                logging.error("Cannot connect to device: no authenticated centrald connection")
                return None

            # Get our device ID and auth key from the centrald connection
            device_id = getattr(centrald_conn, 'device_id', 0)
            centrald_num = getattr(centrald_conn, 'centrald_num', 0)
            auth_key = getattr(centrald_conn, 'auth_key', None)

            if device_id <= 0:
                logging.error("Cannot connect to device: missing device ID from centrald")
                return None

            if auth_key is None:
                logging.error("Cannot connect to device: missing auth key from centrald")
                return None

            # Create socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)

            # Start non-blocking connect
            err = sock.connect_ex((host, port))
            if err != errno.EINPROGRESS and err != 0:
                logging.error(f"Error connecting to device: {os.strerror(err)}")
                return None

            # Create new connection object
            conn_id = str(uuid.uuid4())
            conn = Connection(conn_id, sock, (host, port), conn_type='device')

            # Store target device name if provided
            if device_name:
                conn.remote_device_name = device_name

            # Store auth information for later use
            conn.centrald_id = device_id
            conn.auth_key = auth_key
            conn.centrald_num = centrald_num

            # Register callbacks
            conn.register_command_callback(self._on_command_received)
            conn.register_closed_callback(self._on_connection_closed)

            # Update state to connecting
            conn.update_state(ConnectionState.CONNECTING, "Connecting to device")

            # Add to connection manager
            self.connection_manager.add_connection(conn)

            logging.debug(f"Connecting to device at {host}:{port}")
            return conn_id

        except Exception as e:
            logging.error(f"Exception in _connect_to_device: {e}")
            return None

    def _send_meta_info(self, conn):
        """Send metadata information about all values."""
        with self._lock:
            for value in self.values.values():
                # Send detailed metadata using the E protocol command
                msg = f"E {value.rts2_type} \"{value.name}\" \"{value.description}\"\n"
                conn.send(msg)

                # Handle selection values specially
                from value import ValueSelection
                if isinstance(value, ValueSelection):
                    # First send empty selection to clear any existing values
                    conn.send(f"F \"{value.name}\"\n")

                    # Then send each selection option
                    for sel_val in value._selection_values:
                        conn.send(f"F \"{value.name}\" \"{sel_val}\"\n")

                # Also send current value
                self._send_value(conn, value)

    def _handle_registration_result(self, conn, success, code, msg):
        """Handle result from registration command."""
        if success:
            logging.debug(f"Registration successful: {msg}")
            # Registration successful, now need to wait for registered_as message
            # The centrald_connected_callback will be called after getting registered_as
        else:
            logging.error(f"Registration failed: {msg}")
            conn.update_state(ConnectionState.BROKEN, f"{msg}")

    def _send_value(self, conn, value):
        """Send a value to a connection."""
        msg = f"V {value.name} {value.get_string_value()}\n"
        conn.send(msg)

    def _handle_send_value(self, value, conn_id):
        """Send a value to a specific connection."""
        conn = self.connection_manager.get_connection(conn_id)
        if conn and conn.state == ConnectionState.AUTH_OK:
            self._send_value(conn, value)

    def _handle_broadcast_value(self, value):
        """Broadcast a value to all authenticated connections."""
        # Get all authenticated connections
        auth_conns = self.connection_manager.get_connections_by_state(ConnectionState.AUTH_OK)

        # Send to each connection
        for conn in auth_conns.values():
            self._send_value(conn, value)

    def register_value(self, value):
        """Register a value for network distribution."""
        with self._lock:
            self.values[value.name] = value

    def unregister_value(self, value_name):
        """Unregister a value."""
        with self._lock:
            if value_name in self.values:
                del self.values[value_name]

    def broadcast_value(self, value):
        """Broadcast a value to all connections."""
        # Queue for processing
        self.put_message(('broadcast_value', value))

    def send_value_to(self, value, conn_id):
        """Send a value to a specific connection."""
        # Queue for processing
        self.put_message(('send_value', value, conn_id))

    def set_auth_callback(self, callback):
        """Set the authentication callback."""
        self.auth_callback = callback

    def set_device_state(self, new_state, message=None):
        """Set device state."""
        # Update internal state
        old_state = self.device_state
        self.device_state = new_state
        self.last_status_message = message

        # Send state to all connections (S command)
        self._send_status()

        # Call state changed callback if registered
        if old_state != new_state and self.state_changed_callback:
            self.state_changed_callback(old_state, new_state, message)

    def set_bop_state(self, state, new_bop_state, message=None):
        """
        Set both device state and BOP state at once.

        Args:
            state: Device state
            new_bop_state: Block operation state
            message: Optional status message
        """
        # Update internal states
        old_state = self.device_state

        self.device_state = state
        self.bop_state = new_bop_state
        self.last_status_message = message

        # Send BOP message to all connections
        bop_msg = f"B {self.device_state} {self.bop_state}"
        if message:
            bop_msg += f" \"{message}\""
        bop_msg += "\n"

        # Broadcast to all connections
        self.connection_manager.broadcast_message(bop_msg)

    def set_progress_state(self, state, start, end, message=None):
        """Set device state with progress information."""
        old_state = self.device_state
        self.device_state = state
        self.last_status_message = message

        # Set progress tracking values
        self.state_start = start
        self.state_expected_end = end

        # Send status with progress information
        self._send_status()

        # Call state changed callback if registered
        if old_state != state and self.state_changed_callback:
            self.state_changed_callback(old_state, state, message)

    def send_status_message(self, state, message=None):
        """Send an S command with state information."""
        status_msg = f"S {state}"
        if message:
            status_msg += f" \"{message}\""
        status_msg += "\n"

        # Broadcast to all connections
        self.connection_manager.broadcast_message(status_msg)

        # Update internal state
        old_state = self.device_state
        self.device_state = state
        self.last_status_message = message

        # Call state changed callback if registered
        if old_state != state and self.state_changed_callback:
            self.state_changed_callback(old_state, state, message)

    def send_bop_message(self, state, bop_state, message=None):
        """Send a B command with state and BOP information."""
        bop_msg = f"B {state} {bop_state}"
        if message:
            bop_msg += f" \"{message}\""
        bop_msg += "\n"

        # Broadcast to all connections
        self.connection_manager.broadcast_message(bop_msg)

        # Update internal states
        old_state = self.device_state
        self.device_state = state
        self.bop_state = bop_state
        self.last_status_message = message

        # Call state changed callback if registered
        if old_state != state and self.state_changed_callback:
            self.state_changed_callback(old_state, state, message)

    def _send_status(self, conn=None):
        """Send current device status to a connection or all connections."""
        if math.isnan(self.state_start) and math.isnan(self.state_expected_end):
            # Standard status message
            status_msg = f"S {self.device_state}"
        else:
            # Progress status message
            status_msg = f"R {self.device_state} {self.state_start:.6f} {self.state_expected_end:.6f}"

        # Add message text if available
        if self.last_status_message:
            status_msg += f" \"{self.last_status_message}\""
        status_msg += "\n"

        if conn:
            conn.send(status_msg)
        else:
            # Broadcast to all connections
            self.connection_manager.broadcast_message(status_msg)

    def _send_ok_response(self, conn, message="OK"):
        """Send an OK response to a command."""
        conn.command_in_progress = False
        conn.send(f"+0 {message}\n")

    def _send_error_response(self, conn, message, code=-1):
        """Send an error response to a command."""
        conn.command_in_progress = False
        conn.send(f"{code} {message}\n")

    def _handle_registration_result(self, conn, success, code, msg):
        """Handle result from registration command."""
        if success:
            logging.debug(f"Registration successful: {msg}")
            # Registration successful, now need to wait for registered_as message
        else:
            logging.error(f"Registration failed: {msg}")
            conn.update_state(ConnectionState.BROKEN, f"{msg}")

    def _handle_broadcast_value(self, value):
        """Broadcast a value to all authenticated connections."""
        # Get all authenticated connections
        auth_conns = self.connection_manager.get_connections_by_state(ConnectionState.AUTH_OK)

        # Send to each connection
        for conn in auth_conns.values():
            self._send_value(conn, value)

    def _send_info(self, conn):
        """Send metadata information about all values."""
        with self._lock:
            for value in self.values.values():
                # send current values
                self._send_value(conn, value)

    def _process_command(self, conn, line):
        """Process a command received from a connection."""
        # Store command in connection for processing responses
        conn.current_command = line

        # Queue for processing to avoid blocking network thread
        self.put_message(('command', conn.id, line))

    def send_command(self, conn_id, command, callback=None):
        """Send a command to a connection."""
        conn = self.connection_manager.get_connection(conn_id)
        if not conn:
            logging.warning(f"Cannot send command to unknown connection: {conn_id}")
            return False

        # Use the connection's send_command method
        return conn.send_command(command, callback)

    def _complete_client_authorization(self, conn):
        """Complete client authorization process."""
        logging.debug(f"Authorizing client {conn.name} (ID: {conn.device_id})")

        # Update connection state
        conn.update_state(ConnectionState.AUTH_OK, "Authorization complete")

        # Send device info
        self._send_info(conn)

        # Send BOP status
        conn.send(f"B {self.device_state} {self.bop_state}\n")

        # Send "Authorized" response
        self._send_ok_response(conn, "OK authorized")

        # Notify application of newly authorized client
        if hasattr(self, 'client_authorized_callback') and callable(self.client_authorized_callback):
            self.client_authorized_callback(conn.id)


    def requires_auth(self):
        """Check if authentication is required."""
        return self.auth_callback is not None

    def _get_connection_entity_desc(self, conn):
        """Get a human-readable description of an entity."""
        device_id = conn.device_id
        return self._get_entity_description(device_id)

    def _get_entity_description(self, centrald_id):
        """Get a human-readable description of an entity."""
        if centrald_id in self.entities:
            entity = self.entities[centrald_id]
            if entity['entity_type'] == 'CENTRALD':
                return "centrald"
            else:
                return f"{entity.get('type', 'notype')}{centrald_id}"
        return f"entity-{centrald_id}"

    def update_connection_name(self, conn):
        """Update a connection's descriptive name based on entity information."""
        if conn.type == 'centrald':
            conn.name = "centrald"
            return
        if conn.remote_device_name:
            conn.name = f"{conn.remote_device_name}"
        elif conn.device_id > 0:
            device_type = DevTypes.get(conn.remote_device_type, "unknown")
            conn.name = self._get_entity_description(conn.device_id)
        else:
            conn.name = f"{conn.type}-{conn.id[:8]}"

    def register_interest_in_value(self, device_name, value_name, callback):
        """
        Register interest in updates for a specific value from a device.

        This method will automatically establish a connection to the device
        if needed and maintain the connection.

        Args:
            device_name: Name of the device to monitor
            value_name: Name of the value to monitor
            callback: Function to call when value updates are received
        """
        if not hasattr(self, 'value_interests'):
            self.value_interests = {}

        key = f"{device_name}.{value_name}"
        self.value_interests[key] = callback
        logging.debug(f"Registered interest in {key}")

        # Add to pending interests for connection management
        self.pending_interests.add(device_name)

        # Check if we already have a connection to this device
        device_connected = False
        for conn in self.connection_manager.connections.values():
            if (conn.state == ConnectionState.AUTH_OK and
                hasattr(conn, 'remote_device_name') and
                conn.remote_device_name == device_name):
                device_connected = True
                logging.debug(f"Already have a connection to {device_name}, requesting info")
                conn.send_command("info")
                break

        if not device_connected:
            logging.info(f"No connection to {device_name} yet - waiting for centrald updates")

    def register_state_interest(self, device_name, state_callback):
        """
        Register interest in state updates from a specific device.

        Args:
            device_name: Name of the device to monitor
            state_callback: Callback function(device_name, state, bop_state, message)
        """
        if not hasattr(self, 'state_interests'):
            self.state_interests = {}

        self.state_interests[device_name] = state_callback
        logging.debug(f"Registered interest in state updates from {device_name}")

        # Add to pending interests to ensure connection is established
        self.pending_interests.add(device_name)

        # Check if we already have a connection to this device
        device_connected = False
        for conn in self.connection_manager.connections.values():
            if (conn.state == ConnectionState.AUTH_OK and
                hasattr(conn, 'remote_device_name') and
                conn.remote_device_name == device_name):
                device_connected = True
                logging.debug(f"Already have a connection to {device_name}, requesting status info")
                conn.send_command("device_status")
                break

        if not device_connected:
            logging.info(f"No connection to {device_name} yet - waiting for centrald updates")

    def handle_value_change_request(self, conn, value_name, value_data):
        """
        Handle a request to change a value.

        Args:
            conn: Connection making the request
            value_name: Name of the value to change
            value_data: New value data as string

        Returns:
            True if value was changed, False otherwise
        """
        # Check if connection is authenticated
        if conn.state != ConnectionState.AUTH_OK:
            logging.warning(f"Attempt to change value from non-authenticated connection: {conn.name}")
            self._send_error_response(conn, "Not authenticated")
            return False

        # Check if value exists
        with self._lock:
            if value_name not in self.values:
                logging.warning(f"Attempt to change non-existent value: {value_name}")
                self._send_error_response(conn, f"No such value: {value_name}")
                return False

            value = self.values[value_name]

        # Check if value is writable
        if not value.is_writable():
            logging.warning(f"Attempt to change read-only value: {value_name}")
            self._send_error_response(conn, f"Value {value_name} is read-only")
            return False

        # Attempt to update the value
        try:
            # Update the value from client
            value.update_from_network(value_data)
            # The callbacks didn't handle distribution:
            if value.need_send():
                self.distribute_value_immediate(value)
            self._send_ok_response(conn, f"Value {value_name} changed")
            return True

        except Exception as e:
            logging.error(f"Error updating value {value_name}: {e}")
            self._send_error_response(conn, f"Error updating value: {str(e)}")
            return False

    def distribute_value_immediate(self, value):
        """
        Immediately distribute a value to all connections, bypassing command queue.

        Args:
            value: The value to distribute
        """
        # Get all authenticated connections
        auth_conns = self.connection_manager.get_connections_by_state(ConnectionState.AUTH_OK)

        # Send raw value message to each connection directly
        for conn in auth_conns.values():
            try:
                msg = f"V {value.name} {value.get_string_value()}\n"
                conn.send(msg)
            except Exception as e:
                logging.error(f"Error sending immediate value to {conn.name}: {e}")

        # Mark value as sent
        value.reset_need_send()
