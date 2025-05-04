import socket
import logging
import time
import threading
import queue
from enum import IntEnum
from typing import Dict, Any, Callable, Optional, Union, Tuple, List

from constants import ConnectionState

class QueuedCommand:
    """Represents a command queued for execution."""

    def __init__(self, command: str, callback: Optional[Callable] = None, timeout: float = 60.0):
        """
        Initialize a queued command.

        Args:
            command: The command to execute
            callback: Optional callback function for when command completes
            timeout: Timeout in seconds after which the command will be considered failed
        """
        self.command = command
        self.callback = callback
        self.timeout = timeout
        self.queued_at = time.time()

    def is_timed_out(self) -> bool:
        """Check if the command has timed out."""
        return (time.time() - self.queued_at) > self.timeout

class Connection:
    """
    Represents a network connection in the RTS2 system.

    This class encapsulates the state and behavior of network connections,
    providing a unified interface for different connection types.
    """

    def __init__(self,
                 conn_id: str,
                 sock: socket.socket,
                 addr: Tuple[str, int],
                 conn_type: str = 'client'):
        """
        Initialize a new connection.

        Args:
            conn_id: Unique identifier for this connection
            sock: The socket object
            addr: The (host, port) tuple for the remote endpoint
            conn_type: Type of connection ('client', 'centrald', 'device', etc.)
        """
        # Basic connection properties
        self.id = conn_id
        self.socket = sock
        self.addr = addr
        self.type = conn_type
        self.state = ConnectionState.CONNECTED

        # Buffer management
        self.buffer = bytearray()
        self.write_buffer = bytearray()

        # Connection metadata
        self.last_activity = time.time()
        self.connection_time = time.time()
        self.description = f"{conn_type}-{addr[0]}:{addr[1]}"

        # Authentication and identification
        self.name = f"{conn_type}-{conn_id}"
        self.device_id = -1
        self.centrald_num = -1
        self.remote_device_name = None
        self.remote_device_type = None
        self.auth_key = None

        # Command tracking
        self.command_in_progress = False
        self.current_command = None
        self.pending_command = None
        self.pending_command_time = None
        self.pending_command_callback = None
        self.registration_sent = False

        # Type-specific properties
        if conn_type == 'centrald':
            self.is_centrald = True
        else:
            self.is_centrald = False

        # State tracking for remote devices
        self.device_state = 0
        self.bop_state = 0
        self.progress_start = float('nan')
        self.progress_end = float('nan')

        # Command queue
        self.command_queue = queue.Queue()
        self.command_queue_lock = threading.RLock()
        self.command_timeout = 60.0  # Default timeout in seconds

        # Add a new thread for processing the command queue
        self.command_queue_thread = None
        self.queue_processor_running = False

        # Timeout and keepalive settings - matching C++ implementation
        self.connection_timeout = 300.0  # 5 minutes default timeout
        self.last_keepalive_time = time.time()

        logging.debug(f"Created {conn_type} connection {self.name} from {addr[0]}:{addr[1]}")

    def set_connection_timeout(self, timeout: float) -> None:
        """
        Set the connection timeout value.

        Args:
            timeout: Timeout value in seconds
        """
        self.connection_timeout = timeout
        logging.debug(f"Set connection timeout for {self.name} to {timeout} seconds")

    def update_state(self, new_state: ConnectionState, reason: str = "") -> None:
        """
        Update the connection state with logging.

        Args:
            new_state: The new state to set
            reason: Optional reason for the state change
        """
        old_state = self.state
        self.state = new_state

        # Update descriptive name if info might have changed
        from netman import NetworkManager
        net_manager = NetworkManager._instance if hasattr(NetworkManager, '_instance') else None
        if net_manager:
            net_manager.update_connection_name(self)

        # Log the state change
        logging.debug(f"Connection {self.name} state change: {old_state} -> {new_state} {reason}")

        # Update last activity timestamp
        self.last_activity = time.time()

    def send(self, data: Union[str, bytes]) -> bool:
        """
        Send data through this connection.

        Args:
            data: Data to send (string or bytes)

        Returns:
            True if data was queued for sending, False otherwise
        """
        if self.socket is None:
            return False

        # Convert string to bytes if needed
        if isinstance(data, str):
            data = data.encode('utf-8')

        # Log the outgoing message
        logging.debug(f"SEND {self.name}: {data!r}")

        # Add to write buffer
        self.write_buffer += data

        # Update activity timestamp
        self.last_activity = time.time()

        # Try to flush immediately if possible
        try:
            if self.socket.getblocking():
                self.socket.send(data)
        except:
            pass  # Will be sent by write handler

        return True

    def send_msg(self, message: str) -> int:
        """
        Send a message with trailing newline.

        Args:
            message: Message to send

        Returns:
            Number of bytes queued or -1 on error
        """
        if not message.endswith('\n'):
            message += '\n'

        if self.send(message):
            return len(message.encode('utf-8'))
        return -1

    def send_value_raw(self, value_name: str, value_string: str) -> None:
        """
        Send a raw value update message.

        Args:
            value_name: Name of the value
            value_string: String representation of the value
        """
        msg = f"V {value_name} {value_string}\n"
        self.send(msg)

    def process_data(self, data: bytes) -> None:
        """
        Process received data.

        Args:
            data: Newly received data
        """
        # Update activity timestamp
        self.last_activity = time.time()

        # Add to buffer
        self.buffer.extend(data)

        # Process complete lines
        self._process_buffer()

    def _process_buffer(self) -> None:
        """Process data in the connection's buffer."""
        # Keep processing until no more complete lines
        while True:
            newline_pos = self.buffer.find(b'\n')
            if newline_pos == -1:
                break

            # Extract line and remove from buffer
            line = self.buffer[:newline_pos].decode('utf-8', errors='replace')
            self.buffer = self.buffer[newline_pos + 1:]

            # Skip empty lines
            if not line:
                continue

            # Log the received line
            logging.debug(f"RECV {self.name}: {line!r}")

            # Return result to pending command if this is a response line
            if line[0] in ['+', '-']:
                self._handle_command_return(line)
                continue

            # Store for command processing
            self.current_command = line

            # Notify NetworkManager of the command
            if hasattr(self, 'command_callback') and callable(self.command_callback):
                self.command_callback(self.id, line)

    def is_timed_out(self, timeout: float) -> bool:
        """
        Check if the connection has timed out.

        Args:
            timeout: Timeout duration in seconds

        Returns:
            True if timed out, False otherwise
        """
        current_time = time.time()

        # Use provided timeout or the connection's configured timeout
        actual_timeout = timeout if timeout is not None else self.connection_timeout

        # Different timeout criteria based on connection state and type
        if self.type == 'centrald' and self.state != ConnectionState.AUTH_OK:
            # Shorter timeout for centrald connections that haven't authenticated
            return current_time - self.connection_time > 60.0  # 1 minute
        elif self.state == ConnectionState.CONNECTING:
            # Connections still connecting have a short timeout
            return current_time - self.connection_time > 10.0  # 10 seconds
        else:
            # In C++ implementation, timeout is enforced after 2x the timeout value
            # So we check if 2 * actual_timeout has passed since last activity
            return current_time - self.last_activity > (2 * actual_timeout)

    def check_keepalive(self) -> bool:
        """
        Check if it's time to send a keepalive message.

        Following the C++ implementation, we send a keepalive if 1/4 of the
        timeout period has passed since the last activity.

        Returns:
            True if keepalive was sent, False otherwise
        """
        current_time = time.time()

        # Calculate the idle time threshold (1/4 of timeout)
        idle_threshold = self.connection_timeout / 4

        # Check if we've been idle for more than the threshold
        idle_time = current_time - self.last_activity
        if idle_time > idle_threshold:
            # Time to send a keepalive
            if self.send_keepalive():
                self.last_keepalive_time = current_time
                return True

        return False

    def send_keepalive(self) -> bool:
        """
        Send a keepalive message using the PROTO_TECHNICAL command.

        Returns:
            True if successful, False otherwise
        """
        logging.debug(f"Sending keepalive to {self.name}")
        return self.send_msg("T ready")

    def start_queue_processor(self):
        """Start the command queue processor thread."""
        if self.command_queue_thread is None or not self.command_queue_thread.is_alive():
            self.queue_processor_running = True
            self.command_queue_thread = threading.Thread(
                target=self._process_command_queue,
                name=f"CmdQueue-{self.id}",
                daemon=True
            )
            self.command_queue_thread.start()
            logging.debug(f"Started command queue processor for connection {self.name}")

    def stop_queue_processor(self):
        """Stop the command queue processor thread."""
        self.queue_processor_running = False
        if self.command_queue_thread and self.command_queue_thread.is_alive():
            self.command_queue_thread.join(timeout=1.0)

    def _process_command_queue(self):
        """Thread function to process commands in the queue."""
        while self.queue_processor_running:
            # Check for commands that can be executed
            try:
                with self.command_queue_lock:
                    if not self.pending_command and not self.command_queue.empty():
                        # Get next command from queue
                        queued_cmd = self.command_queue.get_nowait()

                        # Check if command has timed out
                        if hasattr(queued_cmd, 'is_timed_out') and queued_cmd.is_timed_out():
                            logging.warning(f"Command timed out in queue: {queued_cmd.command}")
                            if queued_cmd.callback:
                                try:
                                    queued_cmd.callback(self, False, -1, "Command timed out in queue")
                                except Exception as e:
                                    logging.error(f"Error in timeout callback: {e}")
                            continue

                        # Execute command
                        if self._execute_command(queued_cmd.command, queued_cmd.callback):
                            logging.debug(f"Executed queued command: {queued_cmd.command}")
                        else:
                            logging.error(f"Failed to execute queued command: {queued_cmd.command}")
                            # Add command back to queue if it couldn't be sent?
                            # self.command_queue.put(queued_cmd)

                # Check for stuck commands (timeout detection)
                if self.pending_command and self.pending_command_time:
                    elapsed = time.time() - self.pending_command_time
                    if elapsed > self.command_timeout:
                        logging.warning(f"Command timeout: {self.pending_command} after {elapsed:.1f} seconds")
                        # Call callback with timeout error
                        callback = self.pending_command_callback
                        if callback:
                            try:
                                callback(self, False, -1, f"Command timed out after {elapsed:.1f} seconds")
                            except Exception as e:
                                logging.error(f"Error in timeout callback: {e}")

                        # Clear pending command to unblock queue
                        self.pending_command = None
                        self.pending_command_time = None
                        self.pending_command_callback = None
            except Exception as e:
                logging.error(f"Error in command queue processor: {e}", exc_info=True)

            # Sleep to avoid high CPU usage
            time.sleep(0.1)

    def send_command(self, command: str, callback: Optional[Callable] = None,
                    queue_if_busy: bool = True, timeout: float = 60.0) -> bool:
        """
        Send a command to this connection, with optional queuing.

        Args:
            command: Command to send
            callback: Optional callback for command completion
            queue_if_busy: Whether to queue the command if another is in progress
            timeout: Timeout for the command in seconds

        Returns:
            True if command was sent or queued, False otherwise
        """
        logging.debug(f"send_command to {self.name}: {command}")

        with self.command_queue_lock:
            # Check if a command is already in progress
            if self.pending_command is not None:
                if queue_if_busy:
                    # Queue command for later execution
                    queued_cmd = QueuedCommand(command, callback, timeout)
                    self.command_queue.put(queued_cmd)
                    logging.debug(f"Queued command for {self.name}: {command}, queue size: {self.command_queue.qsize()}")

                    # Ensure queue processor is running
                    self.start_queue_processor()
                    return True
                else:
                    logging.warning(f"Cannot send command to {self.name} - another command is in progress: {self.pending_command}")
                    return False

            # No pending command, execute directly
            return self._execute_command(command, callback)

    def _execute_command(self, command: str, callback: Optional[Callable] = None) -> bool:
        """
        Execute a command immediately.

        Args:
            command: Command to send
            callback: Callback for command completion

        Returns:
            True if command was sent, False otherwise
        """
        # Store command info
        self.pending_command = command
        self.pending_command_time = time.time()
        self.pending_command_callback = callback

        # Send command
        success = self.send_msg(command)
        return success

    def _handle_command_return(self, line: str) -> None:
        """
        Handle command return responses (+/-).

        Args:
            line: Response line starting with + or -
        """
        try:
            # Extract status code and message
            status_sign = line[0]  # + or -
            parts = line[1:].split(maxsplit=1)
            status_code = int(parts[0])
            status_msg = parts[1] if len(parts) > 1 else ""

            # Get the pending command
            command = self.pending_command
            callback = self.pending_command_callback

            if command:
                logging.debug(f"Command completed: {command} with result: {status_sign}{status_code} {status_msg}")

                # Call callback with result if present
                if callback:
                    success = status_sign == '+'
                    try:
                        if callable(callback):
                            callback(self, success, status_code, status_msg)
                        else:
                            logging.error(f"Fix your QueuedCommand={callback}", exc_info=True)
                    except Exception as e:
                        logging.error(f"Error in command callback: {e}", exc_info=True)

            # Clear pending command
            self.pending_command = None
            self.pending_command_time = None
            self.pending_command_callback = None

            # Process next command in queue (non-blocking check)
            if not self.command_queue.empty():
                # Queue processor thread will handle this automatically
                pass

        except Exception as e:
            logging.error(f"Error processing command return: {e}", exc_info=True)
            # Clear pending command to avoid getting stuck
            self.pending_command = None
            self.pending_command_time = None
            self.pending_command_callback = None

    def close(self) -> None:
        """Close the connection and stop queue processor."""
        # Stop queue processor
        self.stop_queue_processor()

        # Existing close code...
        logging.debug(f"Closing connection to {self.name}")

        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass

        self.socket = None
        self.update_state(ConnectionState.BROKEN, "Connection closed")

        # Notify about closure if callback is registered
        if hasattr(self, 'closed_callback') and callable(self.closed_callback):
            self.closed_callback(self.id)

    def flush_write_buffer(self) -> bool:
        """
        Attempt to flush the write buffer.

        Returns:
            True if successful (or nothing to flush), False on error
        """
        if not self.socket or not self.write_buffer:
            return True

        try:
            # Send as much as possible
            sent = self.socket.send(self.write_buffer)
            self.write_buffer = self.write_buffer[sent:]
            self.last_activity = time.time()
            return True
        except ConnectionError:
            self.close()
            return False
        except Exception as e:
            logging.error(f"Error writing to {self.name}: {e}")
            self.close()
            return False

    def register_command_callback(self, callback: Callable) -> None:
        """
        Register a callback for command processing.

        Args:
            callback: Function to call when a command is received
        """
        self.command_callback = callback

    def register_closed_callback(self, callback: Callable) -> None:
        """
        Register a callback for connection closure.

        Args:
            callback: Function to call when the connection is closed
        """
        self.closed_callback = callback

    def set_name(self, name):
        """Set a descriptive name for this connection."""
        self.name = name
        logging.debug(f"Updated connection name to: {name}")

    def __repr__(self) -> str:
        """String representation of the connection."""
        return f"Connection(id={self.id}, type={self.type}, state={self.state}, addr={self.addr})"


class ConnectionManager:
    """
    Manages a collection of Connection objects.

    This class provides a unified interface for managing connections
    of different types.
    """

    def __init__(self):
        """Initialize the connection manager."""
        self.connections = {}  # id -> Connection
        self._lock = threading.RLock()

    def add_connection(self, connection: Connection) -> None:
        """
        Add a connection to the manager.

        Args:
            connection: Connection object to add
        """
        with self._lock:
            self.connections[connection.id] = connection

    def remove_connection(self, conn_id: str) -> None:
        """
        Remove a connection from the manager.

        Args:
            conn_id: ID of the connection to remove
        """
        with self._lock:
            if conn_id in self.connections:
                del self.connections[conn_id]

    def get_connection(self, conn_id: str) -> Optional[Connection]:
        """
        Get a connection by ID.

        Args:
            conn_id: ID of the connection to get

        Returns:
            Connection object or None if not found
        """
        with self._lock:
            return self.connections.get(conn_id)

    def get_connections_by_type(self, conn_type: str) -> Dict[str, Connection]:
        """
        Get all connections of a specific type.

        Args:
            conn_type: Connection type to filter by

        Returns:
            Dictionary of connection ID -> Connection object
        """
        with self._lock:
            return {k: v for k, v in self.connections.items()
                    if v.type == conn_type}

    def get_connections_by_state(self, state: ConnectionState) -> Dict[str, Connection]:
        """
        Get all connections in a specific state.

        Args:
            state: Connection state to filter by

        Returns:
            Dictionary of connection ID -> Connection object
        """
        with self._lock:
            return {k: v for k, v in self.connections.items()
                    if v.state == state}

    def find_connection_by_socket(self, sock: socket.socket) -> Optional[Connection]:
        """
        Find a connection by its socket object.

        Args:
            sock: Socket object to search for

        Returns:
            Connection object or None if not found
        """
        sock_id = id(sock)
        with self._lock:
            for conn in self.connections.values():
                if conn.socket and id(conn.socket) == sock_id:
                    return conn
        return None

    def close_all_connections(self) -> None:
        """Close all connections."""
        with self._lock:
            for conn in list(self.connections.values()):
                conn.close()

    def broadcast_message(self, message: str,
                          conn_type: Optional[str] = None,
                          min_state: Optional[ConnectionState] = None) -> None:
        """
        Broadcast a message to multiple connections.

        Args:
            message: Message to broadcast
            conn_type: Optional connection type filter
            min_state: Optional minimum connection state filter
        """
        with self._lock:
            for conn in list(self.connections.values()):
                # Apply filters if specified
                if conn_type and conn.type != conn_type:
                    continue
                if min_state and conn.state < min_state:
                    continue

                # Send message
                conn.send_msg(message)

    def get_associated_centrald_connection(self, device_id=None):
        """
        Get the centrald connection associated with this device.

        In a single-centrald environment, returns the first authenticated centrald.
        In a multi-centrald environment, returns the specific centrald this device
        is registered with.

        Args:
            device_id: Optional device ID to look for a specific association

        Returns:
            Connection object or None if not found
        """
        with self._lock:
            # If we have a device_id, look for a specific association
            if device_id is not None:
                for conn in self.connections.values():
                    if (conn.type == 'centrald' and
                        conn.state == ConnectionState.AUTH_OK and
                        hasattr(conn, 'associated_devices') and
                        device_id in conn.associated_devices):
                        return conn

            # Otherwise, return first authenticated centrald (single-centrald case)
            for conn in self.connections.values():
                if conn.type == 'centrald' and conn.state == ConnectionState.AUTH_OK:
                    return conn

        return None

    def check_all_keepalives(self) -> None:
        """Check all connections for keepalive needs."""
        with self._lock:
            for conn in list(self.connections.values()):
                # Skip broken connections
                if conn.state in (ConnectionState.BROKEN, ConnectionState.DELETE):
                    continue

                # For established connections, check if keepalive needed
                if conn.state not in (ConnectionState.CONNECTING, ConnectionState.INPROGRESS):
                    conn.check_keepalive()

    def clean_stale_connections(self, timeout: float = None) -> None:
        """
        Clean up stale connections.

        Args:
            timeout: Timeout duration in seconds (default: None, uses connection's timeout)
        """
        with self._lock:
            for conn_id, conn in list(self.connections.items()):
                if conn.is_timed_out(timeout):
                    logging.warning(f"Connection {conn.name} timed out after {time.time() - conn.last_activity:.1f} seconds")
                    conn.close()
                    self.remove_connection(conn_id)

    def __len__(self) -> int:
        """Get the number of connections."""
        with self._lock:
            return len(self.connections)
