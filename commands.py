# commands/registry.py
import logging
from typing import Dict, List, Any, Callable, Optional, Tuple
from constants import ConnectionState, DevTypes

class CommandRegistry:
    """
    A simple registry for command handlers.
    
    This registry works with command handler groups rather than individual handlers.
    """
    
    def __init__(self):
        """Initialize the command registry."""
        self.handlers = []  # List of handler groups
        self.command_map = {}  # Command -> handler group
        
    def register_handler(self, handler):
        """
        Register a command handler group.
        
        Args:
            handler: Handler group that implements can_handle, handle, etc.
        """
        self.handlers.append(handler)
        
        # Map each command to this handler
        for cmd in handler.get_commands():
            if cmd in self.command_map:
                logging.warning(f"Overwriting existing handler for command '{cmd}'")
            self.command_map[cmd] = handler
            
        logging.debug(f"Registered handler for commands: {', '.join(handler.get_commands())}")
        
    def find_handler(self, command: str):
        """
        Find the handler for a command.
        
        Args:
            command: Command to find handler for
            
        Returns:
            Handler or None if not found
        """
        return self.command_map.get(command)
        
    def can_handle(self, command: str) -> bool:
        """
        Check if a command can be handled.
        
        Args:
            command: Command to check
            
        Returns:
            True if command can be handled, False otherwise
        """
        return command in self.command_map
        
    def dispatch(self, command: str, conn, params: str) -> Tuple[bool, Any]:
        """
        Dispatch a command to its handler.
        
        Args:
            command: Command to dispatch
            conn: Connection object
            params: Command parameters
            
        Returns:
            Tuple of (success, result)
        """
        handler = self.find_handler(command)
        if not handler:
            logging.warning(f"No handler for command '{command}'")
            return False, f"Unknown command: {command}"
            
        try:
            result = handler.handle(command, conn, params)
            return True, result
        except Exception as e:
            logging.error(f"Error handling command '{command}': {e}", exc_info=True)
            return False, f"Error handling command {command}: {str(e)}"
            
    def needs_response(self, command: str) -> bool:
        """
        Check if a command needs a response.
        
        Args:
            command: Command to check
            
        Returns:
            True if command needs a response, False otherwise
        """
        handler = self.find_handler(command)
        if not handler:
            # Default to True for unknown commands
            return True
            
        return handler.needs_response_for(command)
        
    def get_all_commands(self) -> List[str]:
        """
        Get all registered command names.
        
        Returns:
            List of command names
        """
        return list(self.command_map.keys())


class ProtocolCommands:
    """
    Handler for protocol-level commands (S, V, B, R, etc.).

    This class handles all single-letter protocol commands in one place.
    """

    def __init__(self, network_manager):
        self.network_manager = network_manager
        # Map of command -> handler method
        self.handlers = {
            "S": self.handle_status,
            "V": self.handle_value,
            "B": self.handle_bop,
            "R": self.handle_progress,
            "T": self.handle_technical,
            "M": self.handle_message,
            "X": self.handle_x_command,
            "E": self.handle_ignore,  
            "F": self.handle_ignore,
            "Z": self.handle_ignore,
            "device": self.handle_device_info,
            "client": self.handle_client,
            "this_device": self.handle_this_device_info,
            "delete_client": self.handle_delete_client,
            "delete_device": self.handle_ignore
        }
        # Commands that need responses
        self.needs_response = {
            "S": False,
            "V": False,
            "B": False,
            "R": False,
            "T": False,
            "M": False,
            "X": True,
            "E": False,
            "F": False,
            "Z": False,
            "device": False,
            "client": False,
            "this_device": False,
            "delete_client": False,
            "delete_device": False
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

    def handle_status(self, conn, params):
        """Handle STATUS command (S protocol command)."""
        # Check if this is a progress status message
        parts = params.split(maxsplit=1)

        if not parts:
            return False

        status_value = int(parts[0])

        # Extract message if present
        status_msg = ""
        if len(parts) > 1:
            # Check for quoted message
            msg_part = parts[1].strip()
            if msg_part.startswith('"') and msg_part.endswith('"'):
                status_msg = msg_part[1:-1]  # Remove quotes
            else:
                status_msg = msg_part

        # Update connection state
        conn.device_state = status_value

        # Notify about status change if needed
        if self.network_manager.state_changed_callback:
            old_state = getattr(self.network_manager, 'device_state', 0)
            self.network_manager.state_changed_callback(old_state, status_value, status_msg)
        
        # Check if any component has registered interest in this device's state
        device_name = self.network_manager._get_connection_entity_desc(conn)
        if hasattr(self.network_manager, 'state_interests') and device_name in self.network_manager.state_interests:
            # Call the registered callback
            self.network_manager.state_interests[device_name](
                device_name, status_value, conn.bop_state, status_msg)
            logging.debug(f"Dispatched state update for {device_name}")

        # Status commands don't expect responses
        conn.command_in_progress = False
        return True

    # Similarly update ProtocolCommands.handle_bop method
    def handle_bop(self, conn, params):
        """Handle BOP (Block Operation) state command."""
        parts = params.split()
        if len(parts) >= 2:
            try:
                device_state = int(parts[0])
                bop_state = int(parts[1])
                
                # Extract message if present
                status_msg = ""
                if len(parts) > 2:
                    msg_start = params.find('"')
                    if msg_start != -1:
                        msg_end = params.rfind('"')
                        if msg_end > msg_start:
                            status_msg = params[msg_start+1:msg_end]

                # Store BOP state in connection
                conn.device_state = device_state
                conn.bop_state = bop_state

                # Notify if needed
                if hasattr(self.network_manager, 'bop_state_changed_callback') and callable(self.network_manager.bop_state_changed_callback):
                    self.network_manager.bop_state_changed_callback(device_state, bop_state)
                    
                # Check if any component has registered interest in this device's state
                device_name = self.network_manager._get_connection_entity_desc(conn)
                if hasattr(self.network_manager, 'state_interests') and device_name in self.network_manager.state_interests:
                    # Call the registered callback
                    self.network_manager.state_interests[device_name](
                        device_name, device_state, bop_state, status_msg)
                    logging.debug(f"Dispatched BOP state update for {device_name}")
                    
            except ValueError:
                logging.warning(f"Invalid BOP state format: {params}")

        # BOP commands don't expect response
        conn.command_in_progress = False
        return True

    def handle_value(self, conn, params):
        """Handle 'V' (value) command."""
        parts = params.split(maxsplit=2)
        if len(parts) < 2:
            conn.command_in_progress = False
            return False

        value_name = parts[0]
        value_data = parts[1] if len(parts) > 1 else ""

        # Check if any component has registered interest in this value
        device_name = self.network_manager._get_connection_entity_desc(conn)
        key = f"{device_name}.{value_name}"

        if hasattr(self.network_manager, 'value_interests') and key in self.network_manager.value_interests:
            # Call the registered callback with the value
            self.network_manager.value_interests[key](value_data)
            logging.debug(f"Dispatched value update for {key}")

        # Value commands don't expect response
        conn.command_in_progress = False
        return True

    def handle_progress(self, conn, params):
        """Handle progress status command (R protocol command)."""
        parts = params.split(maxsplit=3)
        if len(parts) < 3:
            return False

        status_value = int(parts[0])
        start_time = float(parts[1])
        end_time = float(parts[2])

        # Extract message if present
        status_msg = ""
        if len(parts) > 3:
            msg_part = parts[3].strip()
            if msg_part.startswith('"') and msg_part.endswith('"'):
                status_msg = msg_part[1:-1]  # Remove quotes
            else:
                status_msg = msg_part

        # Update connection state with progress
        conn.device_state = status_value
        conn.progress_start = start_time
        conn.progress_end = end_time

        # Status commands don't expect responses
        conn.command_in_progress = False
        return True

    def handle_technical(self, conn, params):
        """Handle technical command (keeps connections alive)."""
        parts = params.split()
        if not parts:
            conn.command_in_progress = False
            return True

        command = parts[0]

        if command == "ready":
            # Respond with T OK
            conn.send("T OK\n")

        # Technical commands don't expect normal response
        conn.command_in_progress = False
        return True

    def handle_x_command(self, conn, params):
        """Handle 'X' (set value) command."""
        parts = params.split(maxsplit=2)
        if len(parts) < 3:
            logging.warning(f"Invalid X command format: {params}")
            self.network_manager._send_error_response(conn, "Invalid command format")
            conn.command_in_progress = False
            return False

        value_name = parts[0]
        value_op = parts[1]
        value_data = parts[2]

        # Notify network manager of value change request
        if value_op == "=":
            result = self.network_manager.handle_value_change_request(conn, value_name, value_data)
            return result
        else:
            logging.warning(f"Operand '{value_op}' not implemented in handle_x_command()")
            return False

    def handle_message(self, conn, params):
        """Handle message command (system messages)."""
        parts = params.split(maxsplit=3)
        if len(parts) < 4:
            conn.command_in_progress = False
            return True

        # Parse message parts
        timestamp_sec = int(parts[0])
        timestamp_usec = int(parts[1])
        origin_name = parts[2]
        msg_type_and_text = parts[3]

        # Split message type and text
        msg_parts = msg_type_and_text.split(maxsplit=1)
        if len(msg_parts) < 2:
            conn.command_in_progress = False
            return True

        msg_type = int(msg_parts[0])
        msg_text = msg_parts[1]

        # Process message
        if hasattr(self.network_manager, 'message_callback') and callable(self.network_manager.message_callback):
            timestamp = timestamp_sec + (timestamp_usec / 1000000.0)
            self.network_manager.message_callback(timestamp, origin_name, msg_type, msg_text)

        conn.command_in_progress = False
        return True

    def handle_device_info(self, conn, params):
        """Handle device info command."""
        parts = params.split()
        if len(parts) < 5:
            conn.command_in_progress = False
            return True

        try:
            centrald_num = int(parts[0])
            centrald_id = int(parts[1])
            device_name = parts[2]
            host = parts[3]
            port = int(parts[4])
            device_type = int(parts[5]) if len(parts) > 5 else -1

            # Store in global registry
            self.network_manager.entities[centrald_id] = {
                'name': device_name,
                'centrald_num': centrald_num,
                'host': host,
                'port': port,
                'type_id': device_type,
                'type': DevTypes.get(device_type),
                'entity_type': 'DEVICE'
            }

            conn.update_descriptive_name()
            logging.debug(f"Registered device: {device_name} (ID: {centrald_id}, type: {device_type})")

        except Exception as e:
            logging.warning(f"Error processing device info: {e}")

        # No response needed
        conn.command_in_progress = False
        return True

    def handle_client(self, conn, params):
        """Handle 'client' command from centrald."""
        parts = params.split()
        if len(parts) < 3:
            conn.command_in_progress = False
            return True

        centrald_id = int(parts[0])
        login = parts[1]
        clitype = parts[2]

        # Store client information in global registry
        self.network_manager.entities[centrald_id] = {
            'type': clitype,
            'name': login,
            'entity_type': 'CLIENT'
        }

        conn.update_descriptive_name()
        logging.debug(f"Registered client: ID {centrald_id}: {clitype} {login}")

        conn.command_in_progress = False
        return True

    def handle_this_device_info(self, conn, params):
        """Handle this_device info command."""
        parts = params.split()
        if len(parts) < 2:
            conn.command_in_progress = False
            return True

        device_name = parts[0]
        device_type = int(parts[1])

        # Update connection info
        conn.remote_device_name = device_name
        conn.remote_device_type = device_type

        logging.debug(f"Connected to device: {device_name}, type: {device_type}")

        conn.command_in_progress = False
        return True

    def handle_delete_client(self, conn, params):
        """Handle delete_client command from centrald."""
        client_id = int(params.strip())
        logging.debug(f"Client with ID {client_id} has been deleted/disconnected")

        # Remove from entity registry
        if client_id in self.network_manager.entities:
            entity_name = self.network_manager.entities[client_id].get('name', 'unknown')
            entity_type = self.network_manager.entities[client_id].get('entity_type', 'entity')
            logging.debug(f"Removing {entity_type.lower()} {entity_name} (ID: {client_id}) from registry")
            del self.network_manager.entities[client_id]
        else:
            logging.warning(f"Received delete_client for unknown client ID: {client_id}")

        conn.command_in_progress = False
        return True

    def handle_ignore(self, conn, params):
        """Handle commands that require no action."""
        conn.command_in_progress = False
        return True


class AuthCommands:
    """
    Handler for authentication-related commands.

    This class handles all commands related to authentication and authorization.
    """

    def __init__(self, network_manager):
        self.network_manager = network_manager
        # Map of command -> handler method
        self.handlers = {
            "auth": self.handle_auth,
            "A": self.handle_auth_response,
            "registered_as": self.handle_registered_as,
            "authorization_key": self.handle_key_response,
            "authorization_ok": self.handle_authorization_ok
        }
        # Commands that need responses
        self.needs_response = {
            "auth": False,
            "A": False,
            "registered_as": False,
            "authorization_key": False,
            "authorization_ok": False
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

    def handle_auth(self, conn, params):
        """Handle authentication command."""
        """The client sends this to request access, it attaches a key that is to be checked by the centrald"""
        # Parse auth parameters
        auth_parts = params.split()
        if len(auth_parts) < 3:
            self._send_error_response(conn, "Invalid auth format")
            return

        device_id = int(auth_parts[0])
        centrald_num = int(auth_parts[1])
        key = int(auth_parts[2])

        conn.update_descriptive_name()

        # Store client info
        conn.device_id = device_id
        conn.centrald_num = centrald_num
        conn.auth_key = key
        conn.update_state(ConnectionState.AUTH_PENDING, "Key to be checked against centrald")

        # Log with entity info if we have it
        conn_desc = self.network_manager._get_entity_description(device_id)
        logging.debug(f"Auth request from {conn_desc} (id:{device_id})") #: {entity['name']} (ID: {device_id})")

        # Find appropriate centrald connection to verify this authorization
        centrald_conn = self.network_manager.connection_manager.get_associated_centrald_connection()

        if centrald_conn:
            # Request key check from centrald
            logging.debug(f"authorize request to {centrald_conn.name} for device {device_id}, key {key}")
            self.network_manager.send_command(
                centrald_conn.id,
                f"authorize {device_id} {key}")
#                lambda c, success, code, msg: self._handle_auth_verification(c, conn.id, success, code, msg)
            
            # No immediate response - wait for centrald verification
        else:
            # No centrald connection available, handle authorization locally
            #logging.debug(f"authorize by default (no connection to centrald) for device {device_id}, key {key}") #: {entity['name']} (ID: {device_id})")
            if self.network_manager.auth_callback:
                try:
                    # Try with the expected signature first
                    auth_result = self.network_manager.auth_callback(device_id, centrald_num, key)
                except TypeError:
                    # If that fails, try with just the auth key
                    try:
                        auth_result = self.network_manager.auth_callback(key)
                    except TypeError as e:
                        # If that also fails, log the error and default to success
                        logging.error(f"Auth callback error: {e}")
                        auth_result = True

                if auth_result:
                    self.network_manager._complete_client_authorization(conn)
                else:
                    conn.update_state(ConnectionState.AUTH_FAILED, "Authorization failed")
            else:
                # No auth callback, assume success for testing
                logging.warning("No auth handler available, authorizing by default")
                self.network_manager._complete_client_authorization(conn)

        # Don't send immediate response - wait for verification
        conn.command_in_progress = False
        return True

    def _handle_auth_verification(self, centrald_conn, client_id, success, code, msg):
        """Handle response from centrald for authorization verification."""
        # Find the client connection directly from ConnectionManager
        client_conn = self.network_manager.connection_manager.get_connection(client_id)
        
        if not client_conn:
            logging.error(f"Client connection {client_id} not found")
            return
        
        if success:
            # Centrald approved the authorization
            logging.debug(f"Centrald approved authorization for client {client_id}")
            self.network_manager._complete_client_authorization(client_conn)
        else:
            # Centrald rejected the authorization
            logging.error(f"Centrald rejected authorization for client {client_id}: {msg}")
            client_conn.update_state(ConnectionState.AUTH_FAILED, "Authorization failed")


    # Other handler methods follow...
    def handle_auth_response(self, conn, params):
        """Handle authentication response commands that start with 'A'."""
        parts = params.split(maxsplit=1)
        if not parts:
            logging.warning(f"Invalid A command format: {params}")
            conn.command_in_progress = False
            return False
        
        # Get the actual command (the word after 'A')
        subcommand = parts[0]
        subparams = parts[1] if len(parts) > 1 else ""
        
        # Handle different authentication response commands
        if subcommand == "registered_as":
            # This is "A registered_as ID" format
            return self.handle_registered_as(conn, f"registered_as {subparams}")
        elif subcommand == "authorization_ok":
            # This is "A authorization_ok ID" format
            return self.handle_authorization_ok(conn, f"A {subcommand} {subparams}")
        elif subcommand == "authorization_failed":
            # This is "A authorization_failed ID" format
            # Implement this handler if needed
            logging.warning(f"Authorization failed: {subparams}")
            conn.command_in_progress = False
            return True
        else:
            logging.warning(f"Unknown A-prefixed command: {subcommand} {subparams}")
            conn.command_in_progress = False
            return False

    def handle_registered_as(self, conn, line):
        """Handle the registration response from centrald."""
        logging.debug(f"Processing registration response: {line}")

        # Handle both "registered_as ID" and "A registered_as ID" formats
        parts = line.split()
        device_id = None

        if len(parts) >= 2 and parts[0] == "registered_as":
            device_id = int(parts[1])
        elif len(parts) >= 3 and parts[0] == "A" and parts[1] == "registered_as":
            device_id = int(parts[2])

        if device_id is not None:
            conn.device_id = device_id
            logging.debug(f"Registered with centrald with device_id {device_id}")

            # Add centrald to entity registry with special type
            self.network_manager.entities[device_id] = {
                'name': 'centrald',
                'type_id': 1,
                'type': DevTypes.get(1),
                'entity_type': 'CENTRALD',  # Special type for centrald
                'host': conn.addr[0],
                'port': conn.addr[1]
            }

            conn.update_descriptive_name()

            # Mark as connected but not yet authorized
            conn.update_state(ConnectionState.AUTH_OK, f"Registered as {device_id}")

            # Request authorization key
            self.network_manager.send_command(
                conn.id,
                f"key {self.network_manager.device_name}",
                None  # Will be handled by key response handler
            )
        else:
            logging.error(f"Invalid registered_as format: {line}")

        # No response needed
        conn.command_in_progress = False
        return True

    def handle_key_response(self, conn, line):
        """Handle authorization_key response from centrald."""
        logging.debug(f"Processing key response: {line}")
        parts = line.split()

        if len(parts) >= 3 and parts[0] == "authorization_key":
            device_name = parts[1]
            auth_key = int(parts[2])

            # Store the key
            conn.auth_key = auth_key

            # Get our centrald ID
            device_id = conn.device_id

            if device_id > 0:
                logging.debug(f"Sending authorize command with ID {device_id} and key {auth_key}")
                # Send authorization request
                self.send_command(
                    conn.id,
                    f"authorize {device_id} {auth_key}",
                    self._handle_authorize_response
                )
            else:
                logging.error("Could not determine centrald ID for authorization")

        conn.command_in_progress = False
        return True

    def handle_authorization_ok(self, conn, line):
        """Handle authorization_ok message from centrald."""
        parts = line.split()
        auth_id = None

        if len(parts) >= 3 and parts[0] == "A" and parts[1] == "authorization_ok":
            auth_id = int(parts[2])

        if auth_id is not None:
            logging.debug(f"Received authorization_ok for device ID {auth_id}")

            if conn.device_id == auth_id:
                # This is authorization for our connection to centrald
                conn.update_state(ConnectionState.AUTH_OK, "Connection to centrald authenticated")

                # Notify that centrald connection is authorized
                if hasattr(self, 'centrald_connected_callback') and callable(self.centrald_connected_callback):
                    self.centrald_connected_callback(conn.id)
            else:
                # This is authorization for a client connecting to us
                for client_conn in self.network_manager.connection_manager.connections.values():
                    if (client_conn.device_id == auth_id and
                        client_conn.state == ConnectionState.AUTH_PENDING):
                        logging.debug(f"Authorizing pending client {client_conn.name} (ID: {auth_id})")
                        self.network_manager._complete_client_authorization(client_conn)
                        return True

            # If we get here, we don't have a matching client yet
            logging.warning(f"authorization_ok for non-pending id:{auth_id} - this should never happen")
                
        # No response needed
        conn.command_in_progress = False
        return True
