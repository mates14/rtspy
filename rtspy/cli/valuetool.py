#!/usr/bin/env python3
"""
RTS2 Value Client - Get and set RTS2 device values via network protocol

Usage:
    rts2-value -w device.value=new_value       # Write single value
    rts2-value -R device.value1,device.value2  # Read multiple values (comma-separated output)
    rts2-value -r device.value1,device.value2  # Read multiple values (shell eval format)

Examples:
    rts2-value -w SEL.enabled=false            # Disable selector
    rts2-value -w SEL.time_slice=180           # Set advance time to 3 minutes
    rts2-value -R SEL.enabled,SEL.time_slice   # Get both values: "true,300"
    rts2-value -r SEL.enabled,SEL.time_slice   # Get both: "enabled=true\\ntime_slice=300"
    eval $(rts2-value -r SEL.enabled,SEL.time_slice)  # Set shell variables
"""

import sys
import time
import logging
import argparse
import threading
import os
from typing import Optional, Any, List, Tuple, Dict

from rtspy.core.value import ValueString
from rtspy.core.device import Device
from rtspy.core.constants import DeviceType
from rtspy.core.config import DeviceConfig
from rtspy.core.app import App

# Configure logging to be quiet by default
logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(name)s %(levelname)s: %(message)s')

class RTS2ValueClient(Device, DeviceConfig):
    """Simple RTS2 network client for getting/setting device values."""

    def setup_config(self, config):
        """Register device-specific configuration options."""
        config.add_argument('--timeout', type=int, default=10,
                          help='Connection timeout in seconds')

    def __init__(self, device_name=None, port=0):
        # Generate unique client ID if none provided
        if device_name is None:
            import uuid
            # Use a short UUID-based identifier for uniqueness across network
            unique_id = str(uuid.uuid4())[:8]  # First 8 chars of UUID
            device_name = f"value-{unique_id}"

        super().__init__(device_name, DeviceType.LOGD, port)
        self.collected_values = {}
        self.target_values = []
        self.completion_event = threading.Event()
        self.set_state(self.STATE_IDLE, "Value client ready")


    def _on_centrald_connected(self, conn_id):
        """Called when connected to centrald."""
        logging.info(f"Connected to centrald with conn_id: {conn_id}")
        super()._on_centrald_connected(conn_id)

        # Log all entities known by network manager
        logging.info(f"Known entities after centrald connection: {list(self.network.entities.keys())}")
        for entity_id, entity_info in self.network.entities.items():
            logging.info(f"  Entity {entity_id}: {entity_info}")

    def _on_value_update(self, context):
        """Handle value updates from devices."""
        try:
            # Extract data from context dictionary
            device_name = context['device']
            value_name = context['value']
            value_data = context['data']

            logging.info(f"ON VALUE UPDATE {device_name}.{value_name} = {value_data}")

            # Now we know exactly which device/value this update is for
            key = f"{device_name}.{value_name}"
            self.collected_values[key] = str(value_data)
            logging.debug(f"Collected value: {key} = {value_data}")

            # Check if we have all values - signal completion
            if len(self.collected_values) >= len(self.target_values):
                self.completion_event.set()

        except Exception as e:
            logging.error(f"Error processing value update: {e}")

    def collect_values(self, device_value_pairs: List[Tuple[str, str]]) -> List[Optional[str]]:
        """Collect multiple values from devices."""
        self.collected_values = {}
        self.target_values = device_value_pairs
        self.completion_event.clear()

        # Register interests after device is already started by App framework
        for device_name, value_name in self.target_values:
            logging.debug(f"Registering interest in {device_name}.{value_name}")
            self.network.register_interest_in_value(device_name, value_name, self._on_value_update)

        # Wait for values to be collected (exit early if we get everything)
        timeout = getattr(self, 'timeout', 10)
        max_wait = min(timeout, 5)
        logging.debug(f"Waiting up to {max_wait} seconds for value collection...")

        # Wait for completion event or timeout
        if self.completion_event.wait(max_wait):
            logging.debug("All values collected - exiting early")
        else:
            logging.debug("Timeout reached")

        logging.debug(f"Collected {len(self.collected_values)} values: {list(self.collected_values.keys())}")

        # Return results in requested order
        results = []
        for device, value in device_value_pairs:
            key = f"{device}.{value}"
            results.append(self.collected_values.get(key))

        return results

    def set_value(self, device_name: str, value_name: str, new_value: str) -> bool:
        """Set a value on a device."""
        try:
            # Start temporarily to get network access
            self.start()
            time.sleep(1)  # Brief wait for connection

            # Send command to device
            success = self.network.send_command_to_device(device_name, f"S {value_name} {new_value}")

            self.stop()
            return success

        except Exception as e:
            logging.error(f"Failed to set {device_name}.{value_name}: {e}")
            self.stop()
            return False

    def on_device_connected(self, device_name, conn_id):
        """Override to trace device connections."""
        logging.info(f"TRACE: Device {device_name} connected with conn_id {conn_id}")
        if hasattr(super(), 'on_device_connected'):
            super().on_device_connected(device_name, conn_id)

    def on_device_value_received(self, device_name, value_name, value):
        """Override to trace value reception."""
        logging.info(f"TRACE: Received {device_name}.{value_name} = {value}")
        if hasattr(super(), 'on_device_value_received'):
            return super().on_device_value_received(device_name, value_name, value)


def parse_device_value_pairs(input_str: str) -> List[Tuple[str, str]]:
    """Parse comma-separated device.value pairs."""
    pairs = []
    for item in input_str.split(','):
        item = item.strip()
        if '.' not in item:
            raise ValueError(f"Invalid format '{item}': expected device.value")
        device, value = item.split('.', 1)
        pairs.append((device.strip(), value.strip()))
    return pairs


def main():
    # Use App framework for proper RTS2 device initialization
    app = App(description="RTS2 Value Client")

    # Add our custom mode arguments to the app
    mode_group = app.parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('-w', '--write', metavar='DEVICE.VALUE=NEWVAL',
                           help='Write mode: set device.value=new_value')
    mode_group.add_argument('-R', '--read-csv', metavar='DEVICE.VALUE,DEVICE.VALUE,...',
                           help='Read mode: comma-separated output')
    mode_group.add_argument('-r', '--read-shell', metavar='DEVICE.VALUE,DEVICE.VALUE,...',
                           help='Read mode: shell eval format')

    # Register device-specific options
    app.register_device_options(RTS2ValueClient)

    # Parse arguments
    args = app.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create device through App framework (this handles all the RTS2 setup)
    client = app.create_device(RTS2ValueClient)

    try:
        if args.write:
            # Write mode: device.value=new_value
            if '=' not in args.write:
                print("Error: Write mode requires format device.value=new_value", file=sys.stderr)
                sys.exit(1)

            device_value, new_value = args.write.split('=', 1)
            if '.' not in device_value:
                print("Error: Invalid format, expected device.value=new_value", file=sys.stderr)
                sys.exit(1)

            device, value = device_value.split('.', 1)
            success = client.set_value(device.strip(), value.strip(), new_value.strip())

            if success:
                if args.verbose:
                    print(f"{device}.{value} = {new_value}")
                sys.exit(0)
            else:
                print(f"Failed to set {device}.{value}", file=sys.stderr)
                sys.exit(1)

        elif args.read_csv:
            # Read mode: CSV output
            try:
                pairs = parse_device_value_pairs(args.read_csv)
            except ValueError as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

            results = client.collect_values(pairs)
            output = []
            for result in results:
                output.append(result if result is not None else "-")
            print(",".join(output))

        elif args.read_shell:
            # Read mode: shell eval format
            try:
                pairs = parse_device_value_pairs(args.read_shell)
            except ValueError as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

            results = client.collect_values(pairs)
            for (device, value), result in zip(pairs, results):
                if result is not None:
                    print(f"{value}={result}")
                else:
                    print(f"{value}=-")

    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        if args.verbose:
            import traceback
            traceback.print_exc()
        else:
            print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
