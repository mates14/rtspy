import argparse
import logging
import time
from typing import Dict, List, Optional, Type, Any

from device import Device

from datetime import datetime, timezone

# Custom formatter class to handle the specific format you want
class RTS2LogFormatter(logging.Formatter):
    def format(self, record):
        # Convert level names to single letters
        level_map = {
            'DEBUG': 'D',
            'INFO': 'I',
            'WARNING': 'W',
            'ERROR': 'E',
            'CRITICAL': 'C'
        }

        # Get the device name from the Device singleton if available
        from device import Device
        device = Device.get_instance()
        device_name = getattr(device, 'device_name', 'UNKNOWN') if device else 'UNKNOWN'

        # Format timestamp in UTC
        timestamp = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

        # Get the single letter level
        level = level_map.get(record.levelname, '?')

        # Format the message
        formatted_msg = f"{timestamp} UTC {device_name} {level} {record.getMessage()}"
        return formatted_msg

class App:
    """Base application class for RTS2 device drivers."""

    def __init__(self, description: str = "RTS2 Device"):
        """Initialize the application framework."""
        self.parser = argparse.ArgumentParser(description=description)
        self.args = None
        self.device = None

        # Register standard options
        self._register_standard_options()

    def _register_standard_options(self):
        """Register standard command line options common to all devices."""
        self.parser.add_argument('-d', '--device', default=None,
                            help='Device name (default based on device type)')
        self.parser.add_argument('-P', '--port', type=int, default=0,
                            help='TCP/IP port for RTS2 communication')
        self.parser.add_argument('-c', '--centrald', default='localhost',
                            help='Centrald hostname')
        self.parser.add_argument('-p', '--centrald-port', type=int, default=617,
                            help='Centrald port')
        self.parser.add_argument('-v', '--verbose', action='store_true',
                            help='Enable verbose logging')

    def register_device_options(self, device_class: Type[Device]):
        """
        Register device-specific options.

        This method should be called by device classes to add their
        specific command line parameters.
        """
        # This is a hook for device classes to add their options
        if hasattr(device_class, 'register_options') and callable(device_class.register_options):
            device_class.register_options(self.parser)

    def parse_args(self):
        """Parse command line arguments."""
        self.args = self.parser.parse_args()

        # Configure logging based on verbosity
        log_level = logging.DEBUG if self.args.verbose else logging.INFO

        # Create custom formatter
        formatter = RTS2LogFormatter()

        # Configure root logger
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)

        # Remove any existing handlers to avoid duplicate logs
        for hdlr in root_logger.handlers[:]:
            root_logger.removeHandler(hdlr)

        root_logger.addHandler(handler)

        return self.args

    def create_device(self, device_class: Type[Device], **kwargs):
        """
        Create device instance with parsed arguments.

        Args:
            device_class: Device class to instantiate
            **kwargs: Additional parameters to pass to the device constructor

        Returns:
            Device instance
        """
        # Get device name - use passed value, args value, or let the device decide
        device_name = kwargs.get('device_name', self.args.device)
        port = kwargs.get('port', self.args.port)

        # Create device instance
        self.device = device_class(
                device_name=device_name,
                port=port
                )

        # Apply standard args to device.network
        self.device.network.centrald_host = self.args.centrald
        self.device.network.centrald_port = self.args.centrald_port

        # Process device-specific arguments
        if hasattr(device_class, 'process_args') and callable(device_class.process_args):
            device_class.process_args(self.device, self.args)

        self.device.network.start()
        self.device.start()

        return self.device

    def run(self):
        """Run the application main loop."""
        if not self.device:
            raise RuntimeError("Device not created - call create_device() first")

        try:
            # Start the device
#            self.device.start()

            # Keep main thread alive
            while True:
                time.sleep(10)

        except KeyboardInterrupt:
            logging.info("Shutting down...")
        finally:
            if self.device:
                self.device.stop()
